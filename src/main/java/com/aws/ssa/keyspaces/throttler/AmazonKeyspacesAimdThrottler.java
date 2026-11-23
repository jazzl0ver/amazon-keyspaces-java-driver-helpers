package com.aws.ssa.keyspaces.throttler;

import com.datastax.oss.driver.api.core.AllNodesFailedException;
import com.datastax.oss.driver.api.core.DriverTimeoutException;
import com.datastax.oss.driver.api.core.RequestThrottlingException;
import com.datastax.oss.driver.api.core.config.DriverExecutionProfile;
import com.datastax.oss.driver.api.core.config.DriverOption;
import com.datastax.oss.driver.api.core.context.DriverContext;
import com.datastax.oss.driver.api.core.servererrors.OverloadedException;
import com.datastax.oss.driver.api.core.servererrors.ReadTimeoutException;
import com.datastax.oss.driver.api.core.servererrors.WriteTimeoutException;
import com.datastax.oss.driver.api.core.session.throttling.RequestThrottler;
import com.datastax.oss.driver.api.core.session.throttling.Throttled;

import java.time.Duration;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Iterator;
import java.util.Objects;
import java.util.function.LongSupplier;

/**
 * Session-wide additive-increase/multiplicative-decrease (AIMD) concurrency controller.
 *
 * <p>This is a driver request throttler, not a retry policy. It observes completed driver
 * requests, admits up to a dynamic concurrency limit, and queues surplus requests without
 * blocking calling threads. On congestion it multiplies the limit by a factor below 1;
 * after enough successful completions and a quiet interval it adds one slot.
 *
 * <p>Driver-level retries can hide intermediate throttling failures from this component.
 * It cannot measure consumed WCUs or identify individual Amazon Keyspaces storage partitions.
 */
public final class AmazonKeyspacesAimdThrottler implements RequestThrottler {

  private enum Option implements DriverOption {
    INITIAL("advanced.throttler.aimd.initial-concurrency"),
    MIN("advanced.throttler.aimd.min-concurrency"),
    MAX("advanced.throttler.aimd.max-concurrency"),
    MAX_QUEUE("advanced.throttler.aimd.max-queue-size"),
    SUCCESSES_PER_INCREASE("advanced.throttler.aimd.successes-per-increase"),
    INCREASE_INTERVAL("advanced.throttler.aimd.increase-interval"),
    DECREASE_COOLDOWN("advanced.throttler.aimd.decrease-cooldown"),
    DECREASE_FACTOR("advanced.throttler.aimd.decrease-factor");

    private final String path;

    Option(String path) { this.path = path; }

    @Override public String getPath() { return path; }
  }

  private final Object lock = new Object();
  private final Deque<Throttled> queued = new ArrayDeque<>();
  private final IdentityHashMap<Throttled, Boolean> queuedIndex = new IdentityHashMap<>();
  private final IdentityHashMap<Throttled, Boolean> active = new IdentityHashMap<>();
  private final int minConcurrency;
  private final int maxConcurrency;
  private final int maxQueueSize;
  private final int successesPerIncrease;
  private final double decreaseFactor;
  private final long increaseIntervalNanos;
  private final long decreaseCooldownNanos;
  private final LongSupplier clock;

  private int limit;
  private int successes;
  private long lastIncrease = Long.MIN_VALUE;
  private long lastDecrease = Long.MIN_VALUE;
  private boolean closed;

  /** Constructor discovered by DataStax Java Driver 4.x via reflection. */
  public AmazonKeyspacesAimdThrottler(DriverContext context) {
    this(context.getConfig().getDefaultProfile());
  }

  private AmazonKeyspacesAimdThrottler(DriverExecutionProfile p) {
    this(
        p.getInt(Option.INITIAL, 64),
        p.getInt(Option.MIN, 8),
        p.getInt(Option.MAX, 512),
        p.getInt(Option.MAX_QUEUE, 1024),
        p.getInt(Option.SUCCESSES_PER_INCREASE, 64),
        p.getDouble(Option.DECREASE_FACTOR, 0.5),
        p.getDuration(Option.INCREASE_INTERVAL, Duration.ofMillis(100)),
        p.getDuration(Option.DECREASE_COOLDOWN, Duration.ofMillis(500)),
        System::nanoTime);
  }

  // Package-private deterministic constructor for tests.
  AmazonKeyspacesAimdThrottler(
      int initial, int minimum, int maximum, int maxQueue, int successesPerIncrease,
      double decreaseFactor, Duration increaseInterval, Duration decreaseCooldown,
      LongSupplier clock) {
    if (minimum < 1 || initial < minimum || maximum < initial) {
      throw new IllegalArgumentException("Require 1 <= min-concurrency <= initial-concurrency <= max-concurrency");
    }
    if (maxQueue < 0 || successesPerIncrease < 1) {
      throw new IllegalArgumentException("Require max-queue-size >= 0 and successes-per-increase >= 1");
    }
    if (!(decreaseFactor > 0.0 && decreaseFactor < 1.0)) {
      throw new IllegalArgumentException("decrease-factor must be strictly between 0 and 1");
    }
    if (increaseInterval.isNegative() || decreaseCooldown.isNegative()) {
      throw new IllegalArgumentException("intervals must not be negative");
    }
    this.clock = Objects.requireNonNull(clock, "clock");
    this.minConcurrency = minimum;
    this.maxConcurrency = maximum;
    this.maxQueueSize = maxQueue;
    this.successesPerIncrease = successesPerIncrease;
    this.decreaseFactor = decreaseFactor;
    this.increaseIntervalNanos = increaseInterval.toNanos();
    this.decreaseCooldownNanos = decreaseCooldown.toNanos();
    this.limit = initial;
  }

  @Override
  public void register(Throttled request) {
    Objects.requireNonNull(request, "request");
    boolean admit = false;
    String rejection = null;
    synchronized (lock) {
      if (closed) {
        rejection = "AIMD request throttler is closed";
      } else if (active.containsKey(request) || queuedIndex.containsKey(request)) {
        throw new IllegalArgumentException("Request already registered");
      } else if (queued.isEmpty() && active.size() < limit) {
        active.put(request, Boolean.TRUE);
        admit = true;
      } else if (queued.size() < maxQueueSize) {
        queued.addLast(request);
        queuedIndex.put(request, Boolean.TRUE);
      } else {
        rejection = "AIMD request throttler queue is full";
      }
    }
    // Never invoke driver callbacks while holding the lock: callbacks may reenter.
    if (admit) {
      notifyReady(request, false);
    } else if (rejection != null) {
      request.onThrottleFailure(new RequestThrottlingException(rejection));
    }
  }

  @Override
  public void signalSuccess(Throttled request) {
    finish(request, false, true);
  }

  @Override
  public void signalError(Throttled request, Throwable error) {
    finish(request, isCongestion(error), false);
  }

  @Override
  public void signalTimeout(Throttled request) {
    // A timeout while queued is not evidence of Keyspaces congestion.
    finish(request, true, false);
  }

  private void finish(Throttled request, boolean congestion, boolean success) {
    List<Throttled> ready;
    synchronized (lock) {
      if (queuedIndex.remove(request) != null) {
        // ArrayDeque.remove(Object) uses equals(); Throttled tracking is identity-based.
        for (Iterator<Throttled> it = queued.iterator(); it.hasNext(); ) {
          if (it.next() == request) {
            it.remove();
            break;
          }
        }
        return;
      }
      if (active.remove(request) == null) {
        return; // late, duplicate, or untracked completion
      }
      if (!closed) {
        long now = clock.getAsLong();
        if (congestion) {
          if (lastDecrease == Long.MIN_VALUE || now - lastDecrease >= decreaseCooldownNanos) {
            int reduced = (int) Math.floor(limit * decreaseFactor);
            limit = Math.max(minConcurrency, Math.min(limit - 1, reduced));
            lastDecrease = now;
          }
          successes = 0;
        } else if (success) {
          if (successes < successesPerIncrease) {
            successes++;
          }
          boolean canIncrease = lastIncrease == Long.MIN_VALUE
              || now - lastIncrease >= increaseIntervalNanos;
          boolean recovered = lastDecrease == Long.MIN_VALUE
              || now - lastDecrease >= decreaseCooldownNanos;
          if (limit < maxConcurrency && successes >= successesPerIncrease
              && canIncrease && recovered) {
            limit++;
            successes = 0;
            lastIncrease = now;
          }
        }
      }
      ready = drain();
    }
    notifyReady(ready);
  }

  /** Caller must hold {@code lock}; reserves slots before releasing the lock. */
  private List<Throttled> drain() {
    List<Throttled> ready = new ArrayList<>();
    if (!closed) {
      while (active.size() < limit && !queued.isEmpty()) {
        Throttled request = queued.removeFirst();
        queuedIndex.remove(request);
        active.put(request, Boolean.TRUE);
        ready.add(request);
      }
    }
    return ready;
  }

  private void notifyReady(List<Throttled> requests) {
    RuntimeException firstFailure = null;
    for (Throttled request : requests) {
      try {
        notifyReady(request, true);
      } catch (RuntimeException error) {
        if (firstFailure == null) {
          firstFailure = error;
        }
      }
    }
    if (firstFailure != null) {
      throw firstFailure;
    }
  }

  private void notifyReady(Throttled request, boolean wasDelayed) {
    // An expiration may have removed a reserved request before callback delivery.
    synchronized (lock) {
      if (!active.containsKey(request)) {
        return;
      }
    }
    try {
      request.onThrottleReady(wasDelayed);
    } catch (RuntimeException e) {
      signalError(request, e); // Do not leak a slot if a callback throws.
      throw e;
    }
  }

  private static boolean isCongestion(Throwable error) {
    if (error == null) {
      return false;
    }
    // A request may fail with AllNodesFailedException whose per-node errors are
    // NOT in its cause chain. Also consider suppressed errors, but bound work.
    Deque<Throwable> toVisit = new ArrayDeque<>();
    IdentityHashMap<Throwable, Boolean> seen = new IdentityHashMap<>();
    toVisit.add(error);
    while (!toVisit.isEmpty() && seen.size() < 64) {
      Throwable next = toVisit.removeFirst();
      if (seen.put(next, Boolean.TRUE) != null) {
        continue;
      }
      if (next instanceof WriteTimeoutException
          || next instanceof ReadTimeoutException
          || next instanceof OverloadedException
          || next instanceof DriverTimeoutException) {
        return true;
      }
      if (next.getCause() != null) {
        toVisit.addLast(next.getCause());
      }
      for (Throwable suppressed : next.getSuppressed()) {
        if (suppressed != null) {
          toVisit.addLast(suppressed);
        }
      }
      if (next instanceof AllNodesFailedException) {
        for (List<Throwable> failures : ((AllNodesFailedException) next).getAllErrors().values()) {
          for (Throwable failure : failures) {
            if (failure != null) {
              toVisit.addLast(failure);
            }
          }
        }
      }
    }
    return false;
  }

  @Override
  public void close() {
    List<Throttled> rejected;
    synchronized (lock) {
      if (closed) {
        return;
      }
      closed = true;
      rejected = new ArrayList<>(queued);
      queued.clear();
      queuedIndex.clear();
      // Active requests are owned by the driver and can still finish normally.
    }
    for (Throttled request : rejected) {
      request.onThrottleFailure(new RequestThrottlingException("AIMD request throttler is closed"));
    }
  }

  // Package-private state accessors used by deterministic unit tests.
  int currentLimit() { synchronized (lock) { return limit; } }
  int inFlight() { synchronized (lock) { return active.size(); } }
  int pending() { synchronized (lock) { return queued.size(); } }
}
