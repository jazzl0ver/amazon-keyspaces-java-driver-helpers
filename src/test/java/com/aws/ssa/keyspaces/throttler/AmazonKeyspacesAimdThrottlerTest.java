package com.aws.ssa.keyspaces.throttler;

import com.datastax.oss.driver.api.core.DriverTimeoutException;
import com.datastax.oss.driver.api.core.RequestThrottlingException;
import com.datastax.oss.driver.api.core.session.throttling.Throttled;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.*;

class AmazonKeyspacesAimdThrottlerTest {
  private final AtomicLong now = new AtomicLong(1_000_000_000L);

  private AmazonKeyspacesAimdThrottler controller(int initial, int min, int max, int queue, int successes) {
    return new AmazonKeyspacesAimdThrottler(initial, min, max, queue, successes, 0.5,
        Duration.ofMillis(100), Duration.ofMillis(500), now::get);
  }

  private static class Request implements Throttled {
    int starts;
    int failures;
    boolean delayed;
    @Override public void onThrottleReady(boolean wasDelayed) {
      starts++;
      delayed = wasDelayed;
    }
    @Override public void onThrottleFailure(RequestThrottlingException error) {
      failures++;
    }
  }

  @Test void queuesInFifoOrderAndHonorsCapacity() {
    AmazonKeyspacesAimdThrottler t = controller(1, 1, 3, 2, 100);
    Request a = new Request(), b = new Request(), c = new Request(), d = new Request();
    t.register(a);
    t.register(b);
    t.register(c);
    t.register(d);
    assertEquals(1, a.starts);
    assertFalse(a.delayed);
    assertEquals(0, b.starts);
    assertEquals(1, d.failures);
    assertEquals(2, t.pending());
    t.signalSuccess(a);
    assertEquals(1, b.starts);
    assertTrue(b.delayed);
    assertEquals(0, c.starts);
    t.signalSuccess(b);
    assertEquals(1, c.starts);
    assertEquals(0, t.pending());
    assertEquals(1, t.inFlight());
  }

  @Test void additiveIncreaseNeedsSuccessCountAndInterval() {
    AmazonKeyspacesAimdThrottler t = controller(1, 1, 4, 0, 2);
    for (int i = 0; i < 2; i++) {
      Request r = new Request(); t.register(r); t.signalSuccess(r);
    }
    assertEquals(2, t.currentLimit());
    // The next two successes do not advance until the increase interval elapses.
    for (int i = 0; i < 2; i++) {
      Request r = new Request(); t.register(r); t.signalSuccess(r);
    }
    assertEquals(2, t.currentLimit());
    now.addAndGet(Duration.ofMillis(100).toNanos());
    Request r = new Request(); t.register(r); t.signalSuccess(r);
    assertEquals(3, t.currentLimit());
  }

  @Test void multiplicativeDecreaseAndCooldownAvoidCollapse() {
    AmazonKeyspacesAimdThrottler t = controller(8, 2, 16, 0, 10);
    Request a = new Request(), b = new Request();
    t.register(a); t.register(b);
    t.signalError(a, new DriverTimeoutException("Keyspaces write timeout"));
    assertEquals(4, t.currentLimit());
    t.signalTimeout(b);
    assertEquals(4, t.currentLimit());
    now.addAndGet(Duration.ofMillis(500).toNanos());
    Request c = new Request(); t.register(c);
    t.signalTimeout(c);
    assertEquals(2, t.currentLimit());
  }

  @Test void wrappedAndSuppressedCongestionSignalsDecrease() {
    AmazonKeyspacesAimdThrottler t = controller(8, 2, 16, 0, 10);
    Request r = new Request(); t.register(r);
    RuntimeException wrapper = new RuntimeException("all nodes failed");
    wrapper.addSuppressed(new DriverTimeoutException("write timeout"));
    t.signalError(r, wrapper);
    assertEquals(4, t.currentLimit());
  }

  @Test void doesNotAdmitQueuedRequestsWhileAboveReducedLimit() {
    AmazonKeyspacesAimdThrottler t = controller(4, 1, 8, 4, 100);
    Request a = new Request(), b = new Request(), c = new Request(), d = new Request();
    Request e = new Request();
    t.register(a); t.register(b); t.register(c); t.register(d); t.register(e);
    assertEquals(4, t.inFlight());
    t.signalTimeout(a); // 4 -> 2, with 3 requests still active
    assertEquals(2, t.currentLimit());
    assertEquals(0, e.starts);
    t.signalSuccess(b); // 2 active; no free capacity
    assertEquals(0, e.starts);
    t.signalSuccess(c); // 1 active; admit e
    assertEquals(1, e.starts);
    assertEquals(2, t.inFlight());
  }

  @Test void nonCongestionErrorDoesNotDecrease() {
    AmazonKeyspacesAimdThrottler t = controller(4, 1, 8, 0, 10);
    Request r = new Request(); t.register(r);
    t.signalError(r, new IllegalArgumentException("invalid CQL"));
    assertEquals(4, t.currentLimit());
    assertEquals(0, t.inFlight());
  }

  @Test void queuedTimeoutDoesNotDecreaseAndNeverExecutes() {
    AmazonKeyspacesAimdThrottler t = controller(1, 1, 2, 2, 10);
    Request a = new Request(), b = new Request(), c = new Request();
    t.register(a); t.register(b); t.register(c);
    t.signalTimeout(b);
    assertEquals(1, t.currentLimit());
    t.signalSuccess(a);
    assertEquals(0, b.starts);
    assertEquals(1, c.starts);
  }

  @Test void lateCompletionIsIgnored() {
    AmazonKeyspacesAimdThrottler t = controller(2, 1, 4, 0, 2);
    Request r = new Request(); t.register(r);
    t.signalTimeout(r);
    assertEquals(1, t.currentLimit());
    t.signalSuccess(r);
    t.signalError(r, new DriverTimeoutException("late"));
    assertEquals(1, t.currentLimit());
    assertEquals(0, t.inFlight());
  }

  @Test void decreasePausesGrowthUntilRecoveryInterval() {
    AmazonKeyspacesAimdThrottler t = controller(4, 1, 8, 0, 1);
    Request r = new Request(); t.register(r); t.signalTimeout(r);
    assertEquals(2, t.currentLimit());
    Request s = new Request(); t.register(s); t.signalSuccess(s);
    assertEquals(2, t.currentLimit());
    now.addAndGet(Duration.ofMillis(500).toNanos());
    Request u = new Request(); t.register(u); t.signalSuccess(u);
    assertEquals(3, t.currentLimit());
  }

  @Test void closeRejectsQueueAndFutureRegistrations() {
    AmazonKeyspacesAimdThrottler t = controller(1, 1, 2, 2, 10);
    Request a = new Request(), b = new Request(), c = new Request();
    t.register(a); t.register(b);
    t.close(); t.close();
    t.register(c);
    assertEquals(1, b.failures);
    assertEquals(1, c.failures);
    assertEquals(0, b.starts);
    t.signalSuccess(a);
    assertEquals(0, t.pending());
  }

  @Test void rejectsInvalidConfiguration() {
    assertThrows(IllegalArgumentException.class,
        () -> new AmazonKeyspacesAimdThrottler(2, 3, 8, 10, 1, .5,
            Duration.ZERO, Duration.ZERO, now::get));
    assertThrows(IllegalArgumentException.class,
        () -> new AmazonKeyspacesAimdThrottler(2, 1, 8, 10, 1, 1.0,
            Duration.ZERO, Duration.ZERO, now::get));
  }
}
