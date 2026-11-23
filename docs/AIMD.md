# Experimental AIMD throttler for Amazon Keyspaces / DSBulk

This change adds an **opt-in** `RequestThrottler` to the existing `amazon-keyspaces-java-driver-helpers` package. No table schema changes are required. Unlike a retry policy, this component observes request completions and applies **AIMD to the number of in-flight driver requests**.

## Configuration

Add to the existing DSBulk Java Driver configuration:

```hocon
datastax-java-driver {
  advanced.throttler {
    class = com.aws.ssa.keyspaces.throttler.AmazonKeyspacesAimdThrottler
    aimd {
      min-concurrency = 8
      initial-concurrency = 64
      max-concurrency = 512
      max-queue-size = 1024
      successes-per-increase = 64
      increase-interval = 100 milliseconds
      decrease-factor = 0.5
      decrease-cooldown = 500 milliseconds
    }
  }
  advanced.retry-policy {
    class = com.aws.ssa.keyspaces.retry.AmazonKeyspacesRetryPolicy
    max-attempts = 3
  }
}
```

All AIMD values shown above are defaults. Values are **starting points for a test**, not throughput guarantees. The class is activated by `advanced.throttler.class`; it is separate from `advanced.retry-policy.class`. Keep `dsbulk.batch.mode = DISABLED` and avoid setting an excessively low `dsbulk.engine.maxConcurrentQueries`: DSBulk's upstream cap may prevent the adaptive limit from increasing.

## Control loop

- **Success:** after `successes-per-increase` successful requests, and no sooner than `increase-interval` after the last increase, grow the admitted in-flight limit by **1**. A decrease also enforces its cooldown before increases resume.
- **Congestion:** on terminal `WriteTimeoutException`, `ReadTimeoutException`, `OverloadedException`, or `DriverTimeoutException` (including wrapped, suppressed, or per-node errors in `AllNodesFailedException`), reduce the limit to `max(min-concurrency, floor(limit * decrease-factor))`. No more than one decrease per `decrease-cooldown` interval.
- **Active request timeout:** decrease the limit. **Queued timeout:** remove the request without changing the limit.
- **Other errors:** release the slot without changing the limit. No retries are performed by the throttler.
- **Backpressure:** queue surplus requests FIFO up to `max-queue-size`. Reject additions after the queue fills with `RequestThrottlingException`. Queue time counts towards `basic.request.timeout`.
- **Shutdown:** reject queued requests and permit in-flight requests to complete.

## Important limitations

1. The feedback is **session-wide**, not per Keyspaces physical partition. A physical partition can still exceed 1,000 WCU/s even if global concurrency is low; the algorithm cannot guarantee zero throttling or full 40k WCU/s.
2. The driver signals completion **after its retry policy finishes**. If retries recover intermediate WriteTimeout errors, the throttler sees success and may continue increasing, so consider testing with fewer retries to expose congestion sooner. The throttler does not implement exponential backoff; use a separately verified retry policy if required.
3. Concurrency is not WCU/s. Actual throughput depends on record size, server latency, and how physical partitions are populated.
4. DSBulk's own concurrency/queues plus this driver's queue consume the same overall request timeout. Start with a modest queue and monitor client-side timeout rates.
5. The implementation intentionally does not block request/event-loop threads. It does not share feedback across JVMs and does not affect other importers.
6. Roll out on a test table first. Track **successful inserts/s**, consumed WCU/s, `WriteThrottleEvents`, `StoragePartitionThroughputCapacityExceeded`, p95/p99 latency, and import elapsed time.

## Build / integration

Copy the `src/main/java/.../AmazonKeyspacesAimdThrottler.java` and the JUnit test into your checkout of https://github.com/aws-samples/amazon-keyspaces-java-driver-helpers and run:

```bash
mvn test package
```

The unit test uses JUnit Jupiter 5. The project declares JUnit Jupiter and Maven Surefire for JUnit 5 test discovery.

Use the resulting helper JAR on DSBulk 1.11.1's classpath, **matching its Java driver dependencies** and avoiding duplicate/conflicting driver JARs. Ensure the class is loaded by checking DSBulk startup logs. Run `mvn test package` in a Maven environment with access to dependencies before production use; real Keyspaces performance testing is still required.
