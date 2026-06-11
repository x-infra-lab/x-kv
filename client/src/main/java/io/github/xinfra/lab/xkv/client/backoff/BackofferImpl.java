package io.github.xinfra.lab.xkv.client.backoff;

import io.github.xinfra.lab.xkv.client.config.ClientConfig;

import java.time.Duration;
import java.time.Instant;
import java.util.EnumMap;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Exponential-backoff-with-jitter implementation.
 *
 * <p>One {@code BackofferImpl} per logical request. Per-{@link Reason} state
 * is tracked separately so a long stretch of region-miss retries doesn't
 * inflate the next not-leader sleep.
 *
 * <p>Per call to {@link #backoff}:
 * <pre>
 *   sleep = min(cap, base * 2^attempt) ± jitter
 * </pre>
 *
 * <p>Total sleep across all reasons is bounded by
 * {@code maxOverallElapsedMs}; exceeding it throws.
 */
public final class BackofferImpl implements Backoffer {

    private final ClientConfig.BackoffConfig cfg;
    private final Instant deadline;
    private final Instant startedAt;

    private final Map<Reason, Integer> attempts = new EnumMap<>(Reason.class);
    private long totalElapsedMs = 0;

    public BackofferImpl(ClientConfig.BackoffConfig cfg) {
        this(cfg, null);
    }

    /**
     * External-deadline-aware constructor. The effective deadline is the
     * SOONER of {@code cfg.maxOverallElapsedMs()} from now and the supplied
     * {@code externalDeadline}. Use this when the caller has an explicit
     * deadline (a gRPC {@code Context.getDeadline()}, an SQL statement
     * timeout, etc.) so the backoff budget never extends past the moment
     * the upstream consumer stops caring about the answer.
     */
    public BackofferImpl(ClientConfig.BackoffConfig cfg, Instant externalDeadline) {
        this.cfg = cfg;
        this.startedAt = Instant.now();
        var internal = startedAt.plusMillis(cfg.maxOverallElapsedMs());
        this.deadline = externalDeadline == null || externalDeadline.isAfter(internal)
                ? internal
                : externalDeadline;
    }

    /** Internal: deadline is already computed (used by {@link #fork}). */
    private static BackofferImpl forked(ClientConfig.BackoffConfig cfg, Instant deadline) {
        var b = new BackofferImpl(cfg, deadline);
        return b;
    }

    @Override
    public void backoff(Reason r, String contextMessage) throws BackoffExceededException {
        if (Instant.now().isAfter(deadline)) {
            throw new BackoffExceededException("backoff budget exceeded: " + contextMessage);
        }
        long base = baseFor(r);
        long cap = capFor(r);
        int attempt = attempts.merge(r, 1, Integer::sum);
        long expBase = base * (1L << Math.min(attempt - 1, 10));   // cap exponent
        long sleepMs = Math.min(cap, expBase);
        long jitter = ThreadLocalRandom.current().nextLong(Math.max(1, sleepMs / 4));
        sleepMs = Math.max(1, sleepMs - jitter / 2 + jitter);
        // Don't oversleep past the deadline.
        long remaining = Math.max(0, deadline.toEpochMilli() - System.currentTimeMillis());
        sleepMs = Math.min(sleepMs, remaining);
        if (sleepMs <= 0) {
            throw new BackoffExceededException("backoff budget exhausted: " + contextMessage);
        }
        try {
            Thread.sleep(sleepMs);
            totalElapsedMs += sleepMs;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new BackoffExceededException("interrupted: " + contextMessage);
        }
    }

    private long baseFor(Reason r) {
        return switch (r) {
            case REGION_MISS, EPOCH_NOT_MATCH -> cfg.regionMissBaseMs();
            case TXN_LOCK -> cfg.txnLockBaseMs();
            case SERVER_BUSY, DATA_NOT_READY, MAX_TS_NOT_SYNCED -> cfg.serverBusyBaseMs();
            case NETWORK -> cfg.networkBaseMs();
            case NOT_LEADER, STALE_COMMAND, OTHER -> cfg.notLeaderBaseMs();
        };
    }

    private long capFor(Reason r) {
        return switch (r) {
            case REGION_MISS, EPOCH_NOT_MATCH -> cfg.regionMissCapMs();
            case TXN_LOCK -> cfg.txnLockCapMs();
            case SERVER_BUSY, DATA_NOT_READY, MAX_TS_NOT_SYNCED -> cfg.serverBusyCapMs();
            case NETWORK -> cfg.networkCapMs();
            case NOT_LEADER, STALE_COMMAND, OTHER -> cfg.notLeaderCapMs();
        };
    }

    @Override public Duration totalElapsed() { return Duration.ofMillis(totalElapsedMs); }

    @Override public Instant deadline() { return deadline; }

    @Override
    public Backoffer fork() { return forked(cfg, deadline); }
}
