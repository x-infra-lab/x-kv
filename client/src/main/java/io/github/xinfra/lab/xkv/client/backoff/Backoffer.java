package io.github.xinfra.lab.xkv.client.backoff;

import java.time.Duration;

/**
 * Per-request retry budget + per-error-class exponential backoff.
 *
 * <p>v1 used a flat {@code Thread.sleep(100)} on every retry, which (a)
 * never escalated for persistent failures, (b) thundered the herd on big
 * outages, and (c) had no overall deadline. v2 mirrors client-go:
 *
 * <ul>
 *   <li>One {@code Backoffer} instance per logical request</li>
 *   <li>Each {@link Reason} has its own base / cap / jitter</li>
 *   <li>{@link #backoff} sleeps with exponential growth + ±10% jitter</li>
 *   <li>The backoffer aborts when total elapsed time exceeds
 *       {@code maxOverallElapsedMs}, throwing {@link BackoffExceededException}</li>
 * </ul>
 *
 * <p>Backoffers are NOT thread-safe — one per logical request lifetime.
 */
public interface Backoffer {

    enum Reason {
        REGION_MISS,        // RegionError other than NotLeader
        NOT_LEADER,         // RegionError NotLeader (leader hint included)
        STALE_COMMAND,      // term moved on
        TXN_LOCK,           // KeyError.locked while reading; caller resolves first
        SERVER_BUSY,        // RegionError ServerIsBusy
        NETWORK,            // gRPC UNAVAILABLE / DEADLINE_EXCEEDED
        DATA_NOT_READY,     // stale-read fallback to leader
        MAX_TS_NOT_SYNCED,
        EPOCH_NOT_MATCH,
        OTHER
    }

    /**
     * Sleep for the {@code Reason}'s next backoff slot. Throws if elapsed
     * time exceeds the budget.
     */
    void backoff(Reason r, String contextMessage) throws BackoffExceededException;

    /** Time spent sleeping in this backoffer so far. */
    Duration totalElapsed();

    /** Hard deadline (instantiation + budget). */
    java.time.Instant deadline();

    /**
     * Fork a child backoffer that shares the same overall deadline. Used
     * by sub-RPCs (e.g. lock resolver) so a long lock-wait does not consume
     * the parent request's entire budget twice.
     */
    Backoffer fork();

    class BackoffExceededException extends RuntimeException {
        public BackoffExceededException(String message) { super(message); }
    }
}
