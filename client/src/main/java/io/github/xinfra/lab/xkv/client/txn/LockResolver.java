package io.github.xinfra.lab.xkv.client.txn;

import io.github.xinfra.lab.xkv.client.backoff.Backoffer;
import io.github.xinfra.lab.xkv.proto.Kvrpcpb.LockInfo;

import java.util.List;
import java.util.concurrent.CompletableFuture;

/**
 * Client-side lock resolver. When a snapshot read encounters a lock from a
 * concurrent transaction, the resolver decides the locking transaction's
 * fate (committed / rolled-back / still-alive) and clears the lock.
 *
 * <h3>Two improvements over v1</h3>
 *
 * <ol>
 *   <li><b>Caffeine-backed cache with TTL.</b> v1 used an unbounded
 *       {@code ConcurrentHashMap<Long, Status>} that grew until OOM. v2
 *       caps cache size and expires entries after 5 minutes (configurable
 *       via {@code TxnConfig}).</li>
 *
 *   <li><b>Per-{@code lock_ts} single-flight.</b> v1 let N concurrent
 *       readers each issue CheckTxnStatus + ResolveLock for the same lock,
 *       hammering the primary peer. v2 keys an in-flight map by
 *       {@code lock_ts}: the first caller does the work; later callers
 *       await the resulting {@link CompletableFuture}.</li>
 * </ol>
 */
public interface LockResolver {

    /**
     * Resolve one lock seen by a reader. Returns when the lock is gone OR
     * the resolver determines the lock is still alive (caller backs off).
     *
     * @return {@code true} if the lock was cleared (caller may retry the read);
     *         {@code false} if the lock is still alive (caller must backoff)
     */
    boolean resolveLock(Backoffer bo, long callerStartTs, LockInfo lock);

    /**
     * Resolve a batch of locks discovered by a scan / range read. Same
     * single-flight rules per {@code lock_ts}.
     */
    boolean resolveLocks(Backoffer bo, long callerStartTs, List<LockInfo> locks);

    /** Clear cache; useful for testing. */
    void clearCache();
}
