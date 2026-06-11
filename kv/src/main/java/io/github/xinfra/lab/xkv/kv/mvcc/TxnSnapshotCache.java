package io.github.xinfra.lab.xkv.kv.mvcc;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.RemovalListener;
import io.github.xinfra.lab.xkv.kv.engine.StorageEngine;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Per-transaction RocksDB snapshot cache, keyed by {@code start_ts}.
 *
 * <h3>Status: legacy / unused</h3>
 *
 * <p>An earlier iteration pinned one snapshot per {@code start_ts} so that
 * every cross-key read of a transaction observed the same physical
 * point-in-time. That design fought the lock-resolver retry loop: if
 * txn A's lock was in the snapshot but resolver removed it from the live
 * lock CF, the reader saw the lock forever and looped indefinitely.
 *
 * <p>The replacement is TiKV-shape: <strong>per-request</strong> snapshots
 * (one {@code db.getSnapshot()} per RPC, released on return) plus the
 * {@link MaxTsTracker} max_ts mechanism for cross-key SI. See
 * {@code TransactionService.kvGet}. This class is retained as a passive
 * field on the service to avoid touching the constructor-shape of every
 * call site; it is intentionally never invoked.
 */
public final class TxnSnapshotCache implements AutoCloseable {
    private static final Logger log = LoggerFactory.getLogger(TxnSnapshotCache.class);

    private final StorageEngine engine;
    private final Cache<Long, StorageEngine.Snapshot> snapshots;
    private final AtomicLong opensTotal = new AtomicLong();
    private final AtomicLong evictionsTotal = new AtomicLong();

    public TxnSnapshotCache(StorageEngine engine) {
        this(engine, 10_000, Duration.ofMinutes(5));
    }

    public TxnSnapshotCache(StorageEngine engine, int maxSize, Duration ttl) {
        this.engine = engine;
        RemovalListener<Long, StorageEngine.Snapshot> remover = (key, value, cause) -> {
            if (value == null) return;
            evictionsTotal.incrementAndGet();
            try { value.close(); }
            catch (Throwable t) { log.warn("snapshot close failed startTs={}", key, t); }
        };
        this.snapshots = Caffeine.newBuilder()
                .maximumSize(maxSize)
                .expireAfterAccess(ttl)
                .removalListener(remover)
                .build();
    }

    /**
     * Returns the snapshot pinned at this {@code start_ts}, opening a
     * fresh one on first use. The returned snapshot is OWNED by the
     * cache; callers must NOT close it directly — that's the cache's
     * job on {@link #release} or eviction.
     */
    public StorageEngine.Snapshot pinFor(long startTs) {
        return snapshots.get(startTs, k -> {
            opensTotal.incrementAndGet();
            return engine.newSnapshot();
        });
    }

    /** Explicit release on commit / rollback. Calls {@code Snapshot.close} via the removal listener. */
    public void release(long startTs) {
        snapshots.invalidate(startTs);
    }

    /** Diagnostic: how many snapshots have been opened over this cache's lifetime. */
    public long opensTotal() { return opensTotal.get(); }

    /** Diagnostic: how many snapshots have been released (explicit or via eviction). */
    public long evictionsTotal() { return evictionsTotal.get(); }

    /** Current size — how many snapshots currently pinned. */
    public long size() {
        snapshots.cleanUp();
        return snapshots.estimatedSize();
    }

    @Override
    public void close() {
        snapshots.invalidateAll();
        snapshots.cleanUp();
    }
}
