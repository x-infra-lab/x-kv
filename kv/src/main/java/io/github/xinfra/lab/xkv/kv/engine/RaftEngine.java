package io.github.xinfra.lab.xkv.kv.engine;

/**
 * Per-region Raft persistence facade. Co-locates Raft log + hard state +
 * applied index + dedup map inside the same {@link StorageEngine} so
 * apply atomicity is a single-writebatch property — see
 * {@link StorageEngine} class doc.
 *
 * <p>The Raft library calls into this engine via its own {@code Storage}
 * SPI (x-raft-lib). This interface is the higher-level wrapper our state
 * machines use directly.
 */
public interface RaftEngine extends AutoCloseable {

    long regionId();

    // ---- Hard state ----

    long currentTerm();
    long votedFor();
    long commitIndex();
    void saveHardState(long term, long votedFor, long commit, StorageEngine.WriteBatch batch);

    // ---- Log ----

    long firstIndex();
    long lastIndex();

    /** Append entries (already serialized) into the same batch as state. */
    void appendEntries(StorageEngine.WriteBatch batch, byte[][] serializedEntries);

    /** Truncate log entries with index ≤ {@code uptoIndex}. */
    void compactLog(long uptoIndex, StorageEngine.WriteBatch batch);

    // ---- Applied index + dedup ----
    //
    // Both live in the SAME RocksDB instance + SAME write batch as the
    // business mutations they describe. This is the v2 invariant.

    long appliedIndex();

    void saveAppliedIndex(long index, StorageEngine.WriteBatch batch);

    long lastDedupReqId(long clientId);

    void recordDedup(long clientId, long requestId, StorageEngine.WriteBatch batch);

    /**
     * Drop one client's dedup entry once it has been quiescent for a while.
     * Bounds memory growth — v1 grew this map without limit.
     */
    void evictDedup(long clientId, StorageEngine.WriteBatch batch);

    // ---- Snapshot exchange ----

    /** Latest applied snapshot meta (term, index). */
    SnapshotMeta lastSnapshotMeta();

    void saveSnapshotMeta(SnapshotMeta meta, StorageEngine.WriteBatch batch);

    record SnapshotMeta(long term, long index, byte[] startKey, byte[] endKey) {}

    /** Wipe all per-region state during region destroy. */
    void destroy();

    @Override void close();
}
