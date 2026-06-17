package io.github.xinfra.lab.xkv.kv.engine;

import java.util.List;

/**
 * Per-store storage abstraction.
 *
 * <p>Wraps a single RocksDB instance with three column families:
 * <ul>
 *   <li>{@code default} — value data (raw + MVCC)</li>
 *   <li>{@code lock}    — Percolator locks (one entry per locked key)</li>
 *   <li>{@code write}   — MVCC commit / rollback records</li>
 * </ul>
 *
 * <h3>Atomicity contract (the load-bearing one)</h3>
 *
 * <p>The v1 implementation wrote {@code appliedIndex} via {@code logStorage}
 * (one RocksDB) and the business batch via {@code kvDb} (a different RocksDB),
 * each with its own {@code sync=true} fsync. A crash in the gap between the
 * two left {@code appliedIndex < actually-applied}. After restart, the same
 * Raft entry replayed: a {@code commit} entry tried to delete a lock that
 * was already gone and failed; the dedup map didn't capture it; the client
 * received a "txn not committed" error for a transaction that <em>was</em>
 * committed.
 *
 * <p><strong>Hard rule for v2:</strong> {@code appliedIndex}, the dedup map,
 * and all CF mutations for one Raft entry MUST live in a single
 * {@link WriteBatch} flushed via a single {@code db.write(syncOpt)} call.
 * Implementations that store Raft state in a separate RocksDB instance are
 * forbidden.
 */
public interface StorageEngine extends AutoCloseable {

    /** Stable column family handle identifiers. */
    enum Cf {
        DEFAULT,
        LOCK,
        WRITE,
        /** Per-region Raft state: log entries, hard state, applied index, dedup. */
        RAFT
    }

    // ---- Reads ----

    byte[] get(Cf cf, byte[] key);

    /**
     * Snapshot-aware get. When {@code opts.snapshot(...)} is set, returns
     * the value as of that snapshot's sequence number — required for cross-CF
     * consistency (so a reader's lock CF check and write CF lookup observe
     * the SAME point in time).
     */
    byte[] get(Cf cf, byte[] key, ReadOptions opts);

    /**
     * Multi-key fetch from one CF. Implementations should use RocksDB's
     * {@code multiGet} which is materially faster than N {@code get}s.
     */
    List<byte[]> multiGet(Cf cf, List<byte[]> keys);

    Iterator newIterator(Cf cf, ReadOptions opts);

    /** A consistent snapshot across all CFs — see snapshot streaming docs. */
    Snapshot newSnapshot();

    // ---- Writes ----

    /** Open a fresh write batch; not thread-safe. */
    WriteBatch newWriteBatch();

    /** Atomically apply a write batch. {@code sync == true} ⇒ fsync before return. */
    void write(WriteBatch batch, boolean sync);

    /**
     * Flush the WAL. With {@code sync == true} this performs an fsync on the
     * WAL file, durably persisting every write made via {@link #write} since
     * the last sync. Used by the apply loop to amortize one fsync over many
     * per-entry batches: each entry writes with sync=false (so subsequent
     * entries' readers see its memtable updates), and the round ends with a
     * single {@code flushWal(true)}.
     */
    void flushWal(boolean sync);

    // ---- Maintenance ----

    /** Approximate on-disk size for [start, end) on one CF. */
    long approximateSize(Cf cf, byte[] start, byte[] end);

    /**
     * Range delete optimized for large ranges. Backed by RocksDB's
     * {@code DeleteRange}; faster than N point deletes by orders of magnitude.
     */
    void deleteRange(WriteBatch batch, Cf cf, byte[] start, byte[] end);

    /** Bulk-ingest one or more pre-built SST files into a CF. */
    void ingestSst(Cf cf, List<java.nio.file.Path> sstFiles);

    /** Manual compaction — used by GC after large drops. */
    void compactRange(Cf cf, byte[] start, byte[] end);

    @Override void close();

    // ---- Companion types ----

    interface Iterator extends AutoCloseable {
        boolean isValid();
        byte[] key();
        byte[] value();
        void seek(byte[] key);
        void seekForPrev(byte[] key);
        void next();
        void prev();
        /** Throws {@link StorageException} if the iterator encountered an I/O error. */
        default void checkStatus() {}
        @Override void close();
    }

    interface WriteBatch extends AutoCloseable {
        void put(Cf cf, byte[] key, byte[] value);
        void delete(Cf cf, byte[] key);
        void deleteRange(Cf cf, byte[] start, byte[] end);
        int count();
        long byteSize();
        @Override void close();
    }

    interface Snapshot extends AutoCloseable {
        @Override void close();
    }

    interface ReadOptions extends AutoCloseable {
        ReadOptions snapshot(Snapshot snap);
        ReadOptions iterateLowerBound(byte[] lower);
        ReadOptions iterateUpperBound(byte[] upper);
        ReadOptions fillCache(boolean fill);
        ReadOptions prefixSameAsStart(boolean v);
        @Override void close();
    }

    /** Fresh ReadOptions; the engine binds defaults. */
    ReadOptions newReadOptions();
}
