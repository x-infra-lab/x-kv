package io.github.xinfra.lab.xkv.kv.engine;

import io.github.xinfra.lab.xkv.kv.config.KvConfig;
import org.rocksdb.AbstractEventListener;
import org.rocksdb.BlockBasedTableConfig;
import org.rocksdb.BloomFilter;
import org.rocksdb.Cache;
import org.rocksdb.ColumnFamilyDescriptor;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.ColumnFamilyOptions;
import org.rocksdb.CompactionStyle;
import org.rocksdb.CompressionType;
import org.rocksdb.DBOptions;
import org.rocksdb.LRUCache;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;
import org.rocksdb.SstFileManager;
import org.rocksdb.Statistics;
import org.rocksdb.WriteOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;

/**
 * Single-RocksDB, four-CF implementation of {@link StorageEngine}.
 *
 * <p>This is the load-bearing class for the v2 atomicity contract (Inv-1).
 * All region data lives in CFs {@code default / lock / write}; per-region
 * Raft state (log, hard state, applied index, dedup, snapshot meta) lives
 * in CF {@code raft}. One {@code RocksDB.write(batch, syncOpt)} commits
 * any combination of the four; ONE fsync per Raft entry.
 *
 * <h3>Per-CF tuning</h3>
 *
 * <ul>
 *   <li><b>default</b> — point-lookup-optimized + BlockCache.</li>
 *   <li><b>lock</b>    — small, in-memory hot, point-lookup-optimized.</li>
 *   <li><b>write</b>   — prefix-bloom on userKey (everything except the
 *       trailing 8 bytes that encode {@code commit_ts}). Seek-by-userKey
 *       is the dominant access pattern.</li>
 *   <li><b>raft</b>    — sequential-write-optimized (append-mostly log).</li>
 * </ul>
 */
public final class RocksStorageEngine implements StorageEngine {
    private static final Logger log = LoggerFactory.getLogger(RocksStorageEngine.class);

    static { RocksDB.loadLibrary(); }

    public static final String CF_DEFAULT = "default";
    public static final String CF_LOCK = "lock";
    public static final String CF_WRITE = "write";
    public static final String CF_RAFT = "raft";

    /**
     * Suffix length on write-CF keys = 8 bytes of bigEndian(~commitTs).
     * Used by the prefix bloom filter so seeks-by-userKey skip IO when
     * no version exists.
     */
    public static final int WRITE_CF_TS_SUFFIX = 8;

    private final RocksDB db;
    private final EnumMap<Cf, ColumnFamilyHandle> cfHandles = new EnumMap<>(Cf.class);
    private final List<AutoCloseable> ownedResources = new ArrayList<>();
    private final WriteOptions syncWrite;
    private final WriteOptions noSyncWrite;

    private RocksStorageEngine(RocksDB db, Map<String, ColumnFamilyHandle> handles,
                               List<AutoCloseable> ownedResources) {
        this.db = db;
        cfHandles.put(Cf.DEFAULT, handles.get(CF_DEFAULT));
        cfHandles.put(Cf.LOCK,    handles.get(CF_LOCK));
        cfHandles.put(Cf.WRITE,   handles.get(CF_WRITE));
        cfHandles.put(Cf.RAFT,    handles.get(CF_RAFT));
        this.ownedResources.addAll(ownedResources);
        this.syncWrite = new WriteOptions().setSync(true).setDisableWAL(false);
        this.noSyncWrite = new WriteOptions().setSync(false).setDisableWAL(false);
        this.ownedResources.add(syncWrite);
        this.ownedResources.add(noSyncWrite);
    }

    public static RocksStorageEngine open(Path dataDir, KvConfig.EngineConfig cfg) throws RocksDBException {
        try {
            Files.createDirectories(dataDir);
        } catch (java.io.IOException e) {
            throw new RuntimeException("create data dir " + dataDir, e);
        }

        var owned = new ArrayList<AutoCloseable>();

        // Shared block cache so default + lock + write use one budget.
        var blockCache = new LRUCache(cfg.blockCacheBytes());
        owned.add(blockCache);

        Statistics statistics = cfg.enableStatistics() ? new Statistics() : null;
        if (statistics != null) owned.add(statistics);

        var dbOpts = new DBOptions()
                .setCreateIfMissing(true)
                .setCreateMissingColumnFamilies(true)
                .setMaxBackgroundJobs(cfg.maxBackgroundJobs())
                .setBytesPerSync(1L << 20)
                .setEnablePipelinedWrite(true);
        if (statistics != null) dbOpts.setStatistics(statistics);
        owned.add(dbOpts);

        // Per-CF descriptors with workload-specific tuning.
        var defaultCfOpts = pointLookupCfOptions(blockCache, cfg.writeBufferBytes(), false);
        var lockCfOpts    = pointLookupCfOptions(blockCache, Math.min(cfg.writeBufferBytes(), 16L << 20), true);
        var writeCfOpts   = writeCfOptions(blockCache, cfg.writeBufferBytes());
        var raftCfOpts    = raftCfOptions(blockCache, cfg.writeBufferBytes());
        owned.add(defaultCfOpts);
        owned.add(lockCfOpts);
        owned.add(writeCfOpts);
        owned.add(raftCfOpts);

        var cfDescriptors = List.of(
                new ColumnFamilyDescriptor(CF_DEFAULT.getBytes(StandardCharsets.UTF_8), defaultCfOpts),
                new ColumnFamilyDescriptor(CF_LOCK.getBytes(StandardCharsets.UTF_8), lockCfOpts),
                new ColumnFamilyDescriptor(CF_WRITE.getBytes(StandardCharsets.UTF_8), writeCfOpts),
                new ColumnFamilyDescriptor(CF_RAFT.getBytes(StandardCharsets.UTF_8), raftCfOpts));

        var cfHandlesList = new ArrayList<ColumnFamilyHandle>(cfDescriptors.size());
        var rocks = RocksDB.open(dbOpts, dataDir.toString(), cfDescriptors, cfHandlesList);

        var byName = new java.util.HashMap<String, ColumnFamilyHandle>();
        byName.put(CF_DEFAULT, cfHandlesList.get(0));
        byName.put(CF_LOCK,    cfHandlesList.get(1));
        byName.put(CF_WRITE,   cfHandlesList.get(2));
        byName.put(CF_RAFT,    cfHandlesList.get(3));

        log.info("opened RocksDB at {} (4 CFs)", dataDir);
        return new RocksStorageEngine(rocks, byName, owned);
    }

    private static ColumnFamilyOptions pointLookupCfOptions(Cache cache, long writeBuffer, boolean tinyHot) {
        var bloom = new BloomFilter(10, false);
        var table = new BlockBasedTableConfig()
                .setBlockCache(cache)
                .setFilterPolicy(bloom)
                .setCacheIndexAndFilterBlocks(true)
                .setPinL0FilterAndIndexBlocksInCache(true)
                .setBlockSize(16 * 1024);
        var opts = new ColumnFamilyOptions()
                .setTableFormatConfig(table)
                .setWriteBufferSize(writeBuffer)
                .setMaxWriteBufferNumber(tinyHot ? 2 : 4)
                .setCompactionStyle(CompactionStyle.LEVEL)
                .setLevelCompactionDynamicLevelBytes(true)
                .setCompressionType(CompressionType.LZ4_COMPRESSION);
        opts.optimizeForPointLookup(cache.getUsage() == 0 ? 64L << 20 : 256L << 20);
        return opts;
    }

    private static ColumnFamilyOptions writeCfOptions(Cache cache, long writeBuffer) {
        var bloom = new BloomFilter(10, false);
        var table = new BlockBasedTableConfig()
                .setBlockCache(cache)
                .setFilterPolicy(bloom)
                .setCacheIndexAndFilterBlocks(true)
                .setPinL0FilterAndIndexBlocksInCache(true)
                .setWholeKeyFiltering(true)
                .setBlockSize(16 * 1024);
        // NOTE: prefix-bloom on the userKey portion is a Phase-2 perf win
        // we will re-enable, but it requires the ReadOptions to set
        // total_order_seek=true (or prefix_same_as_start) on every iterator
        // that crosses prefix boundaries. We default to whole-key bloom
        // until the MvccReader paths land their total-order-seek hooks.
        var opts = new ColumnFamilyOptions()
                .setTableFormatConfig(table)
                .setWriteBufferSize(writeBuffer)
                .setMaxWriteBufferNumber(4)
                .setCompactionStyle(CompactionStyle.LEVEL)
                .setLevelCompactionDynamicLevelBytes(true)
                .setCompressionType(CompressionType.LZ4_COMPRESSION);
        return opts;
    }

    private static ColumnFamilyOptions raftCfOptions(Cache cache, long writeBuffer) {
        var table = new BlockBasedTableConfig()
                .setBlockCache(cache)
                .setBlockSize(64 * 1024);
        var opts = new ColumnFamilyOptions()
                .setTableFormatConfig(table)
                .setWriteBufferSize(writeBuffer)
                .setMaxWriteBufferNumber(2)
                .setCompactionStyle(CompactionStyle.LEVEL)
                .setLevelCompactionDynamicLevelBytes(true)
                .setCompressionType(CompressionType.LZ4_COMPRESSION);
        return opts;
    }

    // ===== Reads =====

    @Override
    public byte[] get(Cf cf, byte[] key) {
        try {
            return db.get(handle(cf), key);
        } catch (RocksDBException e) {
            throw StorageException.from("get", e);
        }
    }

    @Override
    public byte[] get(Cf cf, byte[] key, ReadOptions opts) {
        try {
            var ropts = ((RocksReadOptions) opts).inner();
            return db.get(handle(cf), ropts, key);
        } catch (RocksDBException e) {
            throw StorageException.from("get", e);
        }
    }

    @Override
    public List<byte[]> multiGet(Cf cf, List<byte[]> keys) {
        if (keys.isEmpty()) return List.of();
        try {
            var handles = new ArrayList<ColumnFamilyHandle>(keys.size());
            var keyArr = new ArrayList<byte[]>(keys.size());
            for (byte[] k : keys) {
                handles.add(handle(cf));
                keyArr.add(k);
            }
            return db.multiGetAsList(handles, keyArr);
        } catch (RocksDBException e) {
            throw StorageException.from("multiGet", e);
        }
    }

    @Override
    public Iterator newIterator(Cf cf, ReadOptions opts) {
        var ropts = ((RocksReadOptions) opts).inner();
        RocksIterator it = db.newIterator(handle(cf), ropts);
        return new RocksIter(it);
    }

    @Override
    public Snapshot newSnapshot() { return new RocksSnapshot(db.getSnapshot()); }

    // ===== Writes =====

    @Override
    public WriteBatch newWriteBatch() { return new RocksWriteBatch(); }

    @Override
    public void flushWal(boolean sync) {
        try {
            db.flushWal(sync);
        } catch (RocksDBException e) {
            throw StorageException.from("flushWal", e);
        }
    }

    @Override
    public void write(WriteBatch batch, boolean sync) {
        var rb = (RocksWriteBatch) batch;
        try {
            db.write(sync ? syncWrite : noSyncWrite, rb.inner);
        } catch (RocksDBException e) {
            throw StorageException.from("write", e);
        }
    }

    // ===== Maintenance =====

    @Override
    public long approximateSize(Cf cf, byte[] start, byte[] end) {
        try {
            var ranges = new org.rocksdb.Range[] { new org.rocksdb.Range(
                    new org.rocksdb.Slice(start), new org.rocksdb.Slice(end)) };
            long[] result = db.getApproximateSizes(handle(cf), List.of(ranges[0]));
            return result.length == 0 ? 0L : result[0];
        } catch (Throwable t) {
            // approximate-sizes is best-effort
            return 0L;
        }
    }

    @Override
    public void deleteRange(WriteBatch batch, Cf cf, byte[] start, byte[] end) {
        ((RocksWriteBatch) batch).deleteRangeInternal(handle(cf), start, end);
    }

    @Override
    public void ingestSst(Cf cf, List<Path> sstFiles) {
        try (var opts = new org.rocksdb.IngestExternalFileOptions()) {
            var paths = sstFiles.stream().map(Path::toString).toList();
            db.ingestExternalFile(handle(cf), paths, opts);
        } catch (RocksDBException e) {
            throw StorageException.from("ingestSst", e);
        }
    }

    @Override
    public void compactRange(Cf cf, byte[] start, byte[] end) {
        try {
            db.compactRange(handle(cf), start, end);
        } catch (RocksDBException e) {
            throw StorageException.from("compactRange", e);
        }
    }

    @Override
    public ReadOptions newReadOptions() { return new RocksReadOptions(); }

    @Override
    public void close() {
        // Close RocksDB before owned options/caches; reverse construction order.
        for (var h : cfHandles.values()) h.close();
        db.close();
        for (int i = ownedResources.size() - 1; i >= 0; i--) {
            try { ownedResources.get(i).close(); } catch (Exception ignored) {}
        }
    }

    public RocksDB rawDb() { return db; }
    public ColumnFamilyHandle handle(Cf cf) { return cfHandles.get(cf); }

    // ============================================================
    // Companion implementations
    // ============================================================

    private final class RocksWriteBatch implements WriteBatch {
        private final org.rocksdb.WriteBatch inner = new org.rocksdb.WriteBatch();

        @Override public void put(Cf cf, byte[] key, byte[] value) {
            try { inner.put(handle(cf), key, value); }
            catch (RocksDBException e) { throw StorageException.from("batch.put", e); }
        }
        @Override public void delete(Cf cf, byte[] key) {
            try { inner.delete(handle(cf), key); }
            catch (RocksDBException e) { throw StorageException.from("batch.delete", e); }
        }
        @Override public void deleteRange(Cf cf, byte[] start, byte[] end) {
            try { inner.deleteRange(handle(cf), start, end); }
            catch (RocksDBException e) { throw StorageException.from("batch.deleteRange", e); }
        }
        void deleteRangeInternal(ColumnFamilyHandle h, byte[] start, byte[] end) {
            try { inner.deleteRange(h, start, end); }
            catch (RocksDBException e) { throw StorageException.from("batch.deleteRange", e); }
        }
        @Override public int count() { return inner.count(); }
        @Override public long byteSize() { return inner.getDataSize(); }
        @Override public void close() { inner.close(); }
    }

    /**
     * Long-lived snapshots pin SST files (block compaction, eat disk). They
     * MUST be released via {@code RocksDB.releaseSnapshot} when the holder
     * is done — closing a {@code ReadOptions} does not release the
     * underlying snapshot.
     */
    private final class RocksSnapshot implements Snapshot {
        final org.rocksdb.Snapshot inner;
        private volatile boolean released = false;

        RocksSnapshot(org.rocksdb.Snapshot s) { this.inner = s; }

        @Override
        public synchronized void close() {
            if (released) return;
            released = true;
            try {
                db.releaseSnapshot(inner);
                inner.close();
            } catch (Throwable t) {
                log.warn("releaseSnapshot failed", t);
            }
        }
    }

    private static final class RocksReadOptions implements ReadOptions {
        private final org.rocksdb.ReadOptions inner = new org.rocksdb.ReadOptions();
        org.rocksdb.ReadOptions inner() { return inner; }
        @Override public ReadOptions snapshot(Snapshot snap) {
            inner.setSnapshot(((RocksSnapshot) snap).inner);
            return this;
        }
        @Override public ReadOptions iterateLowerBound(byte[] lower) {
            inner.setIterateLowerBound(new org.rocksdb.Slice(lower));
            return this;
        }
        @Override public ReadOptions iterateUpperBound(byte[] upper) {
            inner.setIterateUpperBound(new org.rocksdb.Slice(upper));
            return this;
        }
        @Override public ReadOptions fillCache(boolean fill) {
            inner.setFillCache(fill); return this;
        }
        @Override public ReadOptions prefixSameAsStart(boolean v) {
            inner.setPrefixSameAsStart(v); return this;
        }
    }

    private static final class RocksIter implements Iterator {
        private final RocksIterator inner;
        RocksIter(RocksIterator it) { this.inner = it; }
        @Override public boolean isValid() { return inner.isValid(); }
        @Override public byte[] key()      { return inner.key(); }
        @Override public byte[] value()    { return inner.value(); }
        @Override public void seek(byte[] key) { inner.seek(key); }
        @Override public void seekForPrev(byte[] key) { inner.seekForPrev(key); }
        @Override public void next()  { inner.next(); }
        @Override public void prev()  { inner.prev(); }
        @Override public void close() { inner.close(); }
    }

    /** Suppress an unused import warning while keeping the listener type imported for Phase 1+. */
    @SuppressWarnings("unused")
    private static AbstractEventListener listenerKeepalive() { return null; }

    /** Reserved for Phase 1's disk-full reporting. */
    @SuppressWarnings("unused")
    private static SstFileManager sstManagerKeepalive() { return null; }
}
