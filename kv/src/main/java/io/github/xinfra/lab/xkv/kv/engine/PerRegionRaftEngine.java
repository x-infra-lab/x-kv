package io.github.xinfra.lab.xkv.kv.engine;

import io.github.xinfra.lab.xkv.proto.Metapb;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static io.github.xinfra.lab.xkv.kv.engine.RaftCfKeys.*;

/**
 * Default {@link RaftEngine} implementation. Backs all per-region Raft state
 * onto the shared {@link StorageEngine}'s {@code RAFT} CF (see
 * {@link RaftCfKeys} for the key layout). All writes go through caller-
 * provided {@link StorageEngine.WriteBatch} so the apply pipeline can
 * fold mutations + applied-index + dedup into one atomic batch (Inv-1).
 *
 * <p>In-memory cached state (lastIndex, firstIndex, term, votedFor,
 * appliedIndex, dedup map) is reloaded from disk on construction; subsequent
 * {@code save*} calls update the cache after the batch is appended (the
 * caller is responsible for serializing apply-loop access; this class is
 * thread-safe only at the granularity that Java's volatile / ConcurrentMap
 * provides).
 */
public final class PerRegionRaftEngine implements RaftEngine {
    private static final Logger log = LoggerFactory.getLogger(PerRegionRaftEngine.class);

    private final StorageEngine storage;
    private final long regionId;
    private volatile boolean closed;

    private volatile long currentTerm;
    private volatile long votedFor;
    private volatile long commitIndex;
    private volatile long firstIndex;       // first log index that has not been compacted
    private volatile long lastIndex;        // highest persisted index
    private volatile long appliedIndex;
    private volatile SnapshotMeta lastSnapshotMeta;
    private volatile long persistedMaxTs;
    private volatile byte[] mergeState;       // null = not merging; non-null = serialized target descriptor

    /** clientId → highest seen requestId. Reload from disk on open. */
    private final ConcurrentHashMap<Long, Long> dedupCache = new ConcurrentHashMap<>();

    /**
     * Open or recover. After construction the in-memory cache is consistent
     * with on-disk state.
     */
    public PerRegionRaftEngine(StorageEngine storage, long regionId) {
        this.storage = storage;
        this.regionId = regionId;
        reload();
    }

    private void reload() {
        // Hard state: [8B term][8B vote][8B commit].
        byte[] meta = storage.get(StorageEngine.Cf.RAFT, metaKey(regionId));
        if (meta != null && meta.length >= 16) {
            this.currentTerm = bytesToLong(slice(meta, 0, 8));
            this.votedFor    = bytesToLong(slice(meta, 8, 8));
            if (meta.length >= 24) {
                this.commitIndex = bytesToLong(slice(meta, 16, 8));
            }
        }

        byte[] applied = storage.get(StorageEngine.Cf.RAFT, appliedKey(regionId));
        this.appliedIndex = applied == null ? 0 : bytesToLong(applied);

        byte[] snapMetaBytes = storage.get(StorageEngine.Cf.RAFT, snapshotMetaKey(regionId));
        if (snapMetaBytes != null) {
            this.lastSnapshotMeta = decodeSnapshotMeta(snapMetaBytes);
            // After a snapshot install, firstIndex == snap.index + 1 (no log
            // entries below); lastIndex == max(snap.index, on-disk-max).
        }

        // max_ts persisted by the apply loop. Reloaded so a restarted leader
        // doesn't serve a prewrite that breaks an in-flight reader's SI.
        byte[] maxTsBytes = storage.get(StorageEngine.Cf.RAFT, maxTsKey(regionId));
        this.persistedMaxTs = maxTsBytes == null ? 0L : bytesToLong(maxTsBytes);

        // Merge state: when this region is mid-merge, a serialized
        // PrepareMergeProposal lives here. The apply path uses presence as
        // a "merging" gate: business writes are rejected while set. The
        // gate survives restart so a leader-crash mid-merge doesn't let
        // writes slip through on the next leader's apply.
        this.mergeState = storage.get(StorageEngine.Cf.RAFT, mergeStateKey(regionId));

        // Index range: scan the log type prefix.
        scanIndexRange();

        // Dedup cache: scan the dedup prefix once on open.
        loadDedup();
    }

    private void scanIndexRange() {
        var lower = regionTypePrefix(regionId, TYPE_LOG);
        var upper = regionTypePrefix(regionId + 1, TYPE_LOG);
        // Default lastIndex / firstIndex when the log is empty:
        long snapIdx = lastSnapshotMeta != null ? lastSnapshotMeta.index() : 0;
        this.firstIndex = snapIdx + 1;
        this.lastIndex = snapIdx;

        try (var ro = storage.newReadOptions().iterateLowerBound(lower).iterateUpperBound(upper);
             var it = storage.newIterator(StorageEngine.Cf.RAFT, ro)) {
            it.seek(lower);
            if (it.isValid()) {
                this.firstIndex = logIndexFromKey(it.key());
            }
            // Walk to the end: seekForPrev gives us last in range.
            it.seekForPrev(logKey(regionId, Long.MAX_VALUE));
            if (it.isValid() && it.key()[0] == TYPE_LOG && logRegionIdFromKey(it.key()) == regionId) {
                this.lastIndex = Math.max(this.lastIndex, logIndexFromKey(it.key()));
            }
        }
    }

    private void loadDedup() {
        var lower = regionTypePrefix(regionId, TYPE_DEDUP);
        var upper = regionTypePrefix(regionId + 1, TYPE_DEDUP);
        try (var ro = storage.newReadOptions().iterateLowerBound(lower).iterateUpperBound(upper);
             var it = storage.newIterator(StorageEngine.Cf.RAFT, ro)) {
            for (it.seek(lower); it.isValid(); it.next()) {
                if (it.key()[0] != TYPE_DEDUP) break;
                long clientId = dedupClientIdFromKey(it.key());
                long reqId = bytesToLong(it.value());
                dedupCache.put(clientId, reqId);
            }
        }
        log.debug("region={} reloaded dedup entries={}", regionId, dedupCache.size());
    }

    private void checkOpen() {
        if (closed) {
            throw new StorageException("raft engine closed for region " + regionId);
        }
    }

    @Override public long regionId() { return regionId; }

    // ---- hard state ----

    @Override public long currentTerm() { return currentTerm; }
    @Override public long votedFor()    { return votedFor; }
    @Override public long commitIndex() { return commitIndex; }

    @Override
    public void saveHardState(long term, long votedFor, long commit, StorageEngine.WriteBatch batch) {
        var packed = new byte[24];
        System.arraycopy(longToBytes(term), 0, packed, 0, 8);
        System.arraycopy(longToBytes(votedFor), 0, packed, 8, 8);
        System.arraycopy(longToBytes(commit), 0, packed, 16, 8);
        batch.put(StorageEngine.Cf.RAFT, metaKey(regionId), packed);
        this.currentTerm = term;
        this.votedFor = votedFor;
        this.commitIndex = commit;
    }

    // ---- log ----

    @Override public long firstIndex() { return firstIndex; }
    @Override public long lastIndex()  { return lastIndex; }

    @Override
    public void appendEntries(StorageEngine.WriteBatch batch, byte[][] serializedEntries) {
        long base = lastIndex;
        for (int i = 0; i < serializedEntries.length; i++) {
            long idx = base + i + 1;
            batch.put(StorageEngine.Cf.RAFT, logKey(regionId, idx), serializedEntries[i]);
        }
        if (serializedEntries.length > 0) {
            this.lastIndex = base + serializedEntries.length;
        }
    }

    /**
     * Truncate-from-end: drop entries with index > {@code newLastIndex}.
     * Used to overwrite a divergent suffix after a leader step-down.
     */
    public void truncateAfter(long newLastIndex, StorageEngine.WriteBatch batch) {
        if (newLastIndex >= lastIndex) return;
        batch.deleteRange(StorageEngine.Cf.RAFT,
                logKey(regionId, newLastIndex + 1),
                logKey(regionId, lastIndex + 1));
        this.lastIndex = newLastIndex;
    }

    @Override
    public void compactLog(long uptoIndex, StorageEngine.WriteBatch batch) {
        if (uptoIndex < firstIndex) return;
        batch.deleteRange(StorageEngine.Cf.RAFT,
                logKey(regionId, firstIndex),
                logKey(regionId, uptoIndex + 1));
        this.firstIndex = uptoIndex + 1;
    }

    /**
     * Read one log entry. Returns null if the index is below firstIndex
     * (compacted) or above lastIndex (does not exist yet).
     */
    public byte[] entryAt(long index) {
        checkOpen();
        if (index < firstIndex || index > lastIndex) return null;
        return storage.get(StorageEngine.Cf.RAFT, logKey(regionId, index));
    }

    // ---- applied + dedup ----

    @Override public long appliedIndex() { return appliedIndex; }

    @Override
    public void saveAppliedIndex(long index, StorageEngine.WriteBatch batch) {
        batch.put(StorageEngine.Cf.RAFT, appliedKey(regionId), longToBytes(index));
        this.appliedIndex = index;
    }

    @Override
    public long lastDedupReqId(long clientId) {
        return dedupCache.getOrDefault(clientId, 0L);
    }

    @Override
    public void recordDedup(long clientId, long requestId, StorageEngine.WriteBatch batch) {
        if (clientId == 0) return;
        batch.put(StorageEngine.Cf.RAFT, dedupKey(regionId, clientId), longToBytes(requestId));
        dedupCache.merge(clientId, requestId, Math::max);
    }

    @Override
    public void evictDedup(long clientId, StorageEngine.WriteBatch batch) {
        batch.delete(StorageEngine.Cf.RAFT, dedupKey(regionId, clientId));
        dedupCache.remove(clientId);
    }

    /** Snapshot the current dedup state. */
    public Map<Long, Long> dedupSnapshot() { return new HashMap<>(dedupCache); }

    // ---- snapshot meta ----

    @Override public SnapshotMeta lastSnapshotMeta() { return lastSnapshotMeta; }

    @Override
    public void saveSnapshotMeta(SnapshotMeta meta, StorageEngine.WriteBatch batch) {
        batch.put(StorageEngine.Cf.RAFT, snapshotMetaKey(regionId), encodeSnapshotMeta(meta));
        this.lastSnapshotMeta = meta;
    }

    /** Value reloaded from disk on startup — the apply loop wrote this. */
    public long persistedMaxTs() { return persistedMaxTs; }

    /**
     * Stage a max_ts persistence into the apply batch. Caller should only
     * call this when {@code ts > persistedMaxTs} so we don't bloat the
     * batch with no-op writes.
     */
    public void saveMaxTs(long ts, StorageEngine.WriteBatch batch) {
        batch.put(StorageEngine.Cf.RAFT, maxTsKey(regionId), longToBytes(ts));
        this.persistedMaxTs = ts;
    }

    /** True iff this region is mid-merge (PrepareMerge applied, not yet rolled back). */
    public boolean isMerging() { return mergeState != null; }

    /** Raw serialized {@code PrepareMergeProposal} — null if not merging. */
    public byte[] mergeState() { return mergeState; }

    /** Stage a "merging" marker into the batch. {@code targetBytes} is the serialized proposal. */
    public void saveMergeState(byte[] targetBytes, StorageEngine.WriteBatch batch) {
        batch.put(StorageEngine.Cf.RAFT, mergeStateKey(regionId), targetBytes);
        this.mergeState = targetBytes;
    }

    /** Clear the merging marker — called on RollbackMerge apply. */
    public void clearMergeState(StorageEngine.WriteBatch batch) {
        batch.delete(StorageEngine.Cf.RAFT, mergeStateKey(regionId));
        this.mergeState = null;
    }

    /** Persist the {@code Region} associated with this peer. */
    public void saveRegion(Metapb.Region region, StorageEngine.WriteBatch batch) {
        batch.put(StorageEngine.Cf.RAFT, regionKey(regionId), region.toByteArray());
    }

    public Metapb.Region region() {
        checkOpen();
        byte[] v = storage.get(StorageEngine.Cf.RAFT, regionKey(regionId));
        if (v == null) return null;
        try {
            return Metapb.Region.parseFrom(v);
        } catch (com.google.protobuf.InvalidProtocolBufferException e) {
            throw new RuntimeException("region parse failed", e);
        }
    }

    @Override
    public void destroy() {
        checkOpen();
        // Drop everything under the region's RAFT-CF subtree, then mark the
        // applied range in the data CFs for later GC by the store.
        try (var b = storage.newWriteBatch()) {
            for (byte type : new byte[] {
                    TYPE_LOG, TYPE_META, TYPE_APPLIED, TYPE_DEDUP,
                    TYPE_SNAPMETA, TYPE_CONFSTATE, TYPE_REGION,
                    TYPE_MAX_TS, TYPE_MERGE_STATE }) {
                b.deleteRange(StorageEngine.Cf.RAFT,
                        regionTypePrefix(regionId, type),
                        regionTypePrefix(regionId + 1, type));
            }
            storage.write(b, true);
        }
        dedupCache.clear();
        firstIndex = 1;
        lastIndex = 0;
        appliedIndex = 0;
        lastSnapshotMeta = null;
        currentTerm = 0;
        votedFor = 0;
        persistedMaxTs = 0;
        mergeState = null;
    }

    @Override public void close() {
        closed = true;
    }

    // ---- helpers ----

    private static byte[] slice(byte[] src, int off, int len) {
        var dst = new byte[len];
        System.arraycopy(src, off, dst, 0, len);
        return dst;
    }

    private static byte[] encodeSnapshotMeta(SnapshotMeta m) {
        // [term=8B][index=8B][startKeyLen=4B][startKey][endKeyLen=4B][endKey]
        byte[] sk = m.startKey() == null ? new byte[0] : m.startKey();
        byte[] ek = m.endKey() == null ? new byte[0] : m.endKey();
        var bb = java.nio.ByteBuffer.allocate(8 + 8 + 4 + sk.length + 4 + ek.length);
        bb.putLong(m.term()).putLong(m.index());
        bb.putInt(sk.length).put(sk);
        bb.putInt(ek.length).put(ek);
        return bb.array();
    }

    private static SnapshotMeta decodeSnapshotMeta(byte[] b) {
        var bb = java.nio.ByteBuffer.wrap(b);
        long term = bb.getLong();
        long index = bb.getLong();
        var sk = new byte[bb.getInt()]; bb.get(sk);
        var ek = new byte[bb.getInt()]; bb.get(ek);
        return new SnapshotMeta(term, index, sk, ek);
    }
}
