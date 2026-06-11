package io.github.xinfra.lab.xkv.kv.raft;

import io.github.xinfra.lab.raft.RaftException;
import io.github.xinfra.lab.raft.Storage;
import io.github.xinfra.lab.raft.proto.Eraftpb;
import io.github.xinfra.lab.xkv.kv.engine.PerRegionRaftEngine;
import io.github.xinfra.lab.xkv.kv.engine.RaftCfKeys;
import io.github.xinfra.lab.xkv.kv.engine.StorageEngine;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * Bridges the x-raft-lib {@link Storage} SPI onto our shared {@link
 * StorageEngine} via {@link PerRegionRaftEngine}.
 *
 * <p>This is the choke point that lets the v2 atomicity contract hold:
 * x-raft-lib calls into us through {@code Storage}, and we route the calls
 * to the same RocksDB instance / same {@code RAFT} CF that the apply loop
 * commits its business mutations to. The standalone {@code RocksDbStorage}
 * shipped with x-raft-lib could not satisfy Inv-1 because it owns its own
 * RocksDB.
 *
 * <h3>What lands here vs in {@code RegionPeerImpl}</h3>
 *
 * <ul>
 *   <li>{@link #append}, {@link #setHardState}, {@link #applySnapshot}:
 *       called by x-raft-lib at "ready" time. We open a fresh batch, write
 *       it with sync = true, return. Apply-loop business mutations come in
 *       a <em>different</em> path (RegionPeerImpl) and use their own batch.
 *       Both paths obey Inv-1 individually; the apply loop additionally
 *       co-bundles its business writes with appliedIndex into one batch.</li>
 *   <li>The fully-fused single-batch path lives on
 *       {@link #appendFused(List, Eraftpb.HardState, StorageEngine.WriteBatch)}
 *       used by RegionPeerImpl when it wants entries + hard state + applied
 *       + business in one fsync. That path bypasses the regular Storage
 *       SPI to avoid double-fsync.</li>
 * </ul>
 *
 * <p>{@code Storage.entries} is the only read hot path; we keep a pre-sized
 * iterator under one ReadOptions and let RocksDB prefix-bloom skip scans
 * outside this region's log range.
 */
public final class RegionRaftStorage implements Storage {
    private static final Logger log = LoggerFactory.getLogger(RegionRaftStorage.class);

    private final StorageEngine storage;
    private final PerRegionRaftEngine raft;
    private final long regionId;
    private final io.github.xinfra.lab.xkv.kv.engine.SnapshotEngine snapshotEngine;

    private volatile Eraftpb.HardState cachedHardState = Eraftpb.HardState.getDefaultInstance();
    private volatile Eraftpb.ConfState cachedConfState = Eraftpb.ConfState.getDefaultInstance();
    private volatile Eraftpb.Snapshot pendingSnapshot;     // for createSnapshot(...)

    public RegionRaftStorage(StorageEngine storage, PerRegionRaftEngine raft) {
        this(storage, raft, null);
    }

    /**
     * Production-grade construction wires a {@link
     * io.github.xinfra.lab.xkv.kv.engine.SnapshotEngine} so {@link
     * #createSnapshot} and {@link #applySnapshot} carry user-data CF
     * payload end-to-end. Without one, snapshots travel meta-only and
     * recovering followers won't have any business data.
     */
    public RegionRaftStorage(StorageEngine storage, PerRegionRaftEngine raft,
                              io.github.xinfra.lab.xkv.kv.engine.SnapshotEngine snapshotEngine) {
        this.storage = storage;
        this.raft = raft;
        this.snapshotEngine = snapshotEngine;
        // Reload hard state (term + vote + commit) from disk.
        this.cachedHardState = Eraftpb.HardState.newBuilder()
                .setTerm(raft.currentTerm())
                .setVote(raft.votedFor())
                .setCommit(raft.commitIndex())
                .build();
        // Reload conf state.
        byte[] cfgBytes = storage.get(StorageEngine.Cf.RAFT, RaftCfKeys.confStateKey(regionId()));
        if (cfgBytes != null) {
            try {
                this.cachedConfState = Eraftpb.ConfState.parseFrom(cfgBytes);
            } catch (com.google.protobuf.InvalidProtocolBufferException e) {
                log.warn("region={} confState parse failed, defaulting", regionId(), e);
            }
        }
        this.regionId = raft.regionId();
    }

    public long regionId() { return raft.regionId(); }

    public PerRegionRaftEngine engine() { return raft; }

    // ===== Reads =====

    @Override
    public InitialStateResult initialState() {
        return new InitialStateResult(cachedHardState, cachedConfState);
    }

    @Override
    public List<Eraftpb.Entry> entries(long lo, long hi, long maxSize) throws RaftException {
        if (lo > hi) return List.of();
        if (lo < raft.firstIndex()) {
            throw RaftException.ErrCompacted;
        }
        if (hi > raft.lastIndex() + 1) {
            throw new RaftException(RaftException.Code.UNAVAILABLE,
                    "entries hi=" + hi + " > lastIndex+1=" + (raft.lastIndex() + 1));
        }

        var lower = RaftCfKeys.logKey(regionId(), lo);
        var upper = RaftCfKeys.logKey(regionId(), hi);
        var ro = storage.newReadOptions().iterateLowerBound(lower).iterateUpperBound(upper);
        var out = new ArrayList<Eraftpb.Entry>((int) Math.min(hi - lo, 1024));
        long size = 0;
        try (var it = storage.newIterator(StorageEngine.Cf.RAFT, ro)) {
            for (it.seek(lower); it.isValid(); it.next()) {
                byte[] v = it.value();
                size += v.length;
                if (!out.isEmpty() && maxSize > 0 && size > maxSize) break;
                try {
                    out.add(Eraftpb.Entry.parseFrom(v));
                } catch (com.google.protobuf.InvalidProtocolBufferException e) {
                    throw new RaftException(RaftException.Code.UNAVAILABLE,
                            "log entry parse failed at index " +
                                    RaftCfKeys.logIndexFromKey(it.key()), e);
                }
            }
        }
        return out;
    }

    @Override
    public long term(long index) throws RaftException {
        if (index < raft.firstIndex() - 1) {
            throw RaftException.ErrCompacted;
        }
        if (index > raft.lastIndex()) {
            throw new RaftException(RaftException.Code.UNAVAILABLE,
                    "term: index " + index + " > lastIndex " + raft.lastIndex());
        }
        // Snapshot index is the only entry below firstIndex with a term we know.
        if (index == raft.firstIndex() - 1) {
            var meta = raft.lastSnapshotMeta();
            return meta == null ? 0 : meta.term();
        }
        byte[] v = raft.entryAt(index);
        if (v == null) {
            // entryAt returns null below firstIndex too; we already filtered above.
            throw new RaftException(RaftException.Code.UNAVAILABLE,
                    "term: missing entry " + index);
        }
        try {
            return Eraftpb.Entry.parseFrom(v).getTerm();
        } catch (com.google.protobuf.InvalidProtocolBufferException e) {
            throw new RaftException(RaftException.Code.UNAVAILABLE, "term: parse failed", e);
        }
    }

    @Override public long lastIndex() { return raft.lastIndex(); }

    @Override
    public long firstIndex() {
        long fi = raft.firstIndex();
        return fi == 0 ? 1 : fi;
    }

    @Override
    public Eraftpb.Snapshot snapshot() throws RaftException {
        if (pendingSnapshot != null) {
            var s = pendingSnapshot;
            pendingSnapshot = null;
            return s;
        }
        // Without a built snapshot ready: tell raft to retry later. The
        // store-side snapshot generator schedules creation asynchronously.
        throw RaftException.ErrSnapshotTemporarilyUnavailable;
    }

    // ===== Writes =====

    @Override
    public void setHardState(Eraftpb.HardState hs) {
        // Single-fsync write of hard state alone. This path is hit by raft
        // lib outside of an apply round; the apply round uses appendFused.
        try (var b = storage.newWriteBatch()) {
            raft.saveHardState(hs.getTerm(), hs.getVote(), hs.getCommit(), b);
            storage.write(b, true);
        }
        this.cachedHardState = hs;
    }

    @Override
    public void append(List<Eraftpb.Entry> entries) {
        if (entries.isEmpty()) return;

        long firstAppendIdx = entries.get(0).getIndex();
        try (var b = storage.newWriteBatch()) {
            // Overwrite-from-firstAppendIdx: x-raft-lib calls append both for
            // fresh-leader appends AND for follower truncate+append after a
            // term change. If our log has entries at or beyond firstAppendIdx,
            // truncate them first.
            if (firstAppendIdx <= raft.lastIndex()) {
                raft.truncateAfter(firstAppendIdx - 1, b);
            }
            // After truncate, lastIndex == firstAppendIdx - 1, so appendEntries
            // will write at firstAppendIdx onward.
            byte[][] serialized = new byte[entries.size()][];
            for (int i = 0; i < entries.size(); i++) {
                serialized[i] = entries.get(i).toByteArray();
            }
            raft.appendEntries(b, serialized);
            storage.write(b, true);
        }
    }

    @Override
    public void applySnapshot(Eraftpb.Snapshot snap) throws RaftException {
        if (snap == null || !snap.hasMetadata()) {
            throw new RaftException(RaftException.Code.UNAVAILABLE, "applySnapshot: no metadata");
        }
        long snapIndex = snap.getMetadata().getIndex();
        long snapTerm  = snap.getMetadata().getTerm();
        if (snapIndex < raft.firstIndex() - 1) {
            throw RaftException.ErrSnapOutOfDate;
        }

        // Step 1: install user-data CFs from the snapshot's `data` envelope.
        // We do this BEFORE writing raft meta so that on a mid-install crash,
        // the next startup still has the OLD raft meta and will be re-served
        // a snapshot by the leader (idempotent restart).
        byte[] dataBytes = snap.getData().toByteArray();
        if (snapshotEngine != null && dataBytes.length > 0) {
            try {
                var chunks = decodeChunkEnvelope(dataBytes);
                snapshotEngine.receiveAndInstall(regionId(), chunks);
            } catch (Throwable t) {
                log.warn("region={} applySnapshot user-data install failed: {}",
                        regionId(), t.getMessage());
                throw new RaftException(RaftException.Code.UNAVAILABLE,
                        "user-data install: " + t.getMessage(), t);
            }
        }

        // Step 2: install raft meta. After this point the local view says
        // "I'm at applied=snapIndex" — must come AFTER the user data is in
        // place or readers between these two writes would see stale state.
        try (var b = storage.newWriteBatch()) {
            // Wipe log entries; the snapshot replaces them.
            b.deleteRange(StorageEngine.Cf.RAFT,
                    RaftCfKeys.logKey(regionId(), 0),
                    RaftCfKeys.logKey(regionId(), Long.MAX_VALUE));
            raft.saveSnapshotMeta(new io.github.xinfra.lab.xkv.kv.engine.RaftEngine.SnapshotMeta(
                    snapTerm, snapIndex, null, null), b);
            raft.saveAppliedIndex(snapIndex, b);
            // Also update first/lastIndex so the in-memory engine cache matches:
            // after a snapshot install, firstIndex = snapIndex + 1 and lastIndex
            // >= snapIndex (no log entries exist below). The engine recalculates
            // its scan range on next access; for in-memory consistency we bump
            // through saveSnapshotMeta which leaves it to the engine.
            // ConfState: persisted as a separate entry alongside snapshot meta.
            var cs = snap.getMetadata().getConfState();
            b.put(StorageEngine.Cf.RAFT, RaftCfKeys.confStateKey(regionId()), cs.toByteArray());
            this.cachedConfState = cs;
            storage.write(b, true);
        }
        log.info("region={} applySnapshot installed at index={} term={}",
                regionId(), snapIndex, snapTerm);
    }

    private static java.util.List<io.github.xinfra.lab.xkv.proto.KvServerpb.SnapshotChunk>
            decodeChunkEnvelope(byte[] data) throws com.google.protobuf.InvalidProtocolBufferException {
        var bb = java.nio.ByteBuffer.wrap(data);
        int count = bb.getInt();
        var out = new java.util.ArrayList<io.github.xinfra.lab.xkv.proto.KvServerpb.SnapshotChunk>(count);
        for (int i = 0; i < count; i++) {
            int len = bb.getInt();
            byte[] chunk = new byte[len];
            bb.get(chunk);
            out.add(io.github.xinfra.lab.xkv.proto.KvServerpb.SnapshotChunk.parseFrom(chunk));
        }
        return out;
    }

    @Override
    public Eraftpb.Snapshot createSnapshot(long appliedIndex,
                                           Eraftpb.ConfState cs,
                                           byte[] data) throws RaftException {
        long ai = raft.appliedIndex();
        if (appliedIndex > ai) {
            throw new RaftException(RaftException.Code.UNAVAILABLE,
                    "createSnapshot: appliedIndex " + appliedIndex + " > storage.applied " + ai);
        }
        long term;
        try { term = term(appliedIndex); } catch (RaftException e) { throw e; }

        // If a SnapshotEngine is wired, dump the user-data CFs into the
        // snapshot's data field so followers can install via applySnapshot
        // and have real state to read. Without this, recovering peers see
        // only raft meta and lose all business data.
        byte[] snapshotData = data == null ? new byte[0] : data;
        if (snapshotEngine != null) {
            var chunks = new java.util.ArrayList<io.github.xinfra.lab.xkv.proto.KvServerpb.SnapshotChunk>();
            snapshotEngine.buildAndStream(regionId(), term, appliedIndex,
                    /* startKey= */ new byte[]{0}, /* endKey= */ null, chunks::add);
            // Serialize the chunk list with a tiny [4B count][N x [4B len][bytes]] envelope.
            int total = 4;
            var perChunkBytes = new byte[chunks.size()][];
            for (int i = 0; i < chunks.size(); i++) {
                perChunkBytes[i] = chunks.get(i).toByteArray();
                total += 4 + perChunkBytes[i].length;
            }
            var bb = java.nio.ByteBuffer.allocate(total);
            bb.putInt(chunks.size());
            for (var c : perChunkBytes) { bb.putInt(c.length); bb.put(c); }
            snapshotData = bb.array();
        }

        var meta = Eraftpb.SnapshotMetadata.newBuilder()
                .setIndex(appliedIndex)
                .setTerm(term)
                .setConfState(cs)
                .build();
        var snap = Eraftpb.Snapshot.newBuilder()
                .setMetadata(meta)
                .setData(com.google.protobuf.ByteString.copyFrom(snapshotData))
                .build();
        // Persist snapshot meta + new conf state.
        try (var b = storage.newWriteBatch()) {
            raft.saveSnapshotMeta(new io.github.xinfra.lab.xkv.kv.engine.RaftEngine.SnapshotMeta(
                    term, appliedIndex, null, null), b);
            b.put(StorageEngine.Cf.RAFT, RaftCfKeys.confStateKey(regionId()), cs.toByteArray());
            this.cachedConfState = cs;
            storage.write(b, true);
        }
        this.pendingSnapshot = snap;
        return snap;
    }

    @Override
    public void compact(long compactIndex) throws RaftException {
        if (compactIndex >= firstIndex()) {
            try (var b = storage.newWriteBatch()) {
                raft.compactLog(compactIndex, b);
                storage.write(b, true);
            }
        }
    }

    @Override public boolean supportsStreamingSnapshot() { return false; }

    @Override public void close() { /* engine ownership is the Store's */ }

    // ===== Fused single-batch path used by RegionPeerImpl =====

    /**
     * The Inv-1 entrypoint. Stages every persistence the apply round needs
     * — entries, hard state, applied index, snapshot meta, and arbitrary
     * business mutations the caller has already added — into one batch
     * supplied by the caller, then the caller does ONE
     * {@code engine.write(batch, sync=true)}. The cached hard state is
     * updated in-memory after the caller's write succeeds; the caller
     * notifies us via {@link #postApply}.
     */
    public void appendFused(List<Eraftpb.Entry> entries,
                            Eraftpb.HardState hs,
                            StorageEngine.WriteBatch batch) {
        if (entries != null && !entries.isEmpty()) {
            long firstAppendIdx = entries.get(0).getIndex();
            if (firstAppendIdx <= raft.lastIndex()) {
                raft.truncateAfter(firstAppendIdx - 1, batch);
            }
            byte[][] serialized = new byte[entries.size()][];
            for (int i = 0; i < entries.size(); i++) {
                serialized[i] = entries.get(i).toByteArray();
            }
            raft.appendEntries(batch, serialized);
        }
        if (hs != null && !hs.equals(Eraftpb.HardState.getDefaultInstance())) {
            raft.saveHardState(hs.getTerm(), hs.getVote(), hs.getCommit(), batch);
        }
    }

    /**
     * Called by RegionPeerImpl after the fused batch was successfully
     * written. Synchronizes the in-memory hard-state cache with what was
     * just persisted.
     */
    public void postApply(Eraftpb.HardState hs) {
        if (hs != null && !hs.equals(Eraftpb.HardState.getDefaultInstance())) {
            this.cachedHardState = hs;
        }
    }

    /** Update the cached ConfState after a ConfChange entry is applied. */
    public void postApplyConfState(Eraftpb.ConfState cs, StorageEngine.WriteBatch batch) {
        batch.put(StorageEngine.Cf.RAFT, RaftCfKeys.confStateKey(regionId()), cs.toByteArray());
        this.cachedConfState = cs;
    }
}
