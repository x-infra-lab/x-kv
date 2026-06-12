package io.github.xinfra.lab.xkv.kv.raft;

import io.github.xinfra.lab.xkv.kv.engine.PerRegionRaftEngine;
import io.github.xinfra.lab.xkv.kv.engine.StorageEngine;
import io.github.xinfra.lab.xkv.proto.KvServerpb;
import io.github.xinfra.lab.xkv.proto.Metapb;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.function.Consumer;

/**
 * Apply handler for region-admin proposals (compact log, split, merge,
 * change peer).
 *
 * <p>v1 had no log compaction at all; the Raft log grew unboundedly,
 * eating disk and slowing every restart (full replay). Production
 * deployments must keep first_index advancing.
 *
 * <h3>ADMIN_COMPACT_LOG</h3>
 *
 * <p>Payload format: {@code [8B targetIndex]}. The chosen index is
 * applied across every replica (proposed through Raft, so quorum-agreed)
 * which means a single coordinator's threshold decision applies cluster-
 * wide. The apply path delegates to {@link PerRegionRaftEngine#compactLog}
 * which drops every log entry with index ≤ targetIndex from the RAFT CF.
 *
 * <p>{@link io.github.xinfra.lab.xkv.kv.store.LogCompactionWorker} drives
 * the proposals: it observes {@code applied_index - first_index} per
 * region and emits a compact when the gap exceeds a configurable
 * threshold, holding back a safety margin so a slow follower can still
 * catch up via log replay (otherwise it would need a snapshot).
 *
 * <h3>Future kinds</h3>
 *
 * <p>ADMIN_SPLIT / ADMIN_PREPARE_MERGE / ADMIN_COMMIT_MERGE / ADMIN_ROLLBACK_MERGE
 * land here when the multi-region work happens. The handler is structured
 * so adding a new admin operation is one switch arm + one method.
 */
public final class AdminApplyHandler implements ApplyHandler {
    private static final Logger log = LoggerFactory.getLogger(AdminApplyHandler.class);

    private final PerRegionRaftEngine raftEngine;
    private final StorageEngine storage;
    private final SplitObserver splitObserver;
    private final MergeObserver mergeObserver;

    public AdminApplyHandler(PerRegionRaftEngine raftEngine) {
        this(raftEngine, null, null, null);
    }

    /**
     * Production wiring: storage is needed so split can write child regions
     * to RAFT CF entries that don't belong to this engine's region_id; the
     * observer is invoked AFTER persistence so the {@code Store} can
     * refresh the parent's in-memory descriptor and spawn a new
     * {@link RegionPeer} for each child.
     */
    public AdminApplyHandler(PerRegionRaftEngine raftEngine,
                              StorageEngine storage,
                              SplitObserver splitObserver) {
        this(raftEngine, storage, splitObserver, null);
    }

    public AdminApplyHandler(PerRegionRaftEngine raftEngine,
                              StorageEngine storage,
                              SplitObserver splitObserver,
                              MergeObserver mergeObserver) {
        this.raftEngine = raftEngine;
        this.storage = storage;
        this.splitObserver = splitObserver;
        this.mergeObserver = mergeObserver;
    }

    /**
     * Receives notification after a split has been atomically persisted.
     * Implementation is expected to refresh the parent peer's region
     * descriptor and spawn a new region peer for each child.
     */
    @FunctionalInterface
    public interface SplitObserver {
        void onSplit(Metapb.Region updatedParent, java.util.List<Metapb.Region> children);
    }

    /** Convenience: callers that only care about children can pass a child-only consumer. */
    public static SplitObserver childrenOnly(Consumer<Metapb.Region> spawn) {
        return (parent, children) -> children.forEach(spawn);
    }

    /**
     * Receives notification after a commit-merge has been atomically
     * persisted on the target. Implementation is expected to refresh the
     * target peer's in-memory descriptor (now covering the merged range)
     * and destroy the source peer locally.
     */
    @FunctionalInterface
    public interface MergeObserver {
        void onMerge(Metapb.Region mergedTarget, Metapb.Region sourceRegion);
    }

    @Override
    public Result apply(ProposalCodec.Decoded decoded, StorageEngine.WriteBatch batch) {
        return switch (decoded.kind()) {
            case ADMIN_COMPACT_LOG -> applyCompactLog(decoded.payload(), batch);
            case ADMIN_SPLIT -> applySplit(decoded.payload(), batch);
            case ADMIN_COMMIT_MERGE -> applyCommitMerge(decoded.payload(), batch);
            case ADMIN_PREPARE_MERGE -> applyPrepareMerge(decoded.payload(), batch);
            case ADMIN_ROLLBACK_MERGE -> applyRollbackMerge(decoded.payload(), batch);
            default -> Result.err("unsupported admin kind: " + decoded.kind());
        };
    }

    private Result applyCompactLog(byte[] payload, StorageEngine.WriteBatch batch) {
        if (payload.length < 8) return Result.err("ADMIN_COMPACT_LOG: payload too short");
        long targetIndex = ByteBuffer.wrap(payload).getLong();
        long first = raftEngine.firstIndex();
        long applied = raftEngine.appliedIndex();
        if (targetIndex < first) {
            // Already compacted past this index — idempotent no-op.
            return Result.ok();
        }
        if (targetIndex >= applied) {
            // NEVER compact past applied — would lose entries we haven't
            // turned into business state.
            log.warn("region={} compact rejected: target={} >= applied={}",
                    raftEngine.regionId(), targetIndex, applied);
            return Result.err("compact target >= applied_index");
        }
        raftEngine.compactLog(targetIndex, batch);
        log.info("region={} compacted log: first_index {} → {} (applied={})",
                raftEngine.regionId(), first, targetIndex + 1, applied);
        return Result.ok();
    }

    /**
     * Region split: shrink the parent's range, persist each child region
     * descriptor. Children's data CFs already contain their keys (split is
     * pure metadata — no key movement). Children's raft groups start fresh
     * (no log, applied_index=0); the {@link #splitChildHook} fires for each
     * so the host {@code Store} can spawn a {@link RegionPeer} for it.
     *
     * <p>Atomicity: all region descriptors land in one {@code WriteBatch}
     * so a crash mid-split either persists the WHOLE split or NONE of it.
     * On replay, the apply is idempotent because epochs / region IDs are
     * deterministic from the proposal payload.
     */
    private Result applySplit(byte[] payload, StorageEngine.WriteBatch batch) {
        KvServerpb.SplitRegionProposal req;
        try { req = KvServerpb.SplitRegionProposal.parseFrom(payload); }
        catch (Throwable t) { return Result.err("ADMIN_SPLIT decode: " + t.getMessage()); }
        if (!req.hasUpdatedParent()) return Result.err("ADMIN_SPLIT: missing parent");
        if (req.getUpdatedParent().getId() != raftEngine.regionId()) {
            return Result.err("ADMIN_SPLIT: parent_id " + req.getUpdatedParent().getId()
                    + " != local region_id " + raftEngine.regionId());
        }

        // Persist parent with shrunken range + bumped epoch.
        raftEngine.saveRegion(req.getUpdatedParent(), batch);

        // Persist each child region descriptor. Children get their own
        // engine state lazily — when the {@code Store} spawns a RegionPeer
        // for them via the hook, that constructor will reload from this
        // persisted descriptor.
        for (var child : req.getChildrenList()) {
            byte[] key = io.github.xinfra.lab.xkv.kv.engine.RaftCfKeys.regionKey(child.getId());
            batch.put(StorageEngine.Cf.RAFT, key, child.toByteArray());
        }

        log.info("region={} split into parent[start={}, end={}] + {} children",
                raftEngine.regionId(),
                hex(req.getUpdatedParent().getStartKey().toByteArray()),
                hex(req.getUpdatedParent().getEndKey().toByteArray()),
                req.getChildrenCount());

        if (splitObserver != null) {
            var parent = req.getUpdatedParent();
            var children = req.getChildrenList();
            return Result.okWithPostFlush(() -> {
                try {
                    splitObserver.onSplit(parent, children);
                } catch (Throwable t) {
                    log.warn("split observer failed parent={}", parent.getId(), t);
                }
            });
        }
        return Result.ok();
    }

    /**
     * Prepare-merge: source quiesces. The persisted "merging" marker on
     * RAFT CF tells the apply loop to reject subsequent business writes.
     * The marker also pins the target descriptor at decide time so the
     * subsequent Commit / Rollback can validate consistency.
     */
    private Result applyPrepareMerge(byte[] payload, StorageEngine.WriteBatch batch) {
        if (raftEngine.isMerging()) {
            // Idempotent replay — already merging, no-op.
            return Result.ok();
        }
        // Validate payload parses (so a malformed proposal doesn't lock the
        // region out forever) but we don't otherwise inspect target — the
        // commit path on TARGET is what validates consistency.
        try { KvServerpb.PrepareMergeProposal.parseFrom(payload); }
        catch (Throwable t) { return Result.err("ADMIN_PREPARE_MERGE decode: " + t.getMessage()); }
        raftEngine.saveMergeState(payload, batch);
        log.info("region={} prepared merge (quiescing writes)", raftEngine.regionId());
        return Result.ok();
    }

    /**
     * Rollback-merge: source resumes accepting writes. Called when the
     * target-side CommitMerge fails (e.g., target leader dies).
     */
    private Result applyRollbackMerge(byte[] payload, StorageEngine.WriteBatch batch) {
        if (!raftEngine.isMerging()) {
            // Idempotent replay — already rolled back.
            return Result.ok();
        }
        raftEngine.clearMergeState(batch);
        log.info("region={} rolled back merge (writes resumed)", raftEngine.regionId());
        return Result.ok();
    }

    /**
     * Commit-merge: target absorbs source's range. Atomicity:
     * <ol>
     *   <li>Save target's new descriptor (merged range, bumped epoch).</li>
     *   <li>Remove source's descriptor from the engine.</li>
     * </ol>
     * Both land in one batch — a crash mid-apply either rolls back the
     * whole merge or persists the whole thing.
     *
     * <p>The data CFs already contain BOTH regions' keys (they live in
     * the same engine, both regions' key ranges are disjoint slices of
     * the same KV space). No data movement — the merge is metadata only,
     * mirroring the split's no-data-movement property.
     *
     * <p>The observer then fires so the host {@code Store} can:
     *   - refresh the target peer's in-memory descriptor
     *   - destroy the source peer (its raft group is shut down; its log
     *     entries are no longer reachable since source's region descriptor
     *     is gone)
     */
    private Result applyCommitMerge(byte[] payload, StorageEngine.WriteBatch batch) {
        KvServerpb.MergeRegionProposal req;
        try { req = KvServerpb.MergeRegionProposal.parseFrom(payload); }
        catch (Throwable t) { return Result.err("ADMIN_COMMIT_MERGE decode: " + t.getMessage()); }
        if (!req.hasMergedTarget() || !req.hasSourceRegion()) {
            return Result.err("ADMIN_COMMIT_MERGE: missing merged_target or source_region");
        }
        if (req.getMergedTarget().getId() != raftEngine.regionId()) {
            return Result.err("ADMIN_COMMIT_MERGE: target_id "
                    + req.getMergedTarget().getId() + " != local region_id " + raftEngine.regionId());
        }

        // Validate source region epoch against persisted state to prevent
        // absorbing a range that has been split or re-merged since the
        // proposal was created.
        if (storage != null) {
            byte[] storedSourceBytes = storage.get(StorageEngine.Cf.RAFT,
                    io.github.xinfra.lab.xkv.kv.engine.RaftCfKeys.regionKey(
                            req.getSourceRegion().getId()));
            if (storedSourceBytes != null) {
                try {
                    var stored = Metapb.Region.parseFrom(storedSourceBytes);
                    var se = stored.getRegionEpoch();
                    var re = req.getSourceRegion().getRegionEpoch();
                    if (se.getConfVer() != re.getConfVer()
                            || se.getVersion() != re.getVersion()) {
                        return Result.err("ADMIN_COMMIT_MERGE: source epoch mismatch: stored=("
                                + se.getConfVer() + "," + se.getVersion()
                                + ") req=(" + re.getConfVer() + "," + re.getVersion() + ")");
                    }
                } catch (com.google.protobuf.InvalidProtocolBufferException e) {
                    log.warn("ADMIN_COMMIT_MERGE: cannot parse stored source region", e);
                }
            }
        }

        // 1. Save target's new descriptor.
        raftEngine.saveRegion(req.getMergedTarget(), batch);
        // 2. Drop source's region descriptor.
        byte[] sourceKey = io.github.xinfra.lab.xkv.kv.engine.RaftCfKeys
                .regionKey(req.getSourceRegion().getId());
        batch.delete(StorageEngine.Cf.RAFT, sourceKey);

        log.info("region={} absorbed source region={} → range [{}, {})",
                raftEngine.regionId(),
                req.getSourceRegion().getId(),
                hex(req.getMergedTarget().getStartKey().toByteArray()),
                hex(req.getMergedTarget().getEndKey().toByteArray()));

        if (mergeObserver != null) {
            var mergedTarget = req.getMergedTarget();
            var sourceRegion = req.getSourceRegion();
            return Result.okWithPostFlush(() -> {
                try { mergeObserver.onMerge(mergedTarget, sourceRegion); }
                catch (Throwable t) {
                    log.warn("merge observer failed target={} source={}",
                            mergedTarget.getId(), sourceRegion.getId(), t);
                }
            });
        }
        return Result.ok();
    }

    private static String hex(byte[] k) {
        if (k == null || k.length == 0) return "<empty>";
        var sb = new StringBuilder(k.length * 2);
        for (byte b : k) sb.append(String.format("%02x", b));
        return sb.toString();
    }

    /** Encode a compact-log payload. */
    public static byte[] encodeCompactLog(long targetIndex) {
        return ByteBuffer.allocate(8).putLong(targetIndex).array();
    }
}
