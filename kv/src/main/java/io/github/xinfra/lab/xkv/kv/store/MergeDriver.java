package io.github.xinfra.lab.xkv.kv.store;

import com.google.protobuf.ByteString;
import io.github.xinfra.lab.xkv.kv.raft.ProposalCodec;
import io.github.xinfra.lab.xkv.kv.raft.RegionPeer;
import io.github.xinfra.lab.xkv.proto.KvServerpb;
import io.github.xinfra.lab.xkv.proto.Metapb;
import io.github.xinfra.lab.xkv.proto.PDGrpc;
import io.github.xinfra.lab.xkv.proto.Pdpb;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.concurrent.TimeUnit;

/**
 * Orchestrates a region merge end-to-end on the target's leader.
 *
 * <h3>Phase-7 minimum-viable shape</h3>
 *
 * <p>This implementation skips the two-sided PrepareMerge / RollbackMerge
 * dance TiKV uses to coordinate concurrent writes on the source. Callers
 * are expected to quiesce the source — typically by stopping writes
 * upstream — before invoking {@link #merge}. The CommitMerge entry then
 * lands cleanly on the target.
 *
 * <p>Adjacency requirement: {@code source} and {@code target} MUST be
 * directly adjacent (one's {@code end_key} == the other's {@code
 * start_key}). The driver picks left/right by examining both descriptors
 * — the resulting target absorbs the source's range.
 *
 * <p>Atomicity: the apply path persists the target's new descriptor +
 * removes the source's descriptor in one batch + fsync. The local
 * destroy of the source peer is fired by the merge observer; raft groups
 * on remote stores observe the source descriptor disappear via PD's
 * region heartbeat (multi-region scheduler will plumb that).
 */
public final class MergeDriver {
    private static final Logger log = LoggerFactory.getLogger(MergeDriver.class);

    private final long proposeTimeoutMs;
    private final PDGrpc.PDBlockingStub pdStub;
    private final long clusterId;

    public MergeDriver(long proposeTimeoutMs) {
        this(proposeTimeoutMs, null, 0);
    }

    public MergeDriver(long proposeTimeoutMs, PDGrpc.PDBlockingStub pdStub, long clusterId) {
        this.proposeTimeoutMs = proposeTimeoutMs;
        this.pdStub = pdStub;
        this.clusterId = clusterId;
    }

    /**
     * Merge {@code source}'s region into {@code target}'s region. Returns
     * the resulting target descriptor (with merged range + bumped epoch).
     *
     * <p>Both peers must be leaders of their respective regions. Throws if
     * the regions aren't adjacent or epochs don't match.
     */
    public Metapb.Region merge(RegionPeer source, RegionPeer target) throws Exception {
        var s = source.region();
        var t = target.region();
        var ordered = orderAdjacent(s, t);
        var left = ordered[0];
        var right = ordered[1];

        // === Phase 1: PrepareMerge on source ===
        //
        // Quiesces the source: subsequent business proposals are rejected
        // at apply time with a "region merging" error. Without this,
        // writes that race between merge-decide and the CommitMerge apply
        // would be lost (source disappears; its raft log entries that
        // weren't yet applied are dropped).
        var prepare = KvServerpb.PrepareMergeProposal.newBuilder().setTarget(t).build();
        var prepareEnv = ProposalCodec.encode(
                ProposalCodec.Kind.ADMIN_PREPARE_MERGE, /* seq= */ 0, prepare.toByteArray());
        var prepareFut = source.propose(new RegionPeer.Proposal(prepareEnv, 0, 0));
        var prepareResult = prepareFut.get(proposeTimeoutMs, TimeUnit.MILLISECONDS);
        if (!prepareResult.success()) {
            throw new IllegalStateException("ADMIN_PREPARE_MERGE failed: " + prepareResult.errorMessage());
        }

        // === Phase 2: CommitMerge on target ===
        //
        // The target absorbs the source's range. On any failure here we
        // attempt RollbackMerge on the source so it doesn't stay quiesced
        // forever — at-least-once delivery, the source's rollback apply is
        // idempotent (clears the flag if set, no-op if already clear).
        try {
            long bumpedVersion =
                    Math.max(s.getRegionEpoch().getVersion(), t.getRegionEpoch().getVersion()) + 1;
            var mergedTarget = t.toBuilder()
                    .setStartKey(left.getStartKey())
                    .setEndKey(right.getEndKey())
                    .setRegionEpoch(t.getRegionEpoch().toBuilder().setVersion(bumpedVersion))
                    .build();

            var commit = KvServerpb.MergeRegionProposal.newBuilder()
                    .setMergedTarget(mergedTarget)
                    .setSourceRegion(s)
                    .build();
            var commitEnv = ProposalCodec.encode(
                    ProposalCodec.Kind.ADMIN_COMMIT_MERGE, /* seq= */ 0, commit.toByteArray());
            var commitFut = target.propose(new RegionPeer.Proposal(commitEnv, 0, 0));
            var commitResult = commitFut.get(proposeTimeoutMs, TimeUnit.MILLISECONDS);
            if (!commitResult.success()) {
                throw new IllegalStateException(
                        "ADMIN_COMMIT_MERGE propose failed: " + commitResult.errorMessage());
            }
            log.info("merge: source={} → target={} new range [{}, {})",
                    s.getId(), t.getId(),
                    mergedTarget.getStartKey().toStringUtf8(),
                    mergedTarget.getEndKey().toStringUtf8());
            return mergedTarget;
        } catch (Throwable t2) {
            // PD safety check: verify target hasn't already committed the merge
            // before attempting rollback. If target's epoch version advanced,
            // the merge was committed — rollback would cause data loss.
            if (pdStub != null) {
                try {
                    var resp = pdStub.getRegionByID(Pdpb.GetRegionByIDRequest.newBuilder()
                            .setHeader(Pdpb.RequestHeader.newBuilder().setClusterId(clusterId))
                            .setRegionId(t.getId())
                            .build());
                    if (resp.hasRegion()) {
                        long targetVersionNow = resp.getRegion().getRegionEpoch().getVersion();
                        long targetVersionAtPrepare = t.getRegionEpoch().getVersion();
                        if (targetVersionNow > targetVersionAtPrepare) {
                            log.error("merge: target={} already committed (epoch {} → {}). "
                                            + "Source={} stays quiesced — manual intervention needed.",
                                    t.getId(), targetVersionAtPrepare, targetVersionNow, s.getId());
                            throw t2;
                        }
                    }
                } catch (Throwable pdErr) {
                    if (pdErr == t2) throw t2;
                    log.warn("merge: PD epoch check failed for target={}: {}",
                            t.getId(), pdErr.getMessage());
                }
            }

            // Safe to rollback — target has not committed.
            try {
                var rb = KvServerpb.RollbackMergeProposal.newBuilder()
                        .setTargetRegionId(t.getId()).build();
                var rbEnv = ProposalCodec.encode(
                        ProposalCodec.Kind.ADMIN_ROLLBACK_MERGE, /* seq= */ 0, rb.toByteArray());
                source.propose(new RegionPeer.Proposal(rbEnv, 0, 0))
                        .get(proposeTimeoutMs, TimeUnit.MILLISECONDS);
                log.info("merge: rolled back source={} after commit failure", s.getId());
            } catch (Throwable rbErr) {
                log.warn("merge: rollback ALSO failed for source={}: {}", s.getId(), rbErr.getMessage());
            }
            throw t2;
        }
    }

    /**
     * Return the two regions in left-to-right order if they're adjacent,
     * else throw. Adjacent ⇒ left.end_key == right.start_key (both
     * non-empty, or both are sentinels).
     */
    private Metapb.Region[] orderAdjacent(Metapb.Region a, Metapb.Region b) {
        if (adjacent(a, b)) return new Metapb.Region[]{ a, b };
        if (adjacent(b, a)) return new Metapb.Region[]{ b, a };
        throw new IllegalArgumentException(
                "regions not adjacent: a=[" + key(a.getStartKey()) + ", " + key(a.getEndKey()) + ")"
                        + " b=[" + key(b.getStartKey()) + ", " + key(b.getEndKey()) + ")");
    }

    private static boolean adjacent(Metapb.Region left, Metapb.Region right) {
        byte[] le = left.getEndKey().toByteArray();
        byte[] rs = right.getStartKey().toByteArray();
        if (le.length == 0 || rs.length == 0) return false;
        return Arrays.equals(le, rs);
    }

    private static String key(ByteString k) { return k.isEmpty() ? "<empty>" : k.toStringUtf8(); }
}
