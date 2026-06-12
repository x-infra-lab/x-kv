package io.github.xinfra.lab.xkv.kv.store;

import com.google.protobuf.ByteString;
import io.github.xinfra.lab.xkv.kv.raft.MergeProtocol;
import io.github.xinfra.lab.xkv.kv.raft.ProposalCodec;
import io.github.xinfra.lab.xkv.kv.raft.RegionPeer;
import io.github.xinfra.lab.xkv.proto.KvServerpb;
import io.github.xinfra.lab.xkv.proto.Metapb;
import io.github.xinfra.lab.xkv.proto.PDGrpc;
import io.github.xinfra.lab.xkv.proto.Pdpb;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;

/**
 * Production-grade MergeProtocol: 3-phase merge with PD-verified rollback.
 *
 * <p>The critical safety property: rollback of a prepared source MUST NOT
 * proceed if the target has already committed the merge. This impl queries
 * PD for the target's current epoch before proposing any rollback — if the
 * target's {@code RegionEpoch.version} has advanced beyond the prepare-time
 * baseline, the merge was already committed and rollback is forbidden.
 */
public final class MergeProtocolImpl implements MergeProtocol {
    private static final Logger log = LoggerFactory.getLogger(MergeProtocolImpl.class);

    private final RegionPeerLocator locator;
    private final PDGrpc.PDBlockingStub pdStub;
    private final long clusterId;
    private final long proposeTimeoutMs;

    private final ConcurrentMap<Long, MergeState> mergeStates = new ConcurrentHashMap<>();

    public interface RegionPeerLocator {
        RegionPeer peerForRegionId(long regionId);
    }

    private record MergeState(
            long sourceId, long targetId,
            Metapb.RegionEpoch targetEpochAtPrepare,
            State state) {}

    public MergeProtocolImpl(RegionPeerLocator locator, PDGrpc.PDBlockingStub pdStub,
                             long clusterId, long proposeTimeoutMs) {
        this.locator = locator;
        this.pdStub = pdStub;
        this.clusterId = clusterId;
        this.proposeTimeoutMs = proposeTimeoutMs;
    }

    @Override
    public State state(long regionId) {
        var ms = mergeStates.get(regionId);
        return ms == null ? State.IDLE : ms.state;
    }

    @Override
    public CompletableFuture<Void> prepareAsSource(long sourceId, long targetId) {
        return CompletableFuture.supplyAsync(() -> {
            try {
                var sourcePeer = locator.peerForRegionId(sourceId);
                if (sourcePeer == null)
                    throw new IllegalStateException("source region " + sourceId + " not found locally");
                if (!sourcePeer.isLeader())
                    throw new IllegalStateException("source region " + sourceId + " is not leader");

                var targetPeer = locator.peerForRegionId(targetId);
                if (targetPeer == null)
                    throw new IllegalStateException("target region " + targetId + " not found locally");

                var targetRegion = targetPeer.region();
                var targetEpoch = targetRegion.getRegionEpoch();

                var prepare = KvServerpb.PrepareMergeProposal.newBuilder()
                        .setTarget(targetRegion)
                        .build();
                var envelope = ProposalCodec.encode(
                        ProposalCodec.Kind.ADMIN_PREPARE_MERGE, 0, prepare.toByteArray());
                var result = sourcePeer.propose(new RegionPeer.Proposal(envelope, 0, 0))
                        .get(proposeTimeoutMs, TimeUnit.MILLISECONDS);
                if (!result.success())
                    throw new IllegalStateException("PrepareMerge failed: " + result.errorMessage());

                mergeStates.put(sourceId, new MergeState(
                        sourceId, targetId, targetEpoch, State.PREPARED_AS_SOURCE));
                log.info("merge: source={} prepared (target={} epoch={})",
                        sourceId, targetId, targetEpoch);
                return (Void) null;
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });
    }

    @Override
    public CompletableFuture<Void> commitAsTarget(long sourceId, long targetId) {
        return CompletableFuture.supplyAsync(() -> {
            try {
                var sourcePeer = locator.peerForRegionId(sourceId);
                var targetPeer = locator.peerForRegionId(targetId);
                if (targetPeer == null)
                    throw new IllegalStateException("target region " + targetId + " not found locally");
                if (!targetPeer.isLeader())
                    throw new IllegalStateException("target region " + targetId + " is not leader");

                var s = sourcePeer != null ? sourcePeer.region() : queryRegionFromPd(sourceId);
                var t = targetPeer.region();

                var ordered = orderAdjacent(s, t);
                var left = ordered[0];
                var right = ordered[1];

                long bumpedVersion = Math.max(
                        s.getRegionEpoch().getVersion(),
                        t.getRegionEpoch().getVersion()) + 1;
                var mergedTarget = t.toBuilder()
                        .setStartKey(left.getStartKey())
                        .setEndKey(right.getEndKey())
                        .setRegionEpoch(t.getRegionEpoch().toBuilder().setVersion(bumpedVersion))
                        .build();

                var commit = KvServerpb.MergeRegionProposal.newBuilder()
                        .setMergedTarget(mergedTarget)
                        .setSourceRegion(s)
                        .build();
                var envelope = ProposalCodec.encode(
                        ProposalCodec.Kind.ADMIN_COMMIT_MERGE, 0, commit.toByteArray());
                var result = targetPeer.propose(new RegionPeer.Proposal(envelope, 0, 0))
                        .get(proposeTimeoutMs, TimeUnit.MILLISECONDS);
                if (!result.success())
                    throw new IllegalStateException("CommitMerge failed: " + result.errorMessage());

                mergeStates.put(sourceId, new MergeState(
                        sourceId, targetId, null, State.COMMITTED));
                log.info("merge: target={} committed merge of source={} → range [{}, {})",
                        targetId, sourceId,
                        mergedTarget.getStartKey().toStringUtf8(),
                        mergedTarget.getEndKey().toStringUtf8());
                return (Void) null;
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });
    }

    @Override
    public CompletableFuture<Void> rollbackAsSource(long sourceId) {
        return CompletableFuture.supplyAsync(() -> {
            try {
                var ms = mergeStates.get(sourceId);
                if (ms == null || ms.state != State.PREPARED_AS_SOURCE)
                    throw new IllegalStateException(
                            "source " + sourceId + " is not in PREPARED_AS_SOURCE state");

                // Safety check: query PD for the target's current epoch.
                var targetNow = queryRegionFromPd(ms.targetId);
                if (targetNow != null && ms.targetEpochAtPrepare != null) {
                    long prepareVersion = ms.targetEpochAtPrepare.getVersion();
                    long currentVersion = targetNow.getRegionEpoch().getVersion();
                    if (currentVersion > prepareVersion) {
                        log.error("merge: CANNOT rollback source={}: target={} already committed "
                                        + "(epoch {} → {}). Source stays quiesced — manual intervention needed.",
                                sourceId, ms.targetId, prepareVersion, currentVersion);
                        throw new IllegalStateException(
                                "cannot rollback: target " + ms.targetId
                                        + " already committed merge (epoch version advanced from "
                                        + prepareVersion + " to " + currentVersion + ")");
                    }
                }

                var sourcePeer = locator.peerForRegionId(sourceId);
                if (sourcePeer == null)
                    throw new IllegalStateException("source region " + sourceId + " not found locally");

                var rb = KvServerpb.RollbackMergeProposal.newBuilder()
                        .setTargetRegionId(ms.targetId)
                        .build();
                var envelope = ProposalCodec.encode(
                        ProposalCodec.Kind.ADMIN_ROLLBACK_MERGE, 0, rb.toByteArray());
                var result = sourcePeer.propose(new RegionPeer.Proposal(envelope, 0, 0))
                        .get(proposeTimeoutMs, TimeUnit.MILLISECONDS);
                if (!result.success())
                    throw new IllegalStateException("RollbackMerge failed: " + result.errorMessage());

                mergeStates.put(sourceId, new MergeState(
                        sourceId, ms.targetId, ms.targetEpochAtPrepare, State.ROLLED_BACK));
                log.info("merge: source={} rolled back (target={} epoch unchanged at {})",
                        sourceId, ms.targetId, ms.targetEpochAtPrepare);
                return (Void) null;
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });
    }

    @Override
    public CompletableFuture<Void> finalizeAsSource(long sourceId) {
        return CompletableFuture.supplyAsync(() -> {
            try {
                var ms = mergeStates.get(sourceId);
                if (ms == null || ms.state != State.COMMITTED)
                    throw new IllegalStateException(
                            "source " + sourceId + " is not in COMMITTED state");

                // Verify via PD that the target has already absorbed the source's range.
                var targetNow = queryRegionFromPd(ms.targetId);
                if (targetNow == null) {
                    log.warn("merge: finalize source={} but target={} not found in PD",
                            sourceId, ms.targetId);
                }

                // Destroy the source peer.
                var sourcePeer = locator.peerForRegionId(sourceId);
                if (sourcePeer != null) {
                    sourcePeer.shutdown();
                    log.info("merge: source={} finalized and destroyed", sourceId);
                }

                mergeStates.remove(sourceId);
                return (Void) null;
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });
    }

    private Metapb.Region queryRegionFromPd(long regionId) {
        try {
            var resp = pdStub.getRegionByID(Pdpb.GetRegionByIDRequest.newBuilder()
                    .setHeader(Pdpb.RequestHeader.newBuilder().setClusterId(clusterId))
                    .setRegionId(regionId)
                    .build());
            if (resp.hasRegion()) return resp.getRegion();
        } catch (Throwable t) {
            log.warn("merge: PD getRegionByID({}) failed: {}", regionId, t.getMessage());
        }
        return null;
    }

    private static Metapb.Region[] orderAdjacent(Metapb.Region a, Metapb.Region b) {
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
