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

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Orchestrates a region split end-to-end on the leader side.
 *
 * <h3>Wire</h3>
 *
 * <ol>
 *   <li>{@link Pdpb.AskBatchSplitRequest} — PD allocates the new region IDs
 *       (one per child) and per-child peer IDs (one per existing peer).</li>
 *   <li>Build a {@link KvServerpb.SplitRegionProposal} describing the
 *       shrunken parent + each child, with epoch.version bumped on every
 *       region.</li>
 *   <li>{@link RegionPeer#propose} an {@code ADMIN_SPLIT} entry through
 *       raft; on apply the regions are persisted atomically, the parent's
 *       in-memory descriptor is refreshed, and the split observer fires
 *       for each child (which the {@code Store} hooks for peer spawn).</li>
 * </ol>
 *
 * <p>Returns the resulting region descriptors so the caller (e.g. a SQL
 * layer driving DDL) can report which regions now own which range.
 */
public final class SplitDriver {
    private static final Logger log = LoggerFactory.getLogger(SplitDriver.class);

    private final PDGrpc.PDBlockingStub pd;
    private final long proposeTimeoutMs;

    public SplitDriver(PDGrpc.PDBlockingStub pd, long proposeTimeoutMs) {
        this.pd = pd;
        this.proposeTimeoutMs = proposeTimeoutMs;
    }

    /**
     * Split {@code parentPeer}'s region at the supplied {@code splitKeys}
     * (must be strictly increasing, all within the parent's current range).
     * Returns the list of {@code N+1} resulting regions in left-to-right
     * order — the first entry is the shrunken parent; the rest are new
     * children.
     */
    public List<Metapb.Region> split(RegionPeer parentPeer, List<byte[]> splitKeys) throws Exception {
        if (splitKeys.isEmpty()) throw new IllegalArgumentException("splitKeys must not be empty");

        var parent = parentPeer.region();
        validateSplitKeys(parent, splitKeys);

        // PD-side ID allocation. Each child needs a fresh region_id and one
        // fresh peer_id per existing peer slot.
        var idsResp = pd.askBatchSplit(Pdpb.AskBatchSplitRequest.newBuilder()
                .setRegion(parent)
                .setSplitCount(splitKeys.size())
                .build());
        if (idsResp.getIdsCount() != splitKeys.size()) {
            throw new IllegalStateException("PD returned " + idsResp.getIdsCount()
                    + " SplitIDs, expected " + splitKeys.size());
        }

        // Build the proposal. The parent's range becomes [parent.start, splitKeys[0]);
        // child i has range [splitKeys[i-1], splitKeys[i]) (or [splitKeys[N-1], parent.end)
        // for the last child). epoch.version is bumped on EVERY region — the
        // standard rule in TiKV: any region range change must increment version.
        long bumpedVersion = parent.getRegionEpoch().getVersion() + 1;
        var updatedParent = parent.toBuilder()
                .setEndKey(ByteString.copyFrom(splitKeys.get(0)))
                .setRegionEpoch(parent.getRegionEpoch().toBuilder().setVersion(bumpedVersion))
                .build();

        var proposal = KvServerpb.SplitRegionProposal.newBuilder().setUpdatedParent(updatedParent);
        var out = new ArrayList<Metapb.Region>(splitKeys.size() + 1);
        out.add(updatedParent);

        for (int i = 0; i < splitKeys.size(); i++) {
            byte[] start = splitKeys.get(i);
            byte[] end = i + 1 < splitKeys.size()
                    ? splitKeys.get(i + 1)
                    : parent.getEndKey().toByteArray();
            var ids = idsResp.getIds(i);
            var childBuilder = Metapb.Region.newBuilder()
                    .setId(ids.getNewRegionId())
                    .setStartKey(ByteString.copyFrom(start))
                    .setEndKey(ByteString.copyFrom(end))
                    .setRegionEpoch(Metapb.RegionEpoch.newBuilder()
                            .setConfVer(parent.getRegionEpoch().getConfVer())
                            .setVersion(bumpedVersion)
                            .build());
            // Child inherits the parent's peer set, with new peer IDs.
            for (int p = 0; p < parent.getPeersCount(); p++) {
                var parentPeerMeta = parent.getPeers(p);
                childBuilder.addPeers(Metapb.Peer.newBuilder()
                        .setId(ids.getNewPeerIds(p))
                        .setStoreId(parentPeerMeta.getStoreId())
                        .setRole(parentPeerMeta.getRole())
                        .build());
            }
            var child = childBuilder.build();
            proposal.addChildren(child);
            out.add(child);
        }

        // Propose through raft. The apply path will atomically persist all
        // regions and fire the split observer.
        var envelope = ProposalCodec.encode(
                ProposalCodec.Kind.ADMIN_SPLIT, /* seq= */ 0, proposal.build().toByteArray());
        var fut = parentPeer.propose(new RegionPeer.Proposal(envelope, 0, 0));
        var result = fut.get(proposeTimeoutMs, TimeUnit.MILLISECONDS);
        if (!result.success()) {
            throw new IllegalStateException("ADMIN_SPLIT propose failed: " + result.errorMessage());
        }
        log.info("split: region={} → {} regions (split at {} keys)",
                parent.getId(), out.size(), splitKeys.size());
        return out;
    }

    private void validateSplitKeys(Metapb.Region parent, List<byte[]> splitKeys) {
        byte[] start = parent.getStartKey().toByteArray();
        byte[] end = parent.getEndKey().toByteArray();
        byte[] prev = start;
        for (int i = 0; i < splitKeys.size(); i++) {
            byte[] k = splitKeys.get(i);
            if (k == null || k.length == 0) {
                throw new IllegalArgumentException("split key " + i + " is empty");
            }
            if (prev.length > 0 && java.util.Arrays.compareUnsigned(k, prev) <= 0) {
                throw new IllegalArgumentException(
                        "split keys not strictly increasing at index " + i);
            }
            if (end.length > 0 && java.util.Arrays.compareUnsigned(k, end) >= 0) {
                throw new IllegalArgumentException("split key " + i + " >= parent.end_key");
            }
            prev = k;
        }
    }
}
