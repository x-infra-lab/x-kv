package io.github.xinfra.lab.xkv.pd.state;

import io.github.xinfra.lab.xkv.proto.Metapb;
import io.github.xinfra.lab.xkv.proto.Pdpb;

import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Per-region FIFO of pending operators that PD wants the region's leader
 * to execute (transfer-leader, change-peer, split-region, merge).
 *
 * <p>The {@link PdServiceImpl#regionHeartbeat} stream pops one operator
 * per heartbeat and ships it back to the leader. The leader's
 * {@code RegionHeartbeater} processes the response — e.g., a
 * {@code transfer_leader} field triggers {@code peer.transferLeader}.
 *
 * <p>This is the channel by which the PD scheduler (when it lands) tells
 * KV nodes what to do. For now operators are inserted programmatically by
 * tests / admin RPCs.
 *
 * <h3>Why FIFO + per-region</h3>
 *
 * <p>Operators on the same region are sequentially dependent (e.g., a
 * change-peer must apply before a subsequent transfer-leader to the new
 * peer can succeed). Cross-region operators are independent, so per-region
 * queues let the scheduler push to many regions without contention.
 *
 * <p>The queue does <strong>not</strong> guarantee delivery: heartbeats
 * are best-effort. A dropped operator is re-emitted on the scheduler's
 * next pass — operators are designed to be idempotent (transferring
 * leadership to a peer that's already leader is a no-op; adding a peer
 * that's already a voter is rejected by the conf-change layer).
 */
public final class OperatorQueue {

    private final ConcurrentHashMap<Long, Pdpb.RegionHeartbeatResponse> byRegion = new ConcurrentHashMap<>();

    /** Schedule a leadership transfer to {@code target} on {@code regionId}. */
    public void scheduleTransferLeader(long regionId, Metapb.Peer target) {
        var resp = Pdpb.RegionHeartbeatResponse.newBuilder()
                .setRegionId(regionId)
                .setTransferLeader(target)
                .build();
        offer(regionId, resp);
    }

    /** Schedule a {@code change_peer} (add / remove / add-learner). */
    public void scheduleChangePeer(long regionId, Metapb.Peer peer, Pdpb.ConfChangeType type) {
        var resp = Pdpb.RegionHeartbeatResponse.newBuilder()
                .setRegionId(regionId)
                .setChangePeer(peer)
                .addChangePeerV2(Pdpb.ChangePeer.newBuilder()
                        .setPeer(peer).setChangeType(type).build())
                .build();
        offer(regionId, resp);
    }

    /** Schedule a region merge — the leader will absorb {@code target}. */
    public void scheduleMerge(long regionId, Metapb.Region target) {
        var resp = Pdpb.RegionHeartbeatResponse.newBuilder()
                .setRegionId(regionId)
                .setMerge(Pdpb.Merge.newBuilder().setTarget(target).build())
                .build();
        offer(regionId, resp);
    }

    /** Schedule a region split at the provided user-keys. */
    public void scheduleSplit(long regionId, java.util.List<byte[]> splitKeys, Pdpb.SplitRegion.Policy policy) {
        var sr = Pdpb.SplitRegion.newBuilder().setPolicy(policy);
        for (var k : splitKeys) sr.addKeys(com.google.protobuf.ByteString.copyFrom(k));
        var resp = Pdpb.RegionHeartbeatResponse.newBuilder()
                .setRegionId(regionId)
                .setSplitRegion(sr.build())
                .build();
        offer(regionId, resp);
    }

    /** Pop the operator for this region, if any. */
    public Optional<Pdpb.RegionHeartbeatResponse> poll(long regionId) {
        var op = byRegion.remove(regionId);
        return Optional.ofNullable(op);
    }

    /** Diagnostic: whether an operator is queued for this region. */
    public int size(long regionId) {
        return byRegion.containsKey(regionId) ? 1 : 0;
    }

    /** Drop every queued operator (used on PD shutdown). */
    public void clear() { byRegion.clear(); }

    void offer(long regionId, Pdpb.RegionHeartbeatResponse op) {
        byRegion.put(regionId, op);
    }
}
