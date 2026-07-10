package io.github.xinfra.lab.xkv.kv.store;

import io.github.xinfra.lab.xkv.common.metrics.XKvMetrics;
import io.github.xinfra.lab.xkv.kv.engine.StorageEngine;
import io.github.xinfra.lab.xkv.kv.raft.RegionPeer;
import io.github.xinfra.lab.xkv.proto.Metapb;
import io.github.xinfra.lab.xkv.proto.PDGrpc;
import io.github.xinfra.lab.xkv.proto.Pdpb;
import io.grpc.stub.StreamObserver;
import io.micrometer.core.instrument.Counter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Periodically publishes one region's state to PD.
 *
 * <p>Each tick the heartbeater sends a {@code RegionHeartbeatRequest}
 * carrying the region descriptor and the elected leader. PD tracks
 * the leader via a dedicated {@code updateLeader} path separate from
 * the region descriptor — the region's epoch only changes on
 * split/merge/conf-change, never on heartbeats (matching TiKV).
 *
 * <p>Only the leader sends heartbeats; follower ticks are no-ops.
 */
public final class RegionHeartbeater implements AutoCloseable {
    private static final Logger log = LoggerFactory.getLogger(RegionHeartbeater.class);

    private final PDGrpc.PDStub asyncStub;
    private final RegionPeer peer;
    private final long intervalMs;
    private final ScheduledExecutorService timer;
    private volatile StreamObserver<Pdpb.RegionHeartbeatRequest> outbound;
    private volatile boolean closed = false;
    private final SplitTrigger splitTrigger;
    private final StorageEngine engine;
    private final Counter errorCounter = XKvMetrics.errorCounter("region_heartbeater", "tick");
    private volatile java.util.concurrent.CompletableFuture<?> confChangeInFlight;

    public RegionHeartbeater(PDGrpc.PDStub asyncStub, RegionPeer peer, long intervalMs) {
        this(asyncStub, peer, intervalMs, null, null);
    }

    public RegionHeartbeater(PDGrpc.PDStub asyncStub, RegionPeer peer,
                              long intervalMs, SplitTrigger splitTrigger) {
        this(asyncStub, peer, intervalMs, splitTrigger, null);
    }

    public RegionHeartbeater(PDGrpc.PDStub asyncStub, RegionPeer peer,
                              long intervalMs, SplitTrigger splitTrigger,
                              StorageEngine engine) {
        this.asyncStub = asyncStub;
        this.peer = peer;
        this.intervalMs = intervalMs;
        this.splitTrigger = splitTrigger;
        this.engine = engine;
        this.timer = Executors.newSingleThreadScheduledExecutor(r -> {
            var t = new Thread(r, "region-heartbeat-" + peer.regionId());
            t.setDaemon(true);
            return t;
        });
    }

    /**
     * Pluggable hook for PD-ordered splits. The implementation is the
     * local SplitDriver wrapped in a closure — it knows how to find the
     * parent peer and orchestrate the propose. We deliberately keep this
     * indirection so RegionHeartbeater doesn't depend on SplitDriver.
     */
    @FunctionalInterface
    public interface SplitTrigger {
        void split(RegionPeer parent, java.util.List<byte[]> splitKeys) throws Exception;
    }

    public void start() {
        timer.scheduleAtFixedRate(this::tickSafely, 0, intervalMs, TimeUnit.MILLISECONDS);
    }

    private void tickSafely() {
        try { tick(); }
        catch (Throwable t) {
            errorCounter.increment();
            log.warn("heartbeat region={} failed: {}", peer.regionId(), t.getMessage());
            resetStream();
        }
    }

    private void tick() {
        if (closed || peer.isDestroyed()) return;
        ensureStream();
        if (outbound == null) return;
        if (!peer.isLeader()) return;

        long approxSize = 0;
        if (engine != null) {
            try {
                var region = peer.region();
                approxSize = engine.approximateSize(StorageEngine.Cf.DEFAULT,
                        region.getStartKey().toByteArray(),
                        region.getEndKey().toByteArray());
            } catch (Throwable t) {
                errorCounter.increment();
                log.warn("approximate size failed region={}: {}", peer.regionId(), t.getMessage());
            }
        }

        outbound.onNext(Pdpb.RegionHeartbeatRequest.newBuilder()
                .setRegion(peer.region())
                .setLeader(peer.self())
                .setApproximateSize(approxSize)
                .setApproximateKeys(0)
                .build());
    }

    private void ensureStream() {
        if (outbound != null) return;
        synchronized (this) {
            if (outbound != null || closed) return;
            outbound = asyncStub.regionHeartbeat(new StreamObserver<>() {
                @Override public void onNext(Pdpb.RegionHeartbeatResponse v) {
                    dispatchOperator(v);
                }
                @Override public void onError(Throwable t) {
                    errorCounter.increment();
                    log.warn("heartbeat stream error region={}: {}", peer.regionId(), t.getMessage());
                    resetStream();
                }
                @Override public void onCompleted() { resetStream(); }
            });
        }
    }

    private void resetStream() {
        synchronized (this) { outbound = null; }
    }

    /**
     * Apply an operator that PD shipped down on the heartbeat stream.
     * Currently handles transfer-leader (smallest scope); change-peer and
     * split land alongside the multi-region work.
     *
     * <p>Operators are idempotent by design — replaying a transfer to the
     * already-elected leader is a no-op; PD will retry on the next
     * heartbeat if the leader doesn't visibly land.
     */
    private void dispatchOperator(Pdpb.RegionHeartbeatResponse resp) {
        try {
            if (resp.hasTransferLeader()) {
                long target = resp.getTransferLeader().getId();
                if (target != 0 && target != peer.self().getId()) {
                    log.info("heartbeat: PD ordered transfer leader of region={} to peer={}",
                            peer.regionId(), target);
                    peer.transferLeader(target);
                }
            }
            if (resp.hasSplitRegion() && splitTrigger != null && peer.isLeader()) {
                var sr = resp.getSplitRegion();
                var keys = new java.util.ArrayList<byte[]>(sr.getKeysCount());
                for (var k : sr.getKeysList()) keys.add(k.toByteArray());
                if (keys.isEmpty() && sr.getPolicy() == Pdpb.SplitRegion.Policy.APPROXIMATE && engine != null) {
                    byte[] mid = computeApproximateMidKey(peer.region());
                    if (mid != null && mid.length > 0) keys.add(mid);
                }
                if (!keys.isEmpty()) {
                    log.info("heartbeat: PD ordered split of region={} at {} keys (policy={})",
                            peer.regionId(), keys.size(), sr.getPolicy());
                    try { splitTrigger.split(peer, keys); }
                    catch (Throwable t) {
                        log.warn("PD-ordered split of region={} failed: {}",
                                peer.regionId(), t.getMessage());
                    }
                }
            }
            // change_peer_v2 — newer typed form. Falls back to legacy
            // change_peer (no type, defaults to AddNode) if v2 is absent.
            if (peer.isLeader()) {
                var changes = !resp.getChangePeerV2List().isEmpty()
                        ? resp.getChangePeerV2List()
                        : (resp.hasChangePeer()
                                ? java.util.List.of(Pdpb.ChangePeer.newBuilder()
                                        .setPeer(resp.getChangePeer())
                                        .setChangeType(Pdpb.ConfChangeType.AddNode).build())
                                : java.util.List.<Pdpb.ChangePeer>of());
                if (!changes.isEmpty()) proposeChangePeers(changes);
            }
            // merge is wired alongside the multi-region scheduler work.
        } catch (Throwable t) {
            log.warn("heartbeat operator dispatch failed: {}", t.getMessage());
        }
    }

    /**
     * Bundle the operator's change list into one ConfChangeV2 and propose
     * it through raft. Context carries the affected {@code metapb.Peer}s
     * so the apply path can update the region descriptor.
     */
    private void proposeChangePeers(java.util.List<Pdpb.ChangePeer> changes) {
        var prev = confChangeInFlight;
        if (prev != null && !prev.isDone()) {
            log.debug("heartbeat: skipping conf-change on region={} — previous still in-flight",
                    peer.regionId());
            return;
        }

        var v2 = io.github.xinfra.lab.raft.proto.Eraftpb.ConfChangeV2.newBuilder();
        var ctx = io.github.xinfra.lab.xkv.proto.KvServerpb.ConfChangeContext.newBuilder();
        for (var cp : changes) {
            v2.addChanges(io.github.xinfra.lab.raft.proto.Eraftpb.ConfChangeSingle.newBuilder()
                    .setType(mapConfChangeType(cp.getChangeType()))
                    .setNodeId(cp.getPeer().getId())
                    .build());
            ctx.addPeers(cp.getPeer());
        }
        v2.setContext(com.google.protobuf.ByteString.copyFrom(ctx.build().toByteArray()));
        log.info("heartbeat: PD ordered conf-change on region={} ({} changes)",
                peer.regionId(), changes.size());
        var fut = peer.proposeConfChange(v2.build());
        confChangeInFlight = fut;
        fut.whenComplete((r, err) -> {
            confChangeInFlight = null;
            if (err != null || (r != null && !r.success())) {
                log.warn("PD-ordered conf-change on region={} failed: {}",
                        peer.regionId(),
                        err != null ? err.getMessage() : (r == null ? "null" : r.errorMessage()));
            }
        });
    }

    private byte[] computeApproximateMidKey(Metapb.Region region) {
        try {
            byte[] start = region.getStartKey().toByteArray();
            byte[] end = region.getEndKey().toByteArray();
            try (var opts = engine.newReadOptions()) {
                if (end.length > 0) opts.iterateUpperBound(end);
                try (var iter = engine.newIterator(StorageEngine.Cf.DEFAULT, opts)) {
                    iter.seek(start.length > 0 ? start : new byte[]{0});
                    int count = 0;
                    while (iter.isValid()) { count++; iter.next(); }
                    if (count < 2) return null;
                    int mid = count / 2;
                    iter.seek(start.length > 0 ? start : new byte[]{0});
                    for (int i = 0; i < mid && iter.isValid(); i++) iter.next();
                    return iter.isValid() ? iter.key() : null;
                }
            }
        } catch (Throwable t) {
            log.debug("computeApproximateMidKey failed region={}: {}", region.getId(), t.getMessage());
            return null;
        }
    }

    private static io.github.xinfra.lab.raft.proto.Eraftpb.ConfChangeType mapConfChangeType(
            Pdpb.ConfChangeType pdType) {
        return switch (pdType) {
            case AddNode -> io.github.xinfra.lab.raft.proto.Eraftpb.ConfChangeType.ConfChangeAddNode;
            case RemoveNode -> io.github.xinfra.lab.raft.proto.Eraftpb.ConfChangeType.ConfChangeRemoveNode;
            case AddLearnerNode -> io.github.xinfra.lab.raft.proto.Eraftpb.ConfChangeType.ConfChangeAddLearnerNode;
            default -> io.github.xinfra.lab.raft.proto.Eraftpb.ConfChangeType.ConfChangeAddNode;
        };
    }

    @Override
    public void close() {
        closed = true;
        timer.shutdownNow();
        synchronized (this) {
            if (outbound != null) {
                try { outbound.onCompleted(); } catch (Throwable e) {
                    log.warn("outbound onCompleted failed: {}", e.getMessage());
                }
                outbound = null;
            }
        }
    }
}
