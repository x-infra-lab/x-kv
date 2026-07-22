package io.github.xinfra.lab.xkv.kv.transport;

import io.github.xinfra.lab.raft.proto.Eraftpb;
import io.github.xinfra.lab.xkv.common.metrics.XKvMetrics;
import io.micrometer.core.instrument.Counter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ConcurrentHashMap;

/**
 * Per-store routing table from region_id → local
 * {@link GrpcRaftTransport}.
 *
 * <p>The gRPC raft service ({@code KvRaftServiceImpl}) receives inbound
 * raft messages on a single bidi stream and delegates by region_id. This
 * dispatcher is the registry that lets the server find the right per-peer
 * transport without coupling the gRPC layer to {@link io.github.xinfra.lab.xkv.kv.raft.RegionPeer}.
 */
public final class RaftMessageDispatcher {
    private static final Logger log = LoggerFactory.getLogger(RaftMessageDispatcher.class);

    private final Counter deliverErrorCounter = XKvMetrics.errorCounter("raft_dispatcher", "deliver");
    private final ConcurrentHashMap<Long, GrpcRaftTransport> byRegion = new ConcurrentHashMap<>();
    /**
     * Per-region "spawn in flight" flag — avoid firing the missing-handler
     * a million times for a flood of MsgAppend retries on the same unknown
     * region. The handler clears the flag when it's done (or fails).
     */
    private final ConcurrentHashMap<Long, Boolean> spawnInFlight = new ConcurrentHashMap<>();
    /**
     * Fires when a raft message arrives for a region with no registered
     * transport. The handler typically queries PD for the region descriptor
     * and spawns a local {@code RegionPeer} if this store should host one.
     * Setting null disables on-demand spawn (messages are dropped).
     */
    private volatile MissingRegionHandler missingHandler;

    @FunctionalInterface
    public interface MissingRegionHandler {
        /**
         * Called from the gRPC server thread. Implementations should kick
         * off async work and return quickly. The dispatcher tags the region
         * as "spawning" until {@link #onSpawnDone} is called.
         *
         * @param fromStoreId store id of the sender (the leader), taken from
         *        the wire {@code RaftMessage.from_peer}. The handler needs it
         *        to wire the outbound link back to the leader when creating an
         *        uninitialized peer (the bare {@code Eraftpb.Message} only
         *        carries peer ids, not store ids).
         */
        void onMissing(long regionId, Eraftpb.Message firstMessage, long fromStoreId);
    }

    public void setMissingHandler(MissingRegionHandler h) { this.missingHandler = h; }

    public void register(long regionId, GrpcRaftTransport t) {
        byRegion.put(regionId, t);
        spawnInFlight.remove(regionId);
    }

    public void unregister(long regionId) { byRegion.remove(regionId); }

    /**
     * Notify the dispatcher that an on-demand spawn finished — pending
     * messages for this region will now be routed to the new transport (or
     * dropped if spawn failed).
     */
    public void onSpawnDone(long regionId) { spawnInFlight.remove(regionId); }

    /** Inbound: route a parsed raft message to the right local peer. */
    public void deliver(long regionId, Eraftpb.Message msg, long fromStoreId) {
        var t = byRegion.get(regionId);
        if (t != null) {
            t.deliver(msg);
            return;
        }
        // No local transport — try create-peer-on-demand. Only fire the
        // handler ONCE per region while a spawn is in flight; the rest of
        // the leader's retried MsgAppends drop. Raft retries them again
        // after the new peer is up.
        var h = missingHandler;
        if (h != null && spawnInFlight.putIfAbsent(regionId, Boolean.TRUE) == null) {
            log.info("on-demand spawn fired for region={} (triggered by {})",
                    regionId, msg.getMsgType());
            try { h.onMissing(regionId, msg, fromStoreId); }
            catch (Throwable t2) {
                deliverErrorCounter.increment();
                log.warn("on-demand spawn handler threw for region={}: {}",
                        regionId, t2.getMessage());
                spawnInFlight.remove(regionId);
            }
            return;
        }
        log.debug("no local transport for region={} (spawn in flight or no handler); dropping {}",
                regionId, msg.getMsgType());
    }
}
