package io.github.xinfra.lab.xkv.pd.state;

import io.github.xinfra.lab.xkv.proto.Metapb;
import io.github.xinfra.lab.xkv.proto.Pdpb;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Enforces placement rules: ensures each region has exactly
 * {@code Cluster.max_peer_count} replicas on healthy stores.
 *
 * <h3>Checks per tick</h3>
 * <ul>
 *   <li><b>Under-replicated:</b> {@code region.peersCount < maxPeerCount}
 *       — schedules {@code AddNode} on the store with the fewest regions
 *       that doesn't already host a peer and isn't Offline/Tombstone/Down.</li>
 *   <li><b>Over-replicated:</b> {@code region.peersCount > maxPeerCount}
 *       — schedules {@code RemoveNode} for the peer on the most-loaded
 *       store (preferring non-leader peers).</li>
 *   <li><b>Down/Tombstone peer:</b> if a peer's store has state Down or
 *       Tombstone, schedules {@code RemoveNode} for that peer.</li>
 * </ul>
 *
 * <p>Caps at {@link #MAX_OPERATORS_PER_TICK} = 4.
 */
public final class RuleCheckerScheduler implements AutoCloseable {
    private static final Logger log = LoggerFactory.getLogger(RuleCheckerScheduler.class);

    public static final int MAX_OPERATORS_PER_TICK = 4;

    private final PdStateMachine state;
    private final OperatorQueue operators;
    private final StoreStatsCache storeStats;
    private final long intervalMs;
    private final ScheduledExecutorService timer;
    private final AtomicLong ticksTotal = new AtomicLong();
    private final AtomicLong operatorsScheduled = new AtomicLong();
    private volatile boolean closed = false;

    public RuleCheckerScheduler(PdStateMachine state,
                                OperatorQueue operators,
                                StoreStatsCache storeStats,
                                long intervalMs) {
        this.state = state;
        this.operators = operators;
        this.storeStats = storeStats;
        this.intervalMs = intervalMs;
        this.timer = Executors.newSingleThreadScheduledExecutor(r -> {
            var t = new Thread(r, "pd-rule-checker");
            t.setDaemon(true);
            return t;
        });
    }

    public void start() {
        timer.scheduleWithFixedDelay(this::tickSafely, intervalMs, intervalMs, TimeUnit.MILLISECONDS);
        log.info("RuleCheckerScheduler started: interval={}ms", intervalMs);
    }

    public long ticksTotal() { return ticksTotal.get(); }
    public long operatorsScheduled() { return operatorsScheduled.get(); }

    private void tickSafely() {
        if (closed) return;
        try { runOnce(); }
        catch (Throwable t) { log.warn("rule-checker tick failed: {}", t.getMessage()); }
    }

    /** Visible for tests. Returns operators enqueued this round. */
    public int runOnce() {
        ticksTotal.incrementAndGet();

        var cluster = state.cluster();
        int maxPeerCount = (cluster != null && cluster.getMaxPeerCount() > 0)
                ? cluster.getMaxPeerCount()
                : 3;

        // Build per-store region count for placement decisions.
        var regionCountByStore = new HashMap<Long, Integer>();
        var allStores = new ArrayList<Metapb.Store>();
        for (var s : state.allStores()) {
            allStores.add(s);
            regionCountByStore.put(s.getId(), 0);
        }
        for (var r : state.allRegions()) {
            for (var p : r.getPeersList()) {
                regionCountByStore.merge(p.getStoreId(), 1, Integer::sum);
            }
        }

        int scheduled = 0;

        for (var region : state.allRegions()) {
            if (scheduled >= MAX_OPERATORS_PER_TICK) break;
            if (operators.size(region.getId()) > 0) continue;

            // 1. Replace Down/Tombstone peers.
            for (var peer : region.getPeersList()) {
                if (scheduled >= MAX_OPERATORS_PER_TICK) break;
                var storeOpt = state.getStore(peer.getStoreId());
                if (storeOpt.isEmpty()) continue;
                var storeState = storeOpt.get().getState();
                if (storeState == Metapb.StoreState.Down
                        || storeState == Metapb.StoreState.Tombstone) {
                    operators.scheduleChangePeer(region.getId(), peer,
                            Pdpb.ConfChangeType.RemoveNode);
                    operatorsScheduled.incrementAndGet();
                    scheduled++;
                    log.info("rule-checker: removing {} peer {} from region={} (store {} is {})",
                            storeState, peer.getId(), region.getId(),
                            peer.getStoreId(), storeState);
                    break;
                }
            }

            if (scheduled >= MAX_OPERATORS_PER_TICK) break;
            if (operators.size(region.getId()) > 0) continue;

            int peerCount = region.getPeersCount();

            // 2. Under-replicated: add a peer on the least-loaded healthy store.
            if (peerCount < maxPeerCount) {
                var existingStores = new HashSet<Long>(peerCount);
                for (var p : region.getPeersList()) existingStores.add(p.getStoreId());

                long bestStore = -1;
                int bestCount = Integer.MAX_VALUE;

                for (var s : allStores) {
                    if (existingStores.contains(s.getId())) continue;
                    if (!isHealthy(s)) continue;
                    if (storeStats.isBusy(s.getId())) continue;
                    int cnt = regionCountByStore.getOrDefault(s.getId(), 0);
                    if (cnt < bestCount) {
                        bestCount = cnt;
                        bestStore = s.getId();
                    }
                }

                if (bestStore >= 0) {
                    long newPeerId = state.allocId(1);
                    var newPeer = Metapb.Peer.newBuilder()
                            .setId(newPeerId)
                            .setStoreId(bestStore)
                            .setRole(Metapb.PeerRole.Voter)
                            .build();
                    operators.scheduleChangePeer(region.getId(), newPeer,
                            Pdpb.ConfChangeType.AddNode);
                    operatorsScheduled.incrementAndGet();
                    scheduled++;
                    regionCountByStore.merge(bestStore, 1, Integer::sum);
                    log.info("rule-checker: under-replicated region={} ({}/{}) — adding peer on store {}",
                            region.getId(), peerCount, maxPeerCount, bestStore);
                }
                continue;
            }

            // 3. Over-replicated: remove the peer on the most-loaded store.
            if (peerCount > maxPeerCount) {
                Metapb.Peer victim = null;
                int victimCount = Integer.MIN_VALUE;

                for (var p : region.getPeersList()) {
                    // Prefer removing non-leader peers (peer[0] is leader by convention).
                    boolean isLeader = (p.getId() == region.getPeers(0).getId());
                    int cnt = regionCountByStore.getOrDefault(p.getStoreId(), 0);
                    // Bias: non-leaders get a +1000 priority bump for removal.
                    int priority = isLeader ? cnt : cnt + 1000;
                    if (priority > victimCount) {
                        victimCount = priority;
                        victim = p;
                    }
                }

                if (victim != null) {
                    operators.scheduleChangePeer(region.getId(), victim,
                            Pdpb.ConfChangeType.RemoveNode);
                    operatorsScheduled.incrementAndGet();
                    scheduled++;
                    log.info("rule-checker: over-replicated region={} ({}/{}) — removing peer {} on store {}",
                            region.getId(), peerCount, maxPeerCount,
                            victim.getId(), victim.getStoreId());
                }
            }
        }

        return scheduled;
    }

    private static boolean isHealthy(Metapb.Store store) {
        return store.getState() == Metapb.StoreState.Up;
    }

    @Override
    public void close() {
        closed = true;
        timer.shutdownNow();
    }
}
