package io.github.xinfra.lab.xkv.pd.state;

import io.github.xinfra.lab.xkv.pd.state.placement.PlacementRule;
import io.github.xinfra.lab.xkv.pd.state.placement.PlacementRuleManager;
import io.github.xinfra.lab.xkv.proto.Metapb;
import io.github.xinfra.lab.xkv.proto.Pdpb;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
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
    private final OperatorController controller;
    private final StoreStatsCache storeStats;
    private final PlacementRuleManager ruleManager;
    private final long intervalMs;
    private final ScheduledExecutorService timer;
    private final AtomicLong ticksTotal = new AtomicLong();
    private final AtomicLong operatorsScheduled = new AtomicLong();
    private volatile boolean closed = false;
    private volatile boolean paused = false;

    public RuleCheckerScheduler(PdStateMachine state,
                                OperatorController controller,
                                StoreStatsCache storeStats,
                                long intervalMs) {
        this(state, controller, storeStats, null, intervalMs);
    }

    public RuleCheckerScheduler(PdStateMachine state,
                                OperatorController controller,
                                StoreStatsCache storeStats,
                                PlacementRuleManager ruleManager,
                                long intervalMs) {
        this.state = state;
        this.controller = controller;
        this.storeStats = storeStats;
        this.ruleManager = ruleManager;
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

    public void pause()  { paused = true; }
    public void resume() { paused = false; }
    public boolean isPaused() { return paused; }

    private void tickSafely() {
        if (closed || paused) return;
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

        // Build per-store region count and store lookup map for placement decisions.
        var regionCountByStore = new HashMap<Long, Integer>();
        var allStores = new ArrayList<Metapb.Store>();
        var storeMap = new HashMap<Long, Metapb.Store>();
        for (var s : state.allStores()) {
            allStores.add(s);
            storeMap.put(s.getId(), s);
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
            if (controller.getOperator(region.getId()).isPresent()) continue;

            // 1. Replace Down/Tombstone peers.
            for (var peer : region.getPeersList()) {
                if (scheduled >= MAX_OPERATORS_PER_TICK) break;
                var storeOpt = state.getStore(peer.getStoreId());
                if (storeOpt.isEmpty()) continue;
                var storeState = storeOpt.get().getState();
                if (storeState == Metapb.StoreState.Down
                        || storeState == Metapb.StoreState.Tombstone) {
                    var resp = Pdpb.RegionHeartbeatResponse.newBuilder()
                            .setRegionId(region.getId())
                            .setChangePeer(peer)
                            .addChangePeerV2(Pdpb.ChangePeer.newBuilder()
                                    .setPeer(peer).setChangeType(Pdpb.ConfChangeType.RemoveNode).build())
                            .build();
                    var op = new SimpleOperator(
                            System.nanoTime(), region.getId(), Operator.Kind.RULE_FIX,
                            "rule-checker: remove " + storeState + " peer " + peer.getId(),
                            resp, java.util.Set.of(peer.getStoreId()));
                    if (!controller.addOperator(op)) break;
                    operatorsScheduled.incrementAndGet();
                    scheduled++;
                    log.info("rule-checker: removing {} peer {} from region={} (store {} is {})",
                            storeState, peer.getId(), region.getId(),
                            peer.getStoreId(), storeState);
                    break;
                }
            }

            if (scheduled >= MAX_OPERATORS_PER_TICK) break;
            if (controller.getOperator(region.getId()).isPresent()) continue;

            // 2. Rule-based placement check.
            List<PlacementRule> rules = ruleManager != null
                    ? ruleManager.rulesForRegion(region)
                    : List.of();

            if (rules.isEmpty()) {
                // Fallback: simple count-based check (backward compat).
                scheduled += checkByCount(region, maxPeerCount, allStores,
                        regionCountByStore, scheduled);
            } else {
                // Label-aware: iterate rules and enforce each.
                scheduled += checkByRules(region, rules, allStores, storeMap,
                        regionCountByStore, scheduled);
            }
        }

        return scheduled;
    }

    private int checkByCount(Metapb.Region region, int maxPeerCount,
                              List<Metapb.Store> allStores,
                              HashMap<Long, Integer> regionCountByStore,
                              int alreadyScheduled) {
        int scheduled = 0;
        int peerCount = region.getPeersCount();

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
                        .setId(newPeerId).setStoreId(bestStore)
                        .setRole(Metapb.PeerRole.Voter).build();
                var resp = Pdpb.RegionHeartbeatResponse.newBuilder()
                        .setRegionId(region.getId()).setChangePeer(newPeer)
                        .addChangePeerV2(Pdpb.ChangePeer.newBuilder()
                                .setPeer(newPeer).setChangeType(Pdpb.ConfChangeType.AddNode))
                        .build();
                var op = new SimpleOperator(System.nanoTime(), region.getId(),
                        Operator.Kind.RULE_FIX,
                        "rule-checker: add peer on store " + bestStore,
                        resp, java.util.Set.of(bestStore));
                if (controller.addOperator(op)) {
                    operatorsScheduled.incrementAndGet();
                    scheduled++;
                    regionCountByStore.merge(bestStore, 1, Integer::sum);
                    log.info("rule-checker: under-replicated region={} ({}/{}) — adding on store {}",
                            region.getId(), peerCount, maxPeerCount, bestStore);
                }
            }
        } else if (peerCount > maxPeerCount) {
            Metapb.Peer victim = null;
            int victimCount = Integer.MIN_VALUE;
            for (var p : region.getPeersList()) {
                boolean isLeader = (p.getId() == region.getPeers(0).getId());
                int cnt = regionCountByStore.getOrDefault(p.getStoreId(), 0);
                int priority = isLeader ? cnt : cnt + 1000;
                if (priority > victimCount) {
                    victimCount = priority;
                    victim = p;
                }
            }
            if (victim != null && (alreadyScheduled + scheduled) < MAX_OPERATORS_PER_TICK) {
                var resp = Pdpb.RegionHeartbeatResponse.newBuilder()
                        .setRegionId(region.getId()).setChangePeer(victim)
                        .addChangePeerV2(Pdpb.ChangePeer.newBuilder()
                                .setPeer(victim).setChangeType(Pdpb.ConfChangeType.RemoveNode))
                        .build();
                var op = new SimpleOperator(System.nanoTime(), region.getId(),
                        Operator.Kind.RULE_FIX,
                        "rule-checker: remove peer " + victim.getId() + " from store " + victim.getStoreId(),
                        resp, java.util.Set.of(victim.getStoreId()));
                if (controller.addOperator(op)) {
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

    private int checkByRules(Metapb.Region region, List<PlacementRule> rules,
                              List<Metapb.Store> allStores,
                              HashMap<Long, Metapb.Store> storeMap,
                              HashMap<Long, Integer> regionCountByStore,
                              int alreadyScheduled) {
        int scheduled = 0;

        for (var rule : rules) {
            if ((alreadyScheduled + scheduled) >= MAX_OPERATORS_PER_TICK) break;
            if (controller.getOperator(region.getId()).isPresent()) break;

            int satisfied = 0;
            var existingStores = new HashSet<Long>();
            var peerStoresForRule = new ArrayList<Metapb.Store>();

            for (var peer : region.getPeersList()) {
                if (!rule.matchesPeerRole(peer)) continue;
                var store = storeMap.get(peer.getStoreId());
                if (store == null) continue;
                existingStores.add(peer.getStoreId());
                if (rule.storeMatchesConstraints(store)) {
                    satisfied++;
                    peerStoresForRule.add(store);
                }
            }

            if (satisfied < rule.count()) {
                // Under-satisfied: add peer on best fitting store.
                long bestStore = -1;
                int bestScore = -1;
                int bestRegionCount = Integer.MAX_VALUE;

                for (var s : allStores) {
                    if (existingStores.contains(s.getId())) continue;
                    if (!isHealthy(s)) continue;
                    if (storeStats.isBusy(s.getId())) continue;
                    if (!rule.storeMatchesConstraints(s)) continue;

                    int score = ruleManager != null
                            ? ruleManager.isolationScore(s, peerStoresForRule, rule.locationLabels())
                            : 0;
                    int cnt = regionCountByStore.getOrDefault(s.getId(), 0);

                    if (score > bestScore || (score == bestScore && cnt < bestRegionCount)) {
                        bestScore = score;
                        bestRegionCount = cnt;
                        bestStore = s.getId();
                    }
                }

                if (bestStore >= 0) {
                    long newPeerId = state.allocId(1);
                    Metapb.PeerRole peerRole = rule.isLearner()
                            ? Metapb.PeerRole.Learner : Metapb.PeerRole.Voter;
                    Pdpb.ConfChangeType changeType = rule.isLearner()
                            ? Pdpb.ConfChangeType.AddLearnerNode : Pdpb.ConfChangeType.AddNode;
                    var newPeer = Metapb.Peer.newBuilder()
                            .setId(newPeerId).setStoreId(bestStore)
                            .setRole(peerRole).build();
                    var resp = Pdpb.RegionHeartbeatResponse.newBuilder()
                            .setRegionId(region.getId()).setChangePeer(newPeer)
                            .addChangePeerV2(Pdpb.ChangePeer.newBuilder()
                                    .setPeer(newPeer).setChangeType(changeType))
                            .build();
                    var op = new SimpleOperator(System.nanoTime(), region.getId(),
                            Operator.Kind.RULE_FIX,
                            "rule-checker: add " + rule.role() + " on store " + bestStore
                                    + " (rule=" + rule.key() + ", isolation=" + bestScore + ")",
                            resp, java.util.Set.of(bestStore));
                    if (controller.addOperator(op)) {
                        operatorsScheduled.incrementAndGet();
                        scheduled++;
                        regionCountByStore.merge(bestStore, 1, Integer::sum);
                        log.info("rule-checker: rule {} under-satisfied for region={} ({}/{}) — adding {} on store {} (isolation={})",
                                rule.key(), region.getId(), satisfied, rule.count(),
                                rule.role(), bestStore, bestScore);
                    }
                }
            } else if (satisfied > rule.count()) {
                // Over-satisfied: remove least-isolated peer matching this rule.
                Metapb.Peer victim = null;
                int worstScore = Integer.MAX_VALUE;
                int worstRegionCount = Integer.MIN_VALUE;

                for (var peer : region.getPeersList()) {
                    if (!rule.matchesPeerRole(peer)) continue;
                    var store = storeMap.get(peer.getStoreId());
                    if (store == null || !rule.storeMatchesConstraints(store)) continue;

                    boolean isLeader = (peer.getId() == region.getPeers(0).getId());
                    if (isLeader) continue;

                    var otherPeerStores = new ArrayList<Metapb.Store>();
                    for (var ps : peerStoresForRule) {
                        if (ps.getId() != store.getId()) otherPeerStores.add(ps);
                    }
                    int score = ruleManager != null
                            ? ruleManager.isolationScore(store, otherPeerStores, rule.locationLabels())
                            : 0;
                    int cnt = regionCountByStore.getOrDefault(peer.getStoreId(), 0);

                    if (score < worstScore || (score == worstScore && cnt > worstRegionCount)) {
                        worstScore = score;
                        worstRegionCount = cnt;
                        victim = peer;
                    }
                }

                if (victim != null) {
                    var resp = Pdpb.RegionHeartbeatResponse.newBuilder()
                            .setRegionId(region.getId()).setChangePeer(victim)
                            .addChangePeerV2(Pdpb.ChangePeer.newBuilder()
                                    .setPeer(victim).setChangeType(Pdpb.ConfChangeType.RemoveNode))
                            .build();
                    var op = new SimpleOperator(System.nanoTime(), region.getId(),
                            Operator.Kind.RULE_FIX,
                            "rule-checker: remove " + rule.role() + " peer " + victim.getId()
                                    + " from store " + victim.getStoreId()
                                    + " (rule=" + rule.key() + ")",
                            resp, java.util.Set.of(victim.getStoreId()));
                    if (controller.addOperator(op)) {
                        operatorsScheduled.incrementAndGet();
                        scheduled++;
                        log.info("rule-checker: rule {} over-satisfied for region={} ({}/{}) — removing peer {} on store {}",
                                rule.key(), region.getId(), satisfied, rule.count(),
                                victim.getId(), victim.getStoreId());
                    }
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
