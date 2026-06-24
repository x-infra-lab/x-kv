package io.github.xinfra.lab.xkv.pd.state;

import io.github.xinfra.lab.xkv.proto.Metapb;
import io.github.xinfra.lab.xkv.proto.Pdpb;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Tiny "leader balance" scheduler — keeps the per-store leader count
 * within ±1 of the optimum.
 *
 * <h3>Algorithm (each tick)</h3>
 *
 * <ol>
 *   <li>Count regions led by each store (a region's leader is the FIRST
 *       entry in {@code Region.peers} — that's how the harness publishes
 *       leadership to PD via the {@code RegionHeartbeater}'s reorder).</li>
 *   <li>Identify the store with the most leaders ({@code overLoaded}) and
 *       the one with the fewest ({@code underLoaded}). If
 *       {@code over.count - under.count > 1}, schedule one transfer:
 *       pick a region led by {@code overLoaded} that has a non-leader
 *       peer on {@code underLoaded}, enqueue a {@code TransferLeader}
 *       operator targeting that peer.</li>
 * </ol>
 *
 * <p>Caps {@link #MAX_OPERATORS_PER_TICK} so a freshly-bootstrapped
 * cluster doesn't get hammered with cross-fire transfers; the next tick
 * picks up where this one left off as state converges.
 *
 * <p>This scheduler is intentionally simple. Phase 8+ will layer on
 * store-capacity / hot-region / placement-rule schedulers using the same
 * operator-queue plumbing.
 */
public final class LeaderBalanceScheduler implements AutoCloseable {
    private static final Logger log = LoggerFactory.getLogger(LeaderBalanceScheduler.class);

    /** Cap per round so a sudden imbalance doesn't churn the cluster. */
    public static final int MAX_OPERATORS_PER_TICK = 4;

    private final PdStateMachine state;
    private final OperatorController controller;
    private final StoreStatsCache storeStats;
    private final long intervalMs;
    private final ScheduledExecutorService timer;
    private final AtomicLong ticksTotal = new AtomicLong();
    private final AtomicLong operatorsScheduled = new AtomicLong();
    private volatile boolean closed = false;
    private volatile boolean paused = false;

    public LeaderBalanceScheduler(PdStateMachine state, OperatorController controller, long intervalMs) {
        this(state, controller, new StoreStatsCache(), intervalMs);
    }

    public LeaderBalanceScheduler(PdStateMachine state, OperatorController controller,
                                   StoreStatsCache storeStats, long intervalMs) {
        this.state = state;
        this.controller = controller;
        this.storeStats = storeStats;
        this.intervalMs = intervalMs;
        this.timer = Executors.newSingleThreadScheduledExecutor(r -> {
            var t = new Thread(r, "pd-leader-balance");
            t.setDaemon(true);
            return t;
        });
    }

    public void start() {
        timer.scheduleAtFixedRate(this::tickSafely, intervalMs, intervalMs, TimeUnit.MILLISECONDS);
        log.info("LeaderBalanceScheduler started: interval={}ms", intervalMs);
    }

    public long ticksTotal() { return ticksTotal.get(); }
    public long operatorsScheduled() { return operatorsScheduled.get(); }

    public void pause()  { paused = true; }
    public void resume() { paused = false; }
    public boolean isPaused() { return paused; }

    private void tickSafely() {
        if (closed || paused) return;
        try { runOnce(); }
        catch (Throwable t) { log.warn("scheduler tick failed: {}", t.getMessage()); }
    }

    /** Visible for tests — fire one round synchronously. Returns operators enqueued this round. */
    public int runOnce() {
        ticksTotal.incrementAndGet();

        // Bucket regions by current leader's store.
        record RegionInfo(Metapb.Region region, long leaderStoreId, Metapb.Peer leader) {}
        var byLeaderStore = new HashMap<Long, java.util.List<RegionInfo>>();
        // Seed empty buckets for every known store so a store with ZERO
        // leaders is a valid transfer target (otherwise the imbalance
        // detector silently ignores starved stores).
        for (var s : state.allStores()) {
            byLeaderStore.put(s.getId(), new ArrayList<>());
        }
        for (var r : state.allRegions()) {
            if (r.getPeersCount() == 0) continue;
            var leader = r.getPeers(0);    // by convention the first peer is the leader
            byLeaderStore.computeIfAbsent(leader.getStoreId(),
                    k -> new ArrayList<>()).add(new RegionInfo(r, leader.getStoreId(), leader));
        }
        if (byLeaderStore.size() < 2) return 0;     // single store hosts everything; nothing to balance

        int scheduled = 0;
        // Keep balancing while any pair is more than 1 apart and we still
        // have budget. Each iteration moves one leader from max to min.
        var counts = new HashMap<Long, Integer>();
        for (var e : byLeaderStore.entrySet()) counts.put(e.getKey(), e.getValue().size());

        while (scheduled < MAX_OPERATORS_PER_TICK) {
            long maxStore = -1;
            long minStore = -1;
            int max = Integer.MIN_VALUE;
            int min = Integer.MAX_VALUE;
            int maxSlow = -1;
            for (var e : counts.entrySet()) {
                int slow = storeStats.slowScore(e.getKey());
                // Prefer moving leaders off the slowest overloaded store.
                if (e.getValue() > max || (e.getValue() == max && slow > maxSlow)) {
                    max = e.getValue(); maxStore = e.getKey(); maxSlow = slow;
                }
                // Don't pick busy stores as transfer targets.
                if (e.getValue() < min && !storeStats.isBusy(e.getKey())) {
                    min = e.getValue(); minStore = e.getKey();
                }
            }
            if (minStore == -1 || max - min <= 1) break;

            // Find a region currently led by maxStore that has a non-leader
            // peer on minStore — that peer is the transfer target.
            var candidates = byLeaderStore.get(maxStore);
            RegionInfo move = null;
            Metapb.Peer target = null;
            for (var ri : candidates) {
                for (var p : ri.region().getPeersList()) {
                    if (p.getStoreId() == minStore && p.getId() != ri.leader().getId()) {
                        move = ri;
                        target = p;
                        break;
                    }
                }
                if (move != null) break;
            }
            if (move == null) {
                counts.put(minStore, max);
                continue;
            }

            var resp = Pdpb.RegionHeartbeatResponse.newBuilder()
                    .setRegionId(move.region().getId())
                    .setTransferLeader(target)
                    .build();
            var op = new SimpleOperator(
                    System.nanoTime(), move.region().getId(), Operator.Kind.BALANCE_LEADER,
                    "leader-balance: transfer leader to store " + target.getStoreId(),
                    resp, java.util.Set.of(target.getStoreId()));
            if (!controller.addOperator(op)) continue;
            operatorsScheduled.incrementAndGet();
            scheduled++;
            counts.merge(maxStore, -1, Integer::sum);
            counts.merge(minStore, 1, Integer::sum);
            // Re-bucket: the region's new leader is on minStore.
            byLeaderStore.get(maxStore).remove(move);
            byLeaderStore.computeIfAbsent(minStore, k -> new ArrayList<>())
                    .add(new RegionInfo(move.region(), minStore, target));
        }
        if (scheduled > 0) {
            log.info("leader-balance: scheduled {} TransferLeader operators", scheduled);
        }
        return scheduled;
    }

    @Override
    public void close() {
        closed = true;
        timer.shutdownNow();
    }
}
