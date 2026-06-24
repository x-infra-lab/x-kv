package io.github.xinfra.lab.xkv.pd.state;

import io.github.xinfra.lab.xkv.proto.Metapb;
import io.github.xinfra.lab.xkv.proto.Pdpb;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Balances hot regions across stores by transferring leadership away from
 * stores that host disproportionately many hot regions.
 *
 * <p>A region is "hot" when its approximate key/byte throughput (reported
 * in region stats) exceeds twice the cluster-wide average. The scheduler
 * identifies the store with the most hot-region leaders and transfers one
 * to the least-hot store that has a follower for that region.
 *
 * <p>Conservative by design: caps at {@link #MAX_OPERATORS_PER_TICK} = 2,
 * because hot-region moves may cause transient throughput drops on the
 * source and target stores.
 */
public final class HotRegionScheduler implements AutoCloseable {
    private static final Logger log = LoggerFactory.getLogger(HotRegionScheduler.class);

    public static final int MAX_OPERATORS_PER_TICK = 2;
    private static final double HOT_MULTIPLIER = 2.0;

    private final PdStateMachine state;
    private final OperatorControllerImpl controller;
    private final OperatorQueue operators;
    private final StoreStatsCache storeStats;
    private final long intervalMs;
    private final ScheduledExecutorService timer;
    private final AtomicLong ticksTotal = new AtomicLong();
    private final AtomicLong operatorsScheduled = new AtomicLong();
    private volatile boolean closed = false;
    private volatile boolean paused = false;

    public HotRegionScheduler(PdStateMachine state,
                              OperatorControllerImpl controller,
                              OperatorQueue operators,
                              StoreStatsCache storeStats,
                              long intervalMs) {
        this.state = state;
        this.controller = controller;
        this.operators = operators;
        this.storeStats = storeStats;
        this.intervalMs = intervalMs;
        this.timer = Executors.newSingleThreadScheduledExecutor(r -> {
            var t = new Thread(r, "pd-hot-region");
            t.setDaemon(true);
            return t;
        });
    }

    public void start() {
        timer.scheduleWithFixedDelay(this::tickSafely, intervalMs, intervalMs, TimeUnit.MILLISECONDS);
        log.info("HotRegionScheduler started: interval={}ms", intervalMs);
    }

    public long ticksTotal() { return ticksTotal.get(); }
    public long operatorsScheduled() { return operatorsScheduled.get(); }

    public void pause()  { paused = true; }
    public void resume() { paused = false; }
    public boolean isPaused() { return paused; }

    private void tickSafely() {
        if (closed || paused) return;
        try { runOnce(); }
        catch (Throwable t) { log.warn("hot-region tick failed: {}", t.getMessage()); }
    }

    /** Visible for tests. Returns operators enqueued this round. */
    public int runOnce() {
        ticksTotal.incrementAndGet();

        var allStats = state.allRegionStats();
        if (allStats.isEmpty()) return 0;

        long totalLoad = 0;
        for (var s : allStats.values()) {
            totalLoad += s.approximateKeys();
        }
        long avgLoad = totalLoad / allStats.size();
        long hotThreshold = (long) (avgLoad * HOT_MULTIPLIER);
        if (hotThreshold <= 0) return 0;

        record HotRegion(long regionId, long load) {}
        var hotRegions = new ArrayList<HotRegion>();
        for (var entry : allStats.entrySet()) {
            long load = entry.getValue().approximateKeys();
            if (load > hotThreshold) {
                hotRegions.add(new HotRegion(entry.getKey(), load));
            }
        }
        if (hotRegions.isEmpty()) return 0;
        hotRegions.sort(Comparator.comparingLong(HotRegion::load).reversed());

        // Count hot regions per leader-store.
        var hotByStore = new HashMap<Long, Integer>();
        var regionMap = new HashMap<Long, Metapb.Region>();
        for (var hr : hotRegions) {
            var regionOpt = state.getRegion(hr.regionId());
            if (regionOpt.isEmpty()) continue;
            var region = regionOpt.get();
            if (region.getPeersCount() == 0) continue;
            regionMap.put(hr.regionId(), region);
            long leaderStore = region.getPeers(0).getStoreId();
            hotByStore.merge(leaderStore, 1, Integer::sum);
        }

        int storeCount = 0;
        for (var s : state.allStores()) storeCount++;
        if (storeCount < 2) return 0;

        int scheduled = 0;

        for (var hr : hotRegions) {
            if (scheduled >= MAX_OPERATORS_PER_TICK) break;
            var region = regionMap.get(hr.regionId());
            if (region == null) continue;
            if (operators.size(region.getId()) > 0) continue;

            long leaderStore = region.getPeers(0).getStoreId();
            int leaderHotCount = hotByStore.getOrDefault(leaderStore, 0);

            // Find the store with fewest hot leaders that has a peer for this region.
            Metapb.Peer bestTarget = null;
            int bestHotCount = Integer.MAX_VALUE;

            for (var peer : region.getPeersList()) {
                if (peer.getStoreId() == leaderStore) continue;
                if (storeStats.isBusy(peer.getStoreId())) continue;
                int peerHotCount = hotByStore.getOrDefault(peer.getStoreId(), 0);
                if (peerHotCount < bestHotCount) {
                    bestHotCount = peerHotCount;
                    bestTarget = peer;
                }
            }

            if (bestTarget == null) continue;
            if (leaderHotCount - bestHotCount <= 1) continue;

            var resp = Pdpb.RegionHeartbeatResponse.newBuilder()
                    .setRegionId(region.getId())
                    .setTransferLeader(bestTarget)
                    .build();
            var op = new SimpleOperator(
                    System.nanoTime(), region.getId(), Operator.Kind.HOT_REGION,
                    "hot-region: transfer leader from store " + leaderStore + " to " + bestTarget.getStoreId(),
                    resp, Set.of(bestTarget.getStoreId()));

            if (controller.addOperator(op)) {
                operatorsScheduled.incrementAndGet();
                scheduled++;
                hotByStore.merge(leaderStore, -1, Integer::sum);
                hotByStore.merge(bestTarget.getStoreId(), 1, Integer::sum);
            }
        }

        if (scheduled > 0) {
            log.info("hot-region: scheduled {} TransferLeader operators", scheduled);
        }
        return scheduled;
    }

    @Override
    public void close() {
        closed = true;
        timer.shutdownNow();
    }
}
