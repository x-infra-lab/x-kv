package io.github.xinfra.lab.xkv.pd.state;

import com.google.protobuf.ByteString;
import io.github.xinfra.lab.xkv.proto.Metapb;
import io.github.xinfra.lab.xkv.proto.Pdpb;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.HashSet;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Merges adjacent small regions whose approximate size is below
 * {@code mergeThresholdBytes}.
 *
 * <h3>Algorithm (each tick)</h3>
 *
 * <ol>
 *   <li>Scan all regions. For each region whose {@code approximateSize}
 *       is below threshold, look up the right neighbor (the region whose
 *       {@code startKey == thisRegion.endKey}).</li>
 *   <li>If the neighbor is also below threshold and both regions are hosted
 *       on the same set of stores, schedule a {@code Merge} operator on the
 *       source (left) region with the neighbor as target.</li>
 *   <li>Skip regions that already have pending operators or that were
 *       already paired this round.</li>
 * </ol>
 *
 * <p>Caps at {@link #MAX_MERGES_PER_TICK} = 2 because merges are
 * heavyweight (PrepareMerge → CommitMerge through Raft).
 */
public final class MergeCheckerScheduler implements AutoCloseable {
    private static final Logger log = LoggerFactory.getLogger(MergeCheckerScheduler.class);

    public static final int MAX_MERGES_PER_TICK = 2;

    private final PdStateMachine state;
    private final OperatorController controller;
    private final long mergeThresholdBytes;
    private final long intervalMs;
    private final ScheduledExecutorService timer;
    private final AtomicLong ticksTotal = new AtomicLong();
    private final AtomicLong operatorsScheduled = new AtomicLong();
    private volatile boolean closed = false;
    private volatile boolean paused = false;

    public MergeCheckerScheduler(PdStateMachine state,
                                 OperatorController controller,
                                 long mergeThresholdBytes,
                                 long intervalMs) {
        this.state = state;
        this.controller = controller;
        this.mergeThresholdBytes = mergeThresholdBytes;
        this.intervalMs = intervalMs;
        this.timer = Executors.newSingleThreadScheduledExecutor(r -> {
            var t = new Thread(r, "pd-merge-checker");
            t.setDaemon(true);
            return t;
        });
    }

    public void start() {
        timer.scheduleWithFixedDelay(this::tickSafely, intervalMs, intervalMs, TimeUnit.MILLISECONDS);
        log.info("MergeCheckerScheduler started: interval={}ms threshold={}B",
                intervalMs, mergeThresholdBytes);
    }

    public long ticksTotal() { return ticksTotal.get(); }
    public long operatorsScheduled() { return operatorsScheduled.get(); }

    public void pause()  { paused = true; }
    public void resume() { paused = false; }
    public boolean isPaused() { return paused; }

    private void tickSafely() {
        if (closed || paused) return;
        try { runOnce(); }
        catch (Throwable t) { log.warn("merge-checker tick failed: {}", t.getMessage()); }
    }

    /** Visible for tests. Returns merges enqueued this round. */
    public int runOnce() {
        ticksTotal.incrementAndGet();

        var allStats = state.allRegionStats();
        var paired = new HashSet<Long>();
        int scheduled = 0;

        // Build an index of start_key → region for neighbor lookup.
        var byStartKey = new HashMap<ByteString, Metapb.Region>();
        for (var r : state.allRegions()) {
            byStartKey.put(r.getStartKey(), r);
        }

        for (var region : state.allRegions()) {
            if (scheduled >= MAX_MERGES_PER_TICK) break;
            long regionId = region.getId();
            if (paired.contains(regionId)) continue;
            if (controller.getOperator(regionId).isPresent()) continue;

            // Check if region is small.
            var stats = allStats.get(regionId);
            if (stats == null) continue;
            if (stats.approximateSize() >= mergeThresholdBytes) continue;

            // Find right neighbor.
            ByteString endKey = region.getEndKey();
            if (endKey.isEmpty()) continue;

            var neighbor = byStartKey.get(endKey);
            if (neighbor == null) continue;
            if (paired.contains(neighbor.getId())) continue;
            if (controller.getOperator(neighbor.getId()).isPresent()) continue;

            // Neighbor must also be small.
            var neighborStats = allStats.get(neighbor.getId());
            if (neighborStats == null) continue;
            if (neighborStats.approximateSize() >= mergeThresholdBytes) continue;

            // Both regions must be hosted on the same stores.
            if (!sameStores(region, neighbor)) continue;

            var storeIds = new HashSet<Long>();
            for (var p : region.getPeersList()) storeIds.add(p.getStoreId());
            var resp = Pdpb.RegionHeartbeatResponse.newBuilder()
                    .setRegionId(region.getId())
                    .setMerge(Pdpb.Merge.newBuilder().setTarget(neighbor).build())
                    .build();
            var op = new SimpleOperator(
                    System.nanoTime(), region.getId(), Operator.Kind.MERGE,
                    "merge-checker: merge region " + region.getId() + " into " + neighbor.getId(),
                    resp, storeIds);
            if (!controller.addOperator(op)) continue;
            operatorsScheduled.incrementAndGet();
            scheduled++;
            paired.add(regionId);
            paired.add(neighbor.getId());

            log.info("merge-checker: scheduling merge region={} → target={} " +
                            "(sizes: {}B + {}B, threshold={}B)",
                    regionId, neighbor.getId(),
                    stats.approximateSize(), neighborStats.approximateSize(),
                    mergeThresholdBytes);
        }

        return scheduled;
    }

    private static boolean sameStores(Metapb.Region a, Metapb.Region b) {
        if (a.getPeersCount() != b.getPeersCount()) return false;
        var storesA = new HashSet<Long>(a.getPeersCount());
        for (var p : a.getPeersList()) storesA.add(p.getStoreId());
        for (var p : b.getPeersList()) {
            if (!storesA.contains(p.getStoreId())) return false;
        }
        return true;
    }

    @Override
    public void close() {
        closed = true;
        timer.shutdownNow();
    }
}
