package io.github.xinfra.lab.xkv.pd.state;

import io.github.xinfra.lab.xkv.common.metrics.XKvMetrics;
import io.github.xinfra.lab.xkv.proto.Pdpb;
import io.micrometer.core.instrument.Counter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Periodically checks region sizes reported via heartbeats. When a region's
 * approximate_size exceeds the configured split threshold, schedules an
 * {@code APPROXIMATE} split operator for that region.
 *
 * <p>The KV leader receives the split operator via the heartbeat response
 * stream, computes the midpoint key locally (since PD does not read KV data),
 * and drives the split through {@code SplitDriver}.
 */
public final class SplitCheckerScheduler implements AutoCloseable {
    private static final Logger log = LoggerFactory.getLogger(SplitCheckerScheduler.class);

    private final PdStateMachine state;
    private final OperatorController controller;
    private final long splitThresholdBytes;
    private final long intervalMs;
    private final ScheduledExecutorService timer;
    private final Counter errorCounter = XKvMetrics.errorCounter("split_checker", "tick");
    private volatile boolean paused = false;

    public SplitCheckerScheduler(PdStateMachine state,
                                  OperatorController controller,
                                  long splitThresholdBytes,
                                  long intervalMs) {
        this.state = state;
        this.controller = controller;
        this.splitThresholdBytes = splitThresholdBytes;
        this.intervalMs = intervalMs;
        this.timer = Executors.newSingleThreadScheduledExecutor(r -> {
            var t = new Thread(r, "pd-split-checker");
            t.setDaemon(true);
            return t;
        });
    }

    public void start() {
        timer.scheduleAtFixedRate(this::tickSafely, intervalMs, intervalMs, TimeUnit.MILLISECONDS);
    }

    public void pause()  { paused = true; }
    public void resume() { paused = false; }
    public boolean isPaused() { return paused; }

    private void tickSafely() {
        if (paused) return;
        try { tick(); }
        catch (Throwable t) {
            errorCounter.increment();
            log.warn("split-checker tick failed: {}", t.getMessage());
        }
    }

    private void tick() {
        for (var entry : state.allRegionStats().entrySet()) {
            long regionId = entry.getKey();
            var stats = entry.getValue();
            if (stats.approximateSize() < splitThresholdBytes) continue;
            if (controller.getOperator(regionId).isPresent()) continue;

            var regionOpt = state.getRegion(regionId);
            if (regionOpt.isEmpty()) continue;

            var region = regionOpt.get();
            var storeIds = new java.util.HashSet<Long>();
            for (var p : region.getPeersList()) storeIds.add(p.getStoreId());

            var sr = Pdpb.SplitRegion.newBuilder()
                    .setPolicy(Pdpb.SplitRegion.Policy.APPROXIMATE).build();
            var resp = Pdpb.RegionHeartbeatResponse.newBuilder()
                    .setRegionId(regionId)
                    .setSplitRegion(sr)
                    .build();
            var op = new SimpleOperator(
                    System.nanoTime(), regionId, Operator.Kind.SPLIT,
                    "split-checker: approxSize=" + stats.approximateSize(),
                    resp, storeIds);
            if (!controller.addOperator(op)) continue;

            log.info("split-checker: region={} approxSize={} exceeds threshold={}, scheduling APPROXIMATE split",
                    regionId, stats.approximateSize(), splitThresholdBytes);
        }
    }

    @Override
    public void close() {
        timer.shutdownNow();
    }
}
