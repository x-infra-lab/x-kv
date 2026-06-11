package io.github.xinfra.lab.xkv.pd.state;

import io.github.xinfra.lab.xkv.common.metrics.XKvMetrics;
import io.github.xinfra.lab.xkv.proto.Pdpb;
import io.micrometer.core.instrument.Counter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
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
    private final OperatorQueue operators;
    private final long splitThresholdBytes;
    private final long intervalMs;
    private final ScheduledExecutorService timer;
    private final Counter errorCounter = XKvMetrics.errorCounter("split_checker", "tick");

    public SplitCheckerScheduler(PdStateMachine state,
                                  OperatorQueue operators,
                                  long splitThresholdBytes,
                                  long intervalMs) {
        this.state = state;
        this.operators = operators;
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

    private void tickSafely() {
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
            if (operators.size(regionId) > 0) continue;

            var regionOpt = state.getRegion(regionId);
            if (regionOpt.isEmpty()) continue;

            log.info("split-checker: region={} approxSize={} exceeds threshold={}, scheduling APPROXIMATE split",
                    regionId, stats.approximateSize(), splitThresholdBytes);
            operators.scheduleSplit(regionId, List.of(), Pdpb.SplitRegion.Policy.APPROXIMATE);
        }
    }

    @Override
    public void close() {
        timer.shutdownNow();
    }
}
