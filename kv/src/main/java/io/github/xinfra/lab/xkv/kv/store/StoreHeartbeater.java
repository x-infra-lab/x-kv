package io.github.xinfra.lab.xkv.kv.store;

import io.github.xinfra.lab.xkv.common.metrics.XKvMetrics;
import io.github.xinfra.lab.xkv.kv.transport.PdEndpointManager;
import io.github.xinfra.lab.xkv.proto.Pdpb;
import io.micrometer.core.instrument.Counter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.nio.file.Path;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Periodically reports store-level statistics to PD via the
 * {@code StoreHeartbeat} RPC.
 *
 * <p>Collects: region count, disk capacity/available/used, and the busy flag.
 * Throughput counters (keys/bytes read/written) are zeroed for now — wiring
 * them requires per-apply-path counting that will land in a later phase.
 *
 * <p>Disk stats come from {@link File#getTotalSpace()}/{@link File#getFreeSpace()}
 * on the data directory. This is a JVM-portable approximation; on Linux it
 * matches the underlying filesystem's {@code statfs}.
 */
public final class StoreHeartbeater implements AutoCloseable {
    private static final Logger log = LoggerFactory.getLogger(StoreHeartbeater.class);

    private final long storeId;
    private final PdEndpointManager pdManager;
    private final StoreImpl store;
    private final Path dataDir;
    private final ScheduledExecutorService timer;
    private volatile boolean closed;
    private final Counter errorCounter = XKvMetrics.errorCounter("store_heartbeater", "tick");

    public StoreHeartbeater(long storeId, PdEndpointManager pdManager,
                            StoreImpl store, Path dataDir, long intervalMs) {
        this.storeId = storeId;
        this.pdManager = pdManager;
        this.store = store;
        this.dataDir = dataDir;
        this.timer = Executors.newSingleThreadScheduledExecutor(r -> {
            var t = new Thread(r, "store-heartbeat-" + storeId);
            t.setDaemon(true);
            return t;
        });
        timer.scheduleAtFixedRate(this::tickSafely, intervalMs, intervalMs, TimeUnit.MILLISECONDS);
    }

    private void tickSafely() {
        if (closed) return;
        try {
            tick();
        } catch (Throwable t) {
            errorCounter.increment();
            log.warn("store heartbeat failed: {}", t.getMessage());
        }
    }

    private void tick() {
        var dir = dataDir.toFile();
        long capacity = dir.getTotalSpace();
        long available = dir.getFreeSpace();
        long usedSize = capacity - available;
        int regionCount = store.regionCount();

        var stats = Pdpb.StoreStats.newBuilder()
                .setStoreId(storeId)
                .setCapacity(capacity)
                .setAvailable(available)
                .setUsedSize(usedSize)
                .setRegionCount(regionCount)
                .build();

        var req = Pdpb.StoreHeartbeatRequest.newBuilder()
                .setStats(stats)
                .build();

        pdManager.blockingStub().storeHeartbeat(req);
    }

    @Override
    public void close() {
        closed = true;
        timer.shutdownNow();
    }
}
