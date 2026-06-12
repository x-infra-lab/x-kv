package io.github.xinfra.lab.xkv.kv.store;

import io.github.xinfra.lab.xkv.common.metrics.XKvMetrics;
import io.github.xinfra.lab.xkv.kv.raft.ProposalCodec;
import io.github.xinfra.lab.xkv.kv.raft.RegionPeer;
import io.github.xinfra.lab.xkv.proto.Kvrpcpb;
import io.github.xinfra.lab.xkv.proto.PDGrpc;
import io.github.xinfra.lab.xkv.proto.Pdpb;
import io.micrometer.core.instrument.Counter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Periodic GC scheduler.
 *
 * <p>Drives MVCC version reclaim: every {@code intervalMs} the worker
 *
 * <ol>
 *   <li>queries PD for the cluster-wide GC safe-point (intersected with
 *       all registered service safe-points already by the PD side);</li>
 *   <li>for every region this {@link Store} is leader of, proposes
 *       {@code MVCC_GC} with that safe-point through the region's Raft
 *       group.</li>
 * </ol>
 *
 * <p>Capped batch size ({@code MAX_GC_DELETES_PER_APPLY} on the apply
 * side) means a single round may not finish a region's full history; the
 * next tick picks up where it left off because the safe-point doesn't
 * regress and the apply-time cursor restarts at the lowest user-key. Over
 * a few rounds the working set is collapsed.
 *
 * <p>Stale (followed) regions are skipped — only a leader writes. If
 * leadership transfers mid-round, the proposal will be rejected at
 * {@code propose()} and the worker treats that as a normal "not leader"
 * outcome rather than an error.
 */
public final class GcWorker implements AutoCloseable {
    private static final Logger log = LoggerFactory.getLogger(GcWorker.class);

    private final Store store;
    private final PDGrpc.PDBlockingStub pd;
    private final long intervalMs;
    private final long proposeTimeoutMs;
    private final ScheduledExecutorService timer;
    private final AtomicLong lastObservedSafePoint = new AtomicLong(0);
    private final AtomicLong roundsTotal = new AtomicLong(0);
    private final AtomicLong proposalsTotal = new AtomicLong(0);
    private volatile boolean closed = false;
    private final Counter errorCounter = XKvMetrics.errorCounter("gc_worker", "tick");

    public GcWorker(Store store, PDGrpc.PDBlockingStub pd, long intervalMs, long proposeTimeoutMs) {
        this.store = store;
        this.pd = pd;
        this.intervalMs = intervalMs;
        this.proposeTimeoutMs = proposeTimeoutMs;
        this.timer = Executors.newSingleThreadScheduledExecutor(r -> {
            var t = new Thread(r, "gc-worker");
            t.setDaemon(true);
            return t;
        });
    }

    public void start() {
        timer.scheduleWithFixedDelay(this::tickSafely, intervalMs, intervalMs, TimeUnit.MILLISECONDS);
        log.info("GcWorker started: interval={}ms", intervalMs);
    }

    /** Diagnostic: most recent safe-point we tried to apply. */
    public long lastObservedSafePoint() { return lastObservedSafePoint.get(); }
    public long roundsTotal() { return roundsTotal.get(); }
    public long proposalsTotal() { return proposalsTotal.get(); }

    private void tickSafely() {
        if (closed) return;
        try { tick(); }
        catch (Throwable t) {
            log.warn("GcWorker tick failed: {}", t.getMessage());
        }
    }

    /**
     * Visible for testing — fire one round synchronously. Returns the
     * number of regions a proposal was sent to.
     */
    public int runOnce() {
        long sp = fetchSafePoint();
        if (sp <= 0) return 0;
        return runOnce(sp);
    }

    private int runOnce(long safePoint) {
        roundsTotal.incrementAndGet();
        lastObservedSafePoint.set(safePoint);
        int sent = 0;
        for (var peer : store.peers()) {
            if (peer.isDestroyed() || !peer.isLeader()) continue;
            try {
                var req = Kvrpcpb.GCRequest.newBuilder().setSafePoint(safePoint).build();
                var envelope = ProposalCodec.encode(
                        ProposalCodec.Kind.MVCC_GC, /* seq= */ 0, req.toByteArray());
                var fut = peer.propose(new RegionPeer.Proposal(envelope, 0, 0));
                var result = fut.get(proposeTimeoutMs, TimeUnit.MILLISECONDS);
                if (!result.success()) {
                    log.debug("GC propose region={} not-leader/error: {}",
                            peer.regionId(), result.errorMessage());
                } else {
                    sent++;
                    proposalsTotal.incrementAndGet();
                }
            } catch (Exception e) {
                errorCounter.increment();
                log.warn("GC propose region={} failed: {}", peer.regionId(), e.getMessage());
            }
        }
        return sent;
    }

    private void tick() { runOnce(); }

    private long fetchSafePoint() {
        try {
            var resp = pd.getGCSafePoint(Pdpb.GetGCSafePointRequest.newBuilder().build());
            return resp.getSafePoint();
        } catch (Throwable t) {
            errorCounter.increment();
            log.warn("GC safe-point fetch failed: {}", t.getMessage());
            return 0;
        }
    }

    @Override
    public void close() {
        closed = true;
        timer.shutdownNow();
    }
}
