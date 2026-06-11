package io.github.xinfra.lab.xkv.kv.store;

import io.github.xinfra.lab.xkv.common.metrics.XKvMetrics;
import io.github.xinfra.lab.xkv.kv.raft.AdminApplyHandler;
import io.github.xinfra.lab.xkv.kv.raft.ProposalCodec;
import io.github.xinfra.lab.xkv.kv.raft.RegionPeer;
import io.micrometer.core.instrument.Counter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Periodic raft-log compaction scheduler.
 *
 * <p>Each tick scans this {@link Store}'s leader peers. For each, if
 * {@code applied_index - first_index > gapThreshold} the worker proposes
 * an {@code ADMIN_COMPACT_LOG} that advances {@code first_index} to
 * {@code applied_index - safetyMargin}. The safety margin is kept so a
 * follower that is at most {@code safetyMargin} entries behind can still
 * catch up by streaming log entries, instead of needing a full snapshot
 * (which we do not yet stream — see {@code KvRaftServiceImpl.snapshot}).
 *
 * <p>Without this worker, the {@code RAFT} CF accumulates entries
 * indefinitely: a long-running cluster eventually exhausts disk on
 * <em>committed-and-applied</em> raft entries that are no longer needed
 * for any purpose. This was a v1 footgun.
 *
 * <p>Tunables:
 * <ul>
 *   <li>{@code intervalMs} — how often the scheduler ticks.</li>
 *   <li>{@code gapThreshold} — minimum applied-vs-first gap before a
 *       compaction is proposed. Smaller = more compactions, less log
 *       overhead per region. Default 10 000.</li>
 *   <li>{@code safetyMargin} — entries to retain past first_index.
 *       Should comfortably exceed the worst-case follower lag.
 *       Default 1 000.</li>
 * </ul>
 *
 * <p>Compactions are only proposed on the leader peer. Followers apply
 * the compaction transparently when the entry replicates to them.
 */
public final class LogCompactionWorker implements AutoCloseable {
    private static final Logger log = LoggerFactory.getLogger(LogCompactionWorker.class);

    private final Store store;
    private final long intervalMs;
    private final long gapThreshold;
    private final long safetyMargin;
    private final long proposeTimeoutMs;
    private final ScheduledExecutorService timer;
    private final AtomicLong roundsTotal = new AtomicLong(0);
    private final AtomicLong compactionsTotal = new AtomicLong(0);
    private volatile boolean closed = false;
    private final Counter errorCounter = XKvMetrics.errorCounter("log_compaction", "compact");

    public LogCompactionWorker(Store store, long intervalMs,
                                long gapThreshold, long safetyMargin,
                                long proposeTimeoutMs) {
        if (gapThreshold <= safetyMargin) {
            throw new IllegalArgumentException(
                    "gapThreshold (" + gapThreshold + ") must be > safetyMargin (" + safetyMargin + ")");
        }
        this.store = store;
        this.intervalMs = intervalMs;
        this.gapThreshold = gapThreshold;
        this.safetyMargin = safetyMargin;
        this.proposeTimeoutMs = proposeTimeoutMs;
        this.timer = Executors.newSingleThreadScheduledExecutor(r -> {
            var t = new Thread(r, "log-compaction-worker");
            t.setDaemon(true);
            return t;
        });
    }

    public void start() {
        timer.scheduleAtFixedRate(this::tickSafely, intervalMs, intervalMs, TimeUnit.MILLISECONDS);
        log.info("LogCompactionWorker started: interval={}ms gapThreshold={} safetyMargin={}",
                intervalMs, gapThreshold, safetyMargin);
    }

    public long roundsTotal() { return roundsTotal.get(); }
    public long compactionsTotal() { return compactionsTotal.get(); }

    private void tickSafely() {
        if (closed) return;
        try { runOnce(); }
        catch (Throwable t) {
            log.warn("LogCompactionWorker tick failed: {}", t.getMessage());
        }
    }

    /** Visible for testing. Returns number of compaction proposals issued this round. */
    public int runOnce() {
        roundsTotal.incrementAndGet();
        int issued = 0;
        for (var peer : store.peers()) {
            if (peer.isDestroyed() || !peer.isLeader()) continue;
            long first = peer.firstIndex();
            long applied = peer.appliedIndex();
            if (applied - first < gapThreshold) continue;
            long target = applied - safetyMargin;
            if (target < first) continue;     // already small enough — skip
            try {
                // Stage a snapshot at applied_index BEFORE compacting log,
                // so a fall-behind follower whose nextIndex < first_index
                // post-compact still has something to catch up from.
                peer.maybeGenerateSnapshot();

                var payload = AdminApplyHandler.encodeCompactLog(target);
                var envelope = ProposalCodec.encode(
                        ProposalCodec.Kind.ADMIN_COMPACT_LOG, /* seq= */ 0, payload);
                var fut = peer.propose(new RegionPeer.Proposal(envelope, 0, 0));
                var result = fut.get(proposeTimeoutMs, TimeUnit.MILLISECONDS);
                if (result.success()) {
                    issued++;
                    compactionsTotal.incrementAndGet();
                } else {
                    log.debug("compact propose region={} rejected: {}",
                            peer.regionId(), result.errorMessage());
                }
            } catch (Exception e) {
                errorCounter.increment();
                log.warn("compact propose region={} failed: {}", peer.regionId(), e.getMessage());
            }
        }
        return issued;
    }

    @Override
    public void close() {
        closed = true;
        timer.shutdownNow();
    }
}
