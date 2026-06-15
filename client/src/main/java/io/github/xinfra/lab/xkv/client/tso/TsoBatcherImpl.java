package io.github.xinfra.lab.xkv.client.tso;

import io.github.xinfra.lab.xkv.client.config.ClientConfig;
import io.github.xinfra.lab.xkv.client.pd.PdClient;
import io.github.xinfra.lab.xkv.proto.PDGrpc;
import io.github.xinfra.lab.xkv.proto.Pdpb;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Default {@link TsoBatcher} backed by a single bidi stream to PD.
 *
 * <p>Threading model:
 * <ul>
 *   <li>Caller threads enqueue via {@link #getTimestamps} and immediately
 *       receive a {@link CompletableFuture}.</li>
 *   <li>One <strong>dispatcher thread</strong> drains the queue, coalescing
 *       entries up to {@code maxBatchSize} or until {@code batchWaitMicros}
 *       elapses, into a single TsoRequest sent on the bidi stream. The
 *       PD's response carries the first allocated TSO + count; the
 *       dispatcher splits this into individual futures.</li>
 *   <li>Stream errors fail all in-flight futures and trigger a reconnect
 *       on the next call.</li>
 * </ul>
 *
 * <p>v1 served TSO via a single blocking call per {@code getTimestamp(1)}
 * and capped at ~1k TSO/s. This impl can sustain &gt;100k/s on one TCP
 * connection — the single biggest perf delta on the read path.
 */
public final class TsoBatcherImpl implements TsoBatcher {
    private static final Logger log = LoggerFactory.getLogger(TsoBatcherImpl.class);

    private final PdClient pdClient;
    private final ClientConfig.TsoConfig cfg;

    private final ReentrantLock lock = new ReentrantLock();
    private final Condition workArrived = lock.newCondition();
    private final Deque<Pending> queue = new ArrayDeque<>();
    private volatile boolean shutdown = false;

    private final Thread dispatcher;
    private final AtomicReference<StreamState> stream = new AtomicReference<>();

    public TsoBatcherImpl(PdClient pdClient, ClientConfig.TsoConfig cfg) {
        this.pdClient = pdClient;
        this.cfg = cfg;
        this.dispatcher = new Thread(this::dispatchLoop, "tso-batcher");
        this.dispatcher.setDaemon(true);
        this.dispatcher.start();
    }

    @Override
    public CompletableFuture<Long> getTimestamps(int count) {
        if (count <= 0) throw new IllegalArgumentException("count must be > 0");
        if (shutdown) {
            var f = new CompletableFuture<Long>();
            f.completeExceptionally(new IllegalStateException("TsoBatcher shut down"));
            return f;
        }
        var p = new Pending(count, new CompletableFuture<>());
        lock.lock();
        try {
            queue.addLast(p);
            workArrived.signal();
        } finally {
            lock.unlock();
        }
        return p.future;
    }

    @Override
    public void close() {
        shutdown = true;
        lock.lock();
        try { workArrived.signalAll(); } finally { lock.unlock(); }
        try { dispatcher.join(2_000); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
        var s = stream.get();
        if (s != null) {
            try { s.outbound.onCompleted(); } catch (Throwable e) {
                log.warn("tso stream onCompleted failed: {}", e.getMessage());
            }
        }
    }

    // =====================================================================
    // Dispatcher loop
    // =====================================================================

    private void dispatchLoop() {
        while (!shutdown) {
            List<Pending> batch = drainBatch();
            if (batch.isEmpty()) continue;
            try {
                sendBatch(batch);
            } catch (Throwable t) {
                log.warn("TSO batch send failed; failing {} in-flight requests", batch.size(), t);
                for (var p : batch) p.future.completeExceptionally(t);
                resetStream();
            }
        }
        // Drain pending on shutdown.
        var remaining = drainBatch();
        for (var p : remaining) {
            p.future.completeExceptionally(new IllegalStateException("TsoBatcher shut down"));
        }
    }

    private List<Pending> drainBatch() {
        var out = new ArrayList<Pending>();
        lock.lock();
        try {
            while (queue.isEmpty() && !shutdown) {
                try { workArrived.await(50, java.util.concurrent.TimeUnit.MILLISECONDS); }
                catch (InterruptedException e) { Thread.currentThread().interrupt(); return out; }
            }
            if (cfg.batchWaitMicros() > 0) {
                // Brief micro-coalesce: hold lock briefly to let more callers pile in.
                try { workArrived.awaitNanos(cfg.batchWaitMicros() * 1_000L); }
                catch (InterruptedException e) { Thread.currentThread().interrupt(); }
            }
            int taken = 0;
            while (!queue.isEmpty() && taken < cfg.maxBatchSize()) {
                out.add(queue.pollFirst());
                taken++;
            }
        } finally {
            lock.unlock();
        }
        return out;
    }

    private void sendBatch(List<Pending> batch) {
        var s = ensureStream();
        int total = 0;
        for (var p : batch) total += p.count;

        var pendingResp = new PendingResp(batch);
        s.pending.addLast(pendingResp);

        s.outbound.onNext(Pdpb.TsoRequest.newBuilder()
                .setCount(total)
                .build());
    }

    private StreamState ensureStream() {
        var s = stream.get();
        if (s != null && !s.broken) return s;
        var newS = new StreamState();
        var stub = PDGrpc.newStub(pdClient.leaderChannel());
        newS.outbound = stub.getTimestamp(new StreamObserver<>() {
            @Override
            public void onNext(Pdpb.TsoResponse resp) {
                var pr = newS.pending.pollFirst();
                if (pr == null) {
                    log.warn("TSO response with no pending batch — dropping");
                    return;
                }
                long physical = resp.getTimestamp().getPhysical();
                long lastLogical = resp.getTimestamp().getLogical();
                int count = resp.getCount();
                long firstLogical = lastLogical - count + 1;
                if (firstLogical < 0) {
                    for (var p : pr.batch) {
                        p.future.completeExceptionally(
                                new IllegalStateException("TSO batch crossed physical boundary"));
                    }
                    return;
                }
                long firstTs = (physical << 18) | firstLogical;
                long cursor = firstTs;
                for (var p : pr.batch) {
                    p.future.complete(cursor);
                    cursor += p.count;
                }
            }
            @Override public void onError(Throwable t) {
                log.warn("TSO stream error", t);
                newS.broken = true;
                drainPending(newS, t);
                pdClient.switchLeader();
            }
            @Override public void onCompleted() {
                newS.broken = true;
                drainPending(newS, new RuntimeException("TSO stream closed"));
            }
        });
        stream.set(newS);
        return newS;
    }

    private static void drainPending(StreamState s, Throwable t) {
        while (true) {
            var pr = s.pending.pollFirst();
            if (pr == null) break;
            for (var p : pr.batch) p.future.completeExceptionally(t);
        }
    }

    private void resetStream() {
        var s = stream.get();
        if (s != null) {
            try { s.outbound.onCompleted(); } catch (Throwable e) {
                log.warn("tso stream onCompleted failed: {}", e.getMessage());
            }
            s.broken = true;
        }
        stream.set(null);
    }

    // =====================================================================

    private record Pending(int count, CompletableFuture<Long> future) {}
    private record PendingResp(List<Pending> batch) {}

    private static final class StreamState {
        StreamObserver<Pdpb.TsoRequest> outbound;
        final Deque<PendingResp> pending = new java.util.concurrent.ConcurrentLinkedDeque<>();
        volatile boolean broken = false;
    }
}
