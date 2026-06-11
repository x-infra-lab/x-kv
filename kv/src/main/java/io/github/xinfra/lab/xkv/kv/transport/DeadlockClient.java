package io.github.xinfra.lab.xkv.kv.transport;

import com.google.protobuf.ByteString;
import io.github.xinfra.lab.xkv.proto.Kvrpcpb;
import io.github.xinfra.lab.xkv.proto.PDGrpc;
import io.github.xinfra.lab.xkv.proto.Pdpb;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * KV-side facade for PD's distributed deadlock detector.
 *
 * <p>On a pessimistic-lock conflict the KV node has exactly enough local
 * information to draw ONE wait-for edge: {@code waiter_txn → holder_txn}
 * (with the key for diagnostics). This client ships that edge to PD and
 * synchronously checks whether inserting it would close a cycle in the
 * global wait-for graph.
 *
 * <h3>Why synchronous</h3>
 *
 * <p>The conflict is observed inline in {@code KvPessimisticLock} — the
 * client is ALREADY blocked waiting for the response, so spending an extra
 * sub-ms PD round-trip to learn "this is a deadlock; rollback now" is
 * strictly better than the 3-second TTL fallback. Production deadlock
 * detection (TiKV's) uses a streaming RPC for higher throughput; the
 * semantics are identical.
 *
 * <h3>Failure mode</h3>
 *
 * <p>If PD is unreachable we treat the request as "no cycle detected"
 * (returns {@link Result#NO_CYCLE}). The pessimistic-lock retry path will
 * naturally back off and the txn will eventually hit its own TTL — slow,
 * but the result is the same. Crashing or escalating on PD failure would
 * mistake a transient PD outage for a deadlock.
 */
public final class DeadlockClient implements AutoCloseable {
    private static final Logger log = LoggerFactory.getLogger(DeadlockClient.class);

    /** Per-call deadline. Small — PD detector is in-memory, sub-ms typical. */
    private static final long CALL_TIMEOUT_MS = 500;

    private final PDGrpc.PDBlockingStub pd;
    private final long clusterId;
    private final java.util.function.Consumer<Runnable> shutdownChannel;

    public DeadlockClient(PDGrpc.PDBlockingStub pd, long clusterId) {
        this(pd, clusterId, r -> r.run());
    }

    public DeadlockClient(PDGrpc.PDBlockingStub pd, long clusterId,
                          java.util.function.Consumer<Runnable> shutdownChannel) {
        this.pd = pd;
        this.clusterId = clusterId;
        this.shutdownChannel = shutdownChannel;
    }

    /** Outcome of a {@link #detect} call. */
    public record Result(boolean isDeadlock, long deadlockKeyHash,
                         List<Kvrpcpb.WaitForEntry> waitChain) {
        public static final Result NO_CYCLE = new Result(false, 0L, List.of());
    }

    /**
     * Insert a wait-for edge and check for cycle. Returns {@link Result#NO_CYCLE}
     * on PD unreachable / timeout — caller falls back to normal retry semantics.
     */
    public Result detect(long waiterTxn, long holderTxn, byte[] key) {
        if (waiterTxn == 0L || holderTxn == 0L) return Result.NO_CYCLE;
        if (waiterTxn == holderTxn) {
            // Self-loop. We can short-circuit without hitting PD, but going
            // through PD makes the test surface identical regardless of who
            // detects. Cheap enough — keep symmetric.
        }
        var req = Pdpb.DetectDeadlockRequest.newBuilder()
                .setHeader(Pdpb.RequestHeader.newBuilder().setClusterId(clusterId).build())
                .setEntry(Kvrpcpb.WaitForEntry.newBuilder()
                        .setTxn(waiterTxn)
                        .setWaitForTxn(holderTxn)
                        .setKey(key == null ? ByteString.EMPTY : ByteString.copyFrom(key))
                        .build())
                .build();
        try {
            var resp = pd.withDeadlineAfter(CALL_TIMEOUT_MS, TimeUnit.MILLISECONDS)
                    .detectDeadlock(req);
            if (resp.getWaitChainCount() == 0) return Result.NO_CYCLE;
            return new Result(true, resp.getDeadlockKeyHash(), resp.getWaitChainList());
        } catch (Throwable t) {
            log.warn("deadlock detect call failed ({}→{}): {}",
                    waiterTxn, holderTxn, t.getMessage());
            return Result.NO_CYCLE;
        }
    }

    /** Drop every edge held by {@code txn} — call after commit / rollback. */
    public void cleanupHolder(long txn) { cleanup(txn, Pdpb.CleanupWaitForRequest.CleanupMode.REMOVE_HOLDER); }

    /** Drop every edge waited on by {@code txn} — call when txn finally acquired or aborted. */
    public void cleanupWaiter(long txn) { cleanup(txn, Pdpb.CleanupWaitForRequest.CleanupMode.REMOVE_WAITER); }

    private void cleanup(long txn, Pdpb.CleanupWaitForRequest.CleanupMode mode) {
        if (txn == 0L) return;
        var req = Pdpb.CleanupWaitForRequest.newBuilder()
                .setHeader(Pdpb.RequestHeader.newBuilder().setClusterId(clusterId).build())
                .setTxn(txn)
                .setMode(mode)
                .build();
        try {
            pd.withDeadlineAfter(CALL_TIMEOUT_MS, TimeUnit.MILLISECONDS).cleanupWaitFor(req);
        } catch (Throwable t) {
            log.warn("deadlock cleanup({}, {}) failed: {}", txn, mode, t.getMessage());
        }
    }

    @Override
    public void close() {
        shutdownChannel.accept(() -> {});
    }
}
