package io.github.xinfra.lab.xkv.test;

import com.google.protobuf.ByteString;
import io.github.xinfra.lab.xkv.proto.Kvrpcpb;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * End-to-end distributed deadlock detection.
 *
 * <p>Two pessimistic transactions form a wait-for cycle:
 * <pre>
 *   txn A (startTs=100)            txn B (startTs=200)
 *   ──────────────────             ──────────────────
 *   pessimistic-lock(k1)  → OK
 *                                  pessimistic-lock(k2)  → OK
 *   pessimistic-lock(k2)  → blocked on B (edge A→B)
 *                                  pessimistic-lock(k1)  → BLOCKED on A
 *                                                          edge B→A closes
 *                                                          the cycle → PD
 *                                                          returns wait_chain
 *                                                          → KV translates
 *                                                          KeyError.locked into
 *                                                          KeyError.deadlock.
 * </pre>
 *
 * <p>The second pessimistic-lock RPC must surface {@code KeyError.deadlock}
 * with a non-empty {@code wait_chain} — otherwise the only recourse for the
 * deadlocked txn is the lock-TTL timer (3 s+), which is exactly the
 * production gap the detector closes.
 */
final class DistributedDeadlockTest {

    @TempDir Path baseDir;
    private ClusterHarness harness;

    @BeforeEach
    void start() throws Exception {
        harness = new ClusterHarness(baseDir, 3).start();
    }

    @AfterEach
    void stop() throws Exception {
        if (harness != null) harness.close();
    }

    @Test
    void twoTxnCycleSurfacesAsKeyErrorDeadlock() {
        var tikv = harness.leader().blockingStub();
        ByteString k1 = ByteString.copyFromUtf8("dl-k1");
        ByteString k2 = ByteString.copyFromUtf8("dl-k2");

        long aStart = 100L;
        long bStart = 200L;

        // 1) Txn A locks k1 (k1 is also A's primary).
        var aLock1 = tikv.kvPessimisticLock(Kvrpcpb.PessimisticLockRequest.newBuilder()
                .setStartVersion(aStart).setForUpdateTs(aStart).setLockTtl(3000)
                .setPrimaryLock(k1)
                .addMutations(Kvrpcpb.Mutation.newBuilder()
                        .setOp(Kvrpcpb.Op.PessimisticLock).setKey(k1))
                .build());
        assertThat(aLock1.getErrorsCount()).as("A locks k1 cleanly").isZero();

        // 2) Txn B locks k2 (k2 is B's primary).
        var bLock2 = tikv.kvPessimisticLock(Kvrpcpb.PessimisticLockRequest.newBuilder()
                .setStartVersion(bStart).setForUpdateTs(bStart).setLockTtl(3000)
                .setPrimaryLock(k2)
                .addMutations(Kvrpcpb.Mutation.newBuilder()
                        .setOp(Kvrpcpb.Op.PessimisticLock).setKey(k2))
                .build());
        assertThat(bLock2.getErrorsCount()).as("B locks k2 cleanly").isZero();

        // 3) A tries k2 — blocked on B. KeyError.locked, edge A→B in PD.
        var aLock2 = tikv.kvPessimisticLock(Kvrpcpb.PessimisticLockRequest.newBuilder()
                .setStartVersion(aStart).setForUpdateTs(aStart).setLockTtl(3000)
                .setPrimaryLock(k1)
                .addMutations(Kvrpcpb.Mutation.newBuilder()
                        .setOp(Kvrpcpb.Op.PessimisticLock).setKey(k2))
                .build());
        assertThat(aLock2.getErrorsCount()).isEqualTo(1);
        var err = aLock2.getErrors(0);
        assertThat(err.hasLocked()).as("A→B is the first edge; no cycle yet").isTrue();
        assertThat(err.getLocked().getLockVersion()).isEqualTo(bStart);

        // 4) B tries k1 — closes the cycle. Must come back as KeyError.deadlock.
        var bLock1 = tikv.kvPessimisticLock(Kvrpcpb.PessimisticLockRequest.newBuilder()
                .setStartVersion(bStart).setForUpdateTs(bStart).setLockTtl(3000)
                .setPrimaryLock(k2)
                .addMutations(Kvrpcpb.Mutation.newBuilder()
                        .setOp(Kvrpcpb.Op.PessimisticLock).setKey(k1))
                .build());
        assertThat(bLock1.getErrorsCount()).isEqualTo(1);
        var dlErr = bLock1.getErrors(0);
        assertThat(dlErr.hasDeadlock()).as("B's request closes the cycle").isTrue();
        var dl = dlErr.getDeadlock();
        assertThat(dl.getLockTs()).isEqualTo(aStart);
        assertThat(dl.getLockKey()).isEqualTo(k1);
        assertThat(dl.getDeadlockKeyHash()).isNotZero();
        assertThat(dl.getWaitChainCount()).as("wait chain is non-empty").isPositive();
    }

    @Test
    void noCycleNoDeadlockSurface() {
        var tikv = harness.leader().blockingStub();
        ByteString k1 = ByteString.copyFromUtf8("nodl-k1");
        ByteString k2 = ByteString.copyFromUtf8("nodl-k2");
        long aStart = 300L, bStart = 400L;

        var aLock = tikv.kvPessimisticLock(Kvrpcpb.PessimisticLockRequest.newBuilder()
                .setStartVersion(aStart).setForUpdateTs(aStart).setLockTtl(3000)
                .setPrimaryLock(k1)
                .addMutations(Kvrpcpb.Mutation.newBuilder()
                        .setOp(Kvrpcpb.Op.PessimisticLock).setKey(k1))
                .build());
        assertThat(aLock.getErrorsCount()).isZero();

        // B waits on A but never builds a cycle.
        var bLock = tikv.kvPessimisticLock(Kvrpcpb.PessimisticLockRequest.newBuilder()
                .setStartVersion(bStart).setForUpdateTs(bStart).setLockTtl(3000)
                .setPrimaryLock(k2)
                .addMutations(Kvrpcpb.Mutation.newBuilder()
                        .setOp(Kvrpcpb.Op.PessimisticLock).setKey(k1))
                .build());
        assertThat(bLock.getErrorsCount()).isEqualTo(1);
        assertThat(bLock.getErrors(0).hasLocked())
                .as("plain conflict, not a cycle → KeyError.locked")
                .isTrue();
        assertThat(bLock.getErrors(0).hasDeadlock()).isFalse();

        // PD-side: only one edge ever inserted.
        assertThat(harness.pdServer().deadlockDetector().edgeCount()).isEqualTo(1);
    }

    @Test
    void commitClearsHolderEdges() throws Exception {
        var tikv = harness.leader().blockingStub();
        ByteString k = ByteString.copyFromUtf8("clr-k");
        long aStart = 500L, bStart = 600L;

        // A locks k, B queues behind it.
        tikv.kvPessimisticLock(Kvrpcpb.PessimisticLockRequest.newBuilder()
                .setStartVersion(aStart).setForUpdateTs(aStart).setLockTtl(3000)
                .setPrimaryLock(k)
                .addMutations(Kvrpcpb.Mutation.newBuilder()
                        .setOp(Kvrpcpb.Op.PessimisticLock).setKey(k))
                .build());
        var bWait = tikv.kvPessimisticLock(Kvrpcpb.PessimisticLockRequest.newBuilder()
                .setStartVersion(bStart).setForUpdateTs(bStart).setLockTtl(3000)
                .setPrimaryLock(k)
                .addMutations(Kvrpcpb.Mutation.newBuilder()
                        .setOp(Kvrpcpb.Op.PessimisticLock).setKey(k))
                .build());
        assertThat(bWait.getErrors(0).hasLocked()).isTrue();
        assertThat(harness.pdServer().deadlockDetector().edgeCount()).isEqualTo(1);

        // A rolls back — the pessimistic lock disappears AND the wait-for
        // edge for A-as-holder must clear so B isn't left "waiting" forever
        // in the graph.
        var rb = tikv.kvBatchRollback(Kvrpcpb.BatchRollbackRequest.newBuilder()
                .setStartVersion(aStart).addKeys(k).build());
        assertThat(rb.hasError()).isFalse();

        // Cleanup is fire-and-forget but synchronous on the response thread —
        // by the time kvBatchRollback returns the RPC has executed.
        assertThat(harness.pdServer().deadlockDetector().edgeCount())
                .as("rollback should have dropped B→A edge")
                .isZero();
    }
}
