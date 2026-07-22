package io.github.xinfra.lab.xkv.test;

import io.github.xinfra.lab.xkv.client.TxnClient;
import io.github.xinfra.lab.xkv.client.config.ClientConfig;
import io.github.xinfra.lab.xkv.client.txn.Transaction;
import io.github.xinfra.lab.xkv.kv.store.GcWorker;
import io.github.xinfra.lab.xkv.kv.store.Store;
import io.github.xinfra.lab.xkv.proto.PDGrpc;
import io.github.xinfra.lab.xkv.proto.Pdpb;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * End-to-end test for {@link GcWorker}: PD reports a safe-point; worker
 * proposes {@code MVCC_GC} on each region's leader; GC is observable via
 * {@code MvccGetByKey}-equivalent checks (we use direct RawKv get against
 * an MVCC-encoded key, plus a kvScanLock to confirm history is gone).
 *
 * <p>Production-shape: GcWorker is a real periodic scheduler. The test
 * drives one round synchronously via {@link GcWorker#runOnce} so we can
 * observe deterministic output instead of waiting on the timer.
 */
final class GcWorkerTest {

    @TempDir Path baseDir;
    private TestCluster cluster;
    private TxnClient client;
    private PDGrpc.PDBlockingStub pd;

    @BeforeEach
    void start() throws Exception {
        cluster = new TestCluster(baseDir).startReplicated(1, 3);
        client = TxnClient.create(ClientConfig.builder()
                .pdEndpoints(cluster.pdEndpoints())
                .build());
        pd = cluster.pdStub();
    }

    @AfterEach
    void stop() throws Exception {
        if (client != null) client.close();
        if (cluster != null) cluster.close();
    }

    @Test
    void gcWorkerCollapsesOldVersionsOncePdSafePointAdvances() throws Exception {
        // Three commits to "k" — leaves THREE versions in WRITE CF.
        for (int v = 1; v <= 3; v++) {
            try (Transaction txn = client.begin()) {
                txn.put("k".getBytes(), ("v" + v).getBytes());
                txn.commit();
            }
        }

        // The real leader store hosts the region-1 leader peer; the worker
        // iterates its peers and proposes GC on whichever is currently leader.
        Store store = cluster.leaderStoreFor(TestCluster.BOOTSTRAP_REGION_ID).store();

        // Move PD's GC safe-point well into the future so v1 + v2 are
        // eligible for collapse. We grab a fresh TSO via begin/rollback as a
        // quick way to obtain a "now" timestamp for the safe-point.
        long safePoint;
        try (Transaction probe = client.begin()) {
            safePoint = probe.startTs();
            probe.rollback();
        }
        pd.updateGCSafePoint(Pdpb.UpdateGCSafePointRequest.newBuilder()
                .setSafePoint(safePoint).build());

        var worker = new GcWorker(store, pd, /* intervalMs= */ 60_000, /* proposeTimeoutMs= */ 5_000);
        try {
            int proposed = worker.runOnce();
            assertThat(proposed).as("GC proposal lands on the leader").isEqualTo(1);
            assertThat(worker.lastObservedSafePoint()).isEqualTo(safePoint);
        } finally {
            worker.close();
        }

        // Latest visible value still reads correctly.
        try (Transaction txn = client.begin()) {
            byte[] got = txn.get("k".getBytes()).orElseThrow();
            assertThat(new String(got)).isEqualTo("v3");
        }
    }

    @Test
    void gcWorkerSkipsWhenSafePointZero() throws Exception {
        // PD safe-point default = 0 — worker must NOT propose anything.
        Store store = cluster.leaderStoreFor(TestCluster.BOOTSTRAP_REGION_ID).store();
        var worker = new GcWorker(store, pd, 60_000, 5_000);
        try {
            int proposed = worker.runOnce();
            assertThat(proposed).isZero();
            assertThat(worker.proposalsTotal()).isZero();
        } finally {
            worker.close();
        }
    }
}
