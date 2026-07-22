package io.github.xinfra.lab.xkv.test;

import io.github.xinfra.lab.xkv.client.TxnClient;
import io.github.xinfra.lab.xkv.client.raw.RawKvClient;
import io.github.xinfra.lab.xkv.client.txn.Transaction;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Black-box cluster lifecycle E2E test.
 *
 * <p>Everything runs against the <em>production</em> {@link TestCluster}
 * harness, which launches real {@code PdServer} / {@code KvServer} entrypoints
 * and observes the cluster only through the client SDK ({@link RawKvClient},
 * {@link TxnClient}) and PD's public gRPC API. There is no reaching into raft
 * internals, region peers, or in-process drivers.
 *
 * <p>Covers the four cluster-lifecycle capabilities:
 * <ol>
 *   <li>Cluster init with a controllable PD node count (3-node raft PD)
 *       and KV store count (start with 1).</li>
 *   <li>Store scale-out: add stores and verify PD replicates the region.</li>
 *   <li>Store scale-in: gracefully take a store offline; data stays served.</li>
 *   <li>PD scale-out / scale-in: add and remove PD members; the cluster
 *       keeps serving reads and writes across the membership changes.</li>
 * </ol>
 *
 * <p>Region split/merge are covered by their own dedicated E2E tests
 * ({@code AutoSplitE2ETest}, {@code MergeDriverE2ETest}); this test focuses on
 * cluster + PD membership lifecycle and data correctness through transitions.
 */
@Timeout(value = 300, unit = TimeUnit.SECONDS)
final class ClusterLifecycleE2ETest {

    private static final Logger log = LoggerFactory.getLogger(ClusterLifecycleE2ETest.class);

    @TempDir Path baseDir;
    private TestCluster cluster;

    @AfterEach
    void teardown() {
        if (cluster != null) cluster.close();
    }

    @Test
    void fullClusterLifecycle() throws Exception {
        // ====================================================================
        // Phase 1: Cluster init — 3-node raft PD + 1 KV store
        // ====================================================================
        log.info("=== Phase 1: Cluster init (3 PD, 1 store) ===");
        cluster = new TestCluster(baseDir).start(3, 1);
        long regionId = TestCluster.BOOTSTRAP_REGION_ID;

        assertThat(cluster.storeUpCount()).isEqualTo(1);

        RawKvClient raw = cluster.newRawKvClient();
        TxnClient txnClient = cluster.newTxnClient();

        // Raw CRUD on the single-store cluster.
        raw.put(bytes("init-key"), bytes("init-val"));
        assertThat(str(raw.get(bytes("init-key")))).isEqualTo("init-val");

        // Txn CRUD.
        try (Transaction txn = txnClient.begin()) {
            for (int i = 0; i < 10; i++) {
                txn.put(bytes("txn-key-" + i), bytes("txn-val-" + i));
            }
            txn.commit();
        }
        try (Transaction txn = txnClient.begin()) {
            for (int i = 0; i < 10; i++) {
                assertThat(str(txn.get(bytes("txn-key-" + i)))).isEqualTo("txn-val-" + i);
            }
            txn.rollback();
        }
        log.info("Phase 1 complete: single-store raw + txn CRUD verified");

        // ====================================================================
        // Phase 2: Store scale-out — grow 1 -> 3, PD replicates the region
        // ====================================================================
        log.info("=== Phase 2: Store scale-out (1 -> 3) ===");
        cluster.addStore();     // store 2
        cluster.addStore();     // store 3
        cluster.awaitStoreUpCount(3);
        // Assert the bootstrap region replicates onto a newly-added store
        // (>= 2 replicas). This proves PD scheduling + the KvServer
        // conf-change transport wiring actually place a live replica on a
        // freshly-joined store. (awaitRegionReplicas uses a >= comparison.)
        cluster.awaitRegionReplicas(regionId, 2);
        log.info("region {} replicated onto a newly-added store", regionId);

        // Data written pre-scale-out survives and is still served.
        assertThat(str(raw.get(bytes("init-key")))).isEqualTo("init-val");

        // Bulk raw CRUD across the replicated cluster.
        int n = 50;
        for (int i = 0; i < n; i++) {
            raw.put(bytes("raw-" + pad(i)), bytes("value-" + i));
        }
        for (int i = 0; i < n; i++) {
            assertThat(str(raw.get(bytes("raw-" + pad(i))))).isEqualTo("value-" + i);
        }
        for (int i = 1; i < n; i += 2) {
            raw.delete(bytes("raw-" + pad(i)));
        }
        for (int i = 0; i < n; i++) {
            Optional<byte[]> v = raw.get(bytes("raw-" + pad(i)));
            if (i % 2 == 1) {
                assertThat(v).as("deleted raw key %d", i).isEmpty();
            } else {
                assertThat(str(v)).as("even raw key %d", i).isEqualTo("value-" + i);
            }
        }
        log.info("Phase 2 complete: replicated cluster raw CRUD verified");

        // ====================================================================
        // Phase 3: Store scale-in — take a store offline, data stays served
        // ====================================================================
        log.info("=== Phase 3: Store scale-in (drain store 3) ===");
        cluster.removeStore(3);
        cluster.awaitStoreUpCount(2);

        // Reads of pre-existing keys still succeed with the surviving replicas.
        assertThat(str(raw.get(bytes("init-key")))).isEqualTo("init-val");
        assertThat(str(raw.get(bytes("raw-" + pad(0))))).isEqualTo("value-0");

        // New writes + reads still work on the 2-store cluster.
        raw.put(bytes("after-drain"), bytes("drain-val"));
        assertThat(str(raw.get(bytes("after-drain")))).isEqualTo("drain-val");

        try (Transaction txn = txnClient.begin()) {
            for (int i = 0; i < 10; i++) {
                assertThat(str(txn.get(bytes("txn-key-" + i)))).isEqualTo("txn-val-" + i);
            }
            txn.rollback();
        }
        log.info("Phase 3 complete: cluster serves after store offline");

        // ====================================================================
        // Phase 4: PD scale-out — grow PD raft group 3 -> 4
        // ====================================================================
        log.info("=== Phase 4: PD scale-out (3 -> 4) ===");
        cluster.addPd();
        cluster.awaitPdMembers(4);

        // Client keeps serving across the PD membership change.
        raw.put(bytes("after-pd-add"), bytes("pd-add-val"));
        assertThat(str(raw.get(bytes("after-pd-add")))).isEqualTo("pd-add-val");
        try (Transaction txn = txnClient.begin()) {
            txn.put(bytes("txn-after-pd-add"), bytes("v"));
            txn.commit();
        }
        log.info("Phase 4 complete: cluster serves after PD scale-out");

        // ====================================================================
        // Phase 5: PD scale-in — remove a PD follower, 4 -> 3
        // ====================================================================
        log.info("=== Phase 5: PD scale-in (4 -> 3) ===");
        long follower = cluster.anyPdFollowerId();
        cluster.removePd(follower);
        cluster.awaitPdMembers(3);

        // Client still serves reads/writes after the PD member left.
        assertThat(str(raw.get(bytes("after-pd-add")))).isEqualTo("pd-add-val");
        raw.put(bytes("after-pd-remove"), bytes("pd-remove-val"));
        assertThat(str(raw.get(bytes("after-pd-remove")))).isEqualTo("pd-remove-val");
        try (Transaction txn = txnClient.begin()) {
            assertThat(str(txn.get(bytes("txn-key-0")))).isEqualTo("txn-val-0");
            assertThat(str(txn.get(bytes("txn-after-pd-add")))).isEqualTo("v");
            txn.rollback();
        }
        log.info("Phase 5 complete: cluster serves after PD scale-in");

        log.info("=== Full black-box cluster lifecycle test PASSED ===");
    }

    // ---- helpers ----

    private static byte[] bytes(String s) {
        return s.getBytes(StandardCharsets.UTF_8);
    }

    private static String str(Optional<byte[]> v) {
        assertThat(v).isPresent();
        return new String(v.get(), StandardCharsets.UTF_8);
    }

    private static String pad(int i) {
        return String.format("%04d", i);
    }
}
