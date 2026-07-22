package io.github.xinfra.lab.xkv.test;

import io.github.xinfra.lab.xkv.client.TxnClient;
import io.github.xinfra.lab.xkv.client.config.ClientConfig;
import io.github.xinfra.lab.xkv.client.txn.Transaction;
import io.github.xinfra.lab.xkv.kv.raft.RegionPeer;
import io.github.xinfra.lab.xkv.kv.store.LogCompactionWorker;
import io.github.xinfra.lab.xkv.kv.store.Store;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * End-to-end test for {@link LogCompactionWorker}: after enough writes
 * push {@code applied_index} past the gap threshold, one round of the
 * worker proposes ADMIN_COMPACT_LOG and {@code first_index} advances.
 *
 * <p>Without log compaction the raft log accumulates forever — exactly
 * the v1 gap this worker closes.
 */
final class LogCompactionWorkerTest {

    @TempDir Path baseDir;
    private TestCluster cluster;
    private TxnClient client;

    @BeforeEach
    void start() throws Exception {
        cluster = new TestCluster(baseDir).startReplicated(1, 3);
        client = TxnClient.create(ClientConfig.builder()
                .pdEndpoints(cluster.pdEndpoints())
                .build());
    }

    @AfterEach
    void stop() throws Exception {
        if (client != null) client.close();
        if (cluster != null) cluster.close();
    }

    @Test
    void compactionAdvancesFirstIndexOnceAppliedGapExceedsThreshold() {
        var store = leaderStore();

        // Push the raft log past the gap threshold. Each commit = 2 raft
        // entries (prewrite + commit), so ~50 commits ⇒ ~100 applied
        // entries. Choose gapThreshold=20 / safetyMargin=5 so even with
        // some setup entries on top we're comfortably over.
        for (int i = 0; i < 50; i++) {
            try (Transaction txn = client.begin()) {
                txn.put(("k" + i).getBytes(), ("v" + i).getBytes());
                txn.commit();
            }
        }

        long firstBefore = leaderPeer().firstIndex();
        long applied = leaderPeer().appliedIndex();
        assertThat(applied - firstBefore)
                .as("workload should drive applied well past first")
                .isGreaterThan(20);

        var worker = new LogCompactionWorker(store, /* intervalMs= */ 60_000,
                /* gapThreshold= */ 20, /* safetyMargin= */ 5,
                /* proposeTimeoutMs= */ 5_000);
        try {
            int issued = worker.runOnce();
            assertThat(issued).as("compaction proposal landed on the leader").isEqualTo(1);
        } finally {
            worker.close();
        }

        // Wait for the compact entry to apply on the leader (it propagates
        // through raft like any other proposal).
        Awaitility.await().atMost(Duration.ofSeconds(5))
                .pollInterval(Duration.ofMillis(50))
                .until(() -> leaderPeer().firstIndex() > firstBefore);

        long firstAfter = leaderPeer().firstIndex();
        long appliedAfter = leaderPeer().appliedIndex();
        assertThat(firstAfter).isGreaterThan(firstBefore);
        // Safety margin honored: first_index <= applied - safetyMargin + 1.
        assertThat(firstAfter).isLessThanOrEqualTo(appliedAfter - 5 + 1);
    }

    @Test
    void compactionNoOpWhenGapBelowThreshold() {
        // No workload pushed through — gap is tiny.
        var store = leaderStore();
        var worker = new LogCompactionWorker(store, 60_000, 10_000, 100, 5_000);
        try {
            int issued = worker.runOnce();
            assertThat(issued).isZero();
            assertThat(worker.compactionsTotal()).isZero();
        } finally {
            worker.close();
        }
    }

    // ===== helpers =====

    private Store leaderStore() {
        return cluster.leaderStoreFor(TestCluster.BOOTSTRAP_REGION_ID).store();
    }

    private RegionPeer leaderPeer() {
        var s = cluster.leaderStoreFor(TestCluster.BOOTSTRAP_REGION_ID);
        return cluster.realPeer(s.storeId, TestCluster.BOOTSTRAP_REGION_ID);
    }
}
