package io.github.xinfra.lab.xkv.test;

import io.github.xinfra.lab.xkv.client.TxnClient;
import io.github.xinfra.lab.xkv.client.config.ClientConfig;
import io.github.xinfra.lab.xkv.client.txn.Transaction;
import io.github.xinfra.lab.xkv.kv.raft.RegionPeer;
import io.github.xinfra.lab.xkv.kv.store.LogCompactionWorker;
import io.github.xinfra.lab.xkv.kv.store.Store;
import io.github.xinfra.lab.xkv.proto.Metapb;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.time.Duration;
import java.util.List;

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
    private ClusterHarness harness;
    private TxnClient client;

    @BeforeEach
    void start() throws Exception {
        harness = new ClusterHarness(baseDir, 3).start();
        client = TxnClient.create(ClientConfig.builder()
                .pdEndpoints(List.of("127.0.0.1:" + harness.pdPort()))
                .build());
    }

    @AfterEach
    void stop() throws Exception {
        if (client != null) client.close();
        if (harness != null) harness.close();
    }

    @Test
    void compactionAdvancesFirstIndexOnceAppliedGapExceedsThreshold() {
        var store = wrapAllAsStore();

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
        var store = wrapAllAsStore();
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

    private RegionPeer leaderPeer() {
        return harness.leader().peer;
    }

    private Store wrapAllAsStore() {
        var nodes = harness.kvNodes();
        return new Store() {
            @Override public java.util.Optional<RegionPeer> peerForRegion(long regionId) {
                return nodes.stream().map(n -> (RegionPeer) n.peer)
                        .filter(p -> p.regionId() == regionId).findFirst();
            }
            @Override public java.util.Optional<RegionPeer> peerForKey(byte[] key) {
                return java.util.Optional.empty();
            }
            @Override public java.util.Collection<RegionPeer> peers() {
                return nodes.stream().map(n -> (RegionPeer) n.peer).toList();
            }
            @Override public void registerPeer(RegionPeer peer) {}
            @Override public void destroyPeer(long regionId) {}
            @Override public long storeId() { return 0L; }
            @Override public Metapb.Store metadata() {
                return Metapb.Store.newBuilder().setId(0L).build();
            }
            @Override public void shutdown() {}
            @Override public void runHeartbeatTick() {}
        };
    }
}
