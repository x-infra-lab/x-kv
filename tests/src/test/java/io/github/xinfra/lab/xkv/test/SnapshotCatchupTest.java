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
 * End-to-end: drive log compaction with the snapshot pre-stage hook,
 * verify the leader can hand a real Snapshot back to raft library when
 * a follower's nextIndex falls below first_index.
 *
 * <p>Without {@link RegionPeer#maybeGenerateSnapshot}, raft library's
 * {@code Storage.snapshot()} returns ErrSnapshotTemporarilyUnavailable
 * forever, and the lagging follower can never catch up.
 */
final class SnapshotCatchupTest {

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
    void snapshotIsStagedBeforeLogCompaction() throws Exception {
        // Generate enough applied entries to push the leader past the gap
        // threshold we'll set in the worker.
        for (int i = 0; i < 40; i++) {
            try (Transaction txn = client.begin()) {
                txn.put(("k" + i).getBytes(), ("v" + i).getBytes());
                txn.commit();
            }
        }

        var leaderNode = cluster.leaderStoreFor(TestCluster.BOOTSTRAP_REGION_ID);
        var leaderPeer = cluster.realPeer(leaderNode.storeId, TestCluster.BOOTSTRAP_REGION_ID);
        var store = leaderNode.store();
        long firstBefore = leaderPeer.firstIndex();

        // Run compaction worker — it should stage a snapshot AND advance
        // first_index.
        var worker = new LogCompactionWorker(store,
                /* intervalMs= */ 60_000,
                /* gapThreshold= */ 20,
                /* safetyMargin= */ 5,
                /* proposeTimeoutMs= */ 5_000);
        try {
            int issued = worker.runOnce();
            assertThat(issued).as("compaction proposed by worker").isEqualTo(1);
        } finally {
            worker.close();
        }
        Awaitility.await().atMost(Duration.ofSeconds(5))
                .pollInterval(Duration.ofMillis(50))
                .until(() -> leaderPeer.firstIndex() > firstBefore);

        // The leader's storage now has a pending snapshot (otherwise raft
        // library couldn't recover a follower whose nextIndex < first_index).
        // We can't directly read pendingSnapshot from outside, but we CAN
        // verify the staged snapshot's coherent point-in-time by querying
        // through raftStorage.snapshot() — should return a Snapshot, not throw.
        var snap = ((io.github.xinfra.lab.xkv.kv.raft.BatchRegionPeer) leaderPeer)
                .raftStorage().snapshot();
        assertThat(snap).isNotNull();
        assertThat(snap.getMetadata().getIndex())
                .as("snapshot stages at or above leader applied_index post-compact")
                .isGreaterThanOrEqualTo(leaderPeer.firstIndex() - 1);
        assertThat(snap.getData().size())
                .as("snapshot carries user-data CFs")
                .isPositive();
    }
}
