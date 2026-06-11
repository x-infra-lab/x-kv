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
    void snapshotIsStagedBeforeLogCompaction() throws Exception {
        // Generate enough applied entries to push the leader past the gap
        // threshold we'll set in the worker.
        for (int i = 0; i < 40; i++) {
            try (Transaction txn = client.begin()) {
                txn.put(("k" + i).getBytes(), ("v" + i).getBytes());
                txn.commit();
            }
        }

        var leader = harness.leader();
        var store = wrapAllAsStore();
        long firstBefore = leader.peer.firstIndex();

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
                .until(() -> leader.peer.firstIndex() > firstBefore);

        // The leader's storage now has a pending snapshot (otherwise raft
        // library couldn't recover a follower whose nextIndex < first_index).
        // We can't directly read pendingSnapshot from outside, but we CAN
        // verify the staged snapshot's coherent point-in-time by querying
        // through raftStorage.snapshot() — should return a Snapshot, not throw.
        var snap = leader.peer.raftStorage().snapshot();
        assertThat(snap).isNotNull();
        assertThat(snap.getMetadata().getIndex())
                .as("snapshot stages at or above leader applied_index post-compact")
                .isGreaterThanOrEqualTo(leader.peer.firstIndex() - 1);
        assertThat(snap.getData().size())
                .as("snapshot carries user-data CFs")
                .isPositive();
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
