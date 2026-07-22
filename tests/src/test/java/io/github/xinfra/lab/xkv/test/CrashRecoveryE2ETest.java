package io.github.xinfra.lab.xkv.test;

import io.github.xinfra.lab.xkv.client.XKvClient;
import io.github.xinfra.lab.xkv.client.config.ClientConfig;
import io.github.xinfra.lab.xkv.kv.engine.StorageEngine;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Validates crash recovery: a killed node restarts from the same dataDir,
 * replays its raft log + RocksDB WAL, and rejoins the cluster with all
 * data intact.
 *
 * <p>Differs from {@link NodeRestartTest} in that the killed node is
 * <b>restarted</b> rather than permanently removed.
 */
final class CrashRecoveryE2ETest {

    @TempDir Path baseDir;
    private TestCluster harness;
    private XKvClient client;

    @BeforeEach
    void start() throws Exception {
        harness = new TestCluster(baseDir).startReplicated(1, 3);
        client = XKvClient.create(ClientConfig.builder()
                .pdEndpoints(harness.pdEndpoints())
                .build());
    }

    @AfterEach
    void teardown() {
        if (client != null) client.close();
        if (harness != null) harness.close();
    }

    @Test
    void nodeRecoverAfterHardKill() throws Exception {
        var raw = client.raw();

        // Write data.
        for (int i = 0; i < 100; i++) {
            raw.put(String.format("crash:%04d", i).getBytes(),
                    ("v-" + i).getBytes());
        }

        // Verify writes.
        for (int i = 0; i < 100; i++) {
            assertThat(raw.get(String.format("crash:%04d", i).getBytes()))
                    .as("pre-kill key %d", i)
                    .isPresent();
        }

        // Kill a follower (not the leader — so the cluster stays available).
        var follower = harness.followerStoresFor(TestCluster.BOOTSTRAP_REGION_ID).get(0);
        long killedStoreId = follower.storeId;
        harness.killStore(killedStoreId);

        // Write a few more keys while the follower is down.
        for (int i = 100; i < 110; i++) {
            raw.put(String.format("crash:%04d", i).getBytes(),
                    ("v-" + i).getBytes());
        }

        // Restart the killed node.
        var restarted = harness.restartStore(killedStoreId);

        // Wait for the restarted node's raft engine to catch up.
        Awaitility.await()
                .atMost(Duration.ofSeconds(30))
                .pollInterval(Duration.ofMillis(200))
                .until(() -> {
                    // Check that the restarted node has all 110 keys in RocksDB.
                    for (int i = 0; i < 110; i++) {
                        byte[] key = String.format("crash:%04d", i).getBytes();
                        byte[] val = restarted.server.engine().get(StorageEngine.Cf.DEFAULT, key);
                        if (val == null) return false;
                    }
                    return true;
                });

        // Verify via client that reads still work end-to-end.
        for (int i = 0; i < 110; i++) {
            assertThat(raw.get(String.format("crash:%04d", i).getBytes()))
                    .as("post-restart key %d", i)
                    .isPresent();
        }
    }

    @Test
    void writesDuringRecoveryReplicatedToRestoredNode() throws Exception {
        var raw = client.raw();

        // Phase 1: write 50 keys.
        for (int i = 0; i < 50; i++) {
            raw.put(String.format("recover:%04d", i).getBytes(),
                    ("v-" + i).getBytes());
        }

        // Kill the leader.
        var oldLeader = harness.leaderStoreFor(TestCluster.BOOTSTRAP_REGION_ID);
        long killedStoreId = oldLeader.storeId;
        harness.killStore(killedStoreId);

        // Wait for a new leader on a surviving store.
        harness.waitForNewLeaderOtherThan(TestCluster.BOOTSTRAP_REGION_ID, killedStoreId);

        // Invalidate client caches for the dead node.
        var impl = (io.github.xinfra.lab.xkv.client.XKvClientImpl) client;
        impl.regionCache().invalidate(TestCluster.BOOTSTRAP_REGION_ID);
        impl.storeCache().closeStore(killedStoreId);

        // Phase 2: write 50 more keys while old leader is down.
        for (int i = 50; i < 100; i++) {
            raw.put(String.format("recover:%04d", i).getBytes(),
                    ("v-" + i).getBytes());
        }

        // Phase 3: restart the killed leader.
        var restarted = harness.restartStore(killedStoreId);

        // Wait for the restarted node to replicate all 100 keys.
        Awaitility.await()
                .atMost(Duration.ofSeconds(30))
                .pollInterval(Duration.ofMillis(200))
                .until(() -> {
                    for (int i = 0; i < 100; i++) {
                        byte[] key = String.format("recover:%04d", i).getBytes();
                        byte[] val = restarted.server.engine().get(StorageEngine.Cf.DEFAULT, key);
                        if (val == null) return false;
                    }
                    return true;
                });
    }

    @Test
    void leaderRecoveryAndReElection() throws Exception {
        var raw = client.raw();

        // Write data.
        for (int i = 0; i < 30; i++) {
            raw.put(String.format("leader:%04d", i).getBytes(),
                    ("v-" + i).getBytes());
        }

        // Kill the leader.
        var oldLeader = harness.leaderStoreFor(TestCluster.BOOTSTRAP_REGION_ID);
        long killedStoreId = oldLeader.storeId;
        harness.killStore(killedStoreId);

        // Wait for a new leader on a surviving store.
        harness.waitForNewLeaderOtherThan(TestCluster.BOOTSTRAP_REGION_ID, killedStoreId);

        var impl = (io.github.xinfra.lab.xkv.client.XKvClientImpl) client;
        impl.regionCache().invalidate(TestCluster.BOOTSTRAP_REGION_ID);
        impl.storeCache().closeStore(killedStoreId);

        // Restart old leader.
        var restarted = harness.restartStore(killedStoreId);

        // Wait for the restarted node to join the cluster.
        Awaitility.await()
                .atMost(Duration.ofSeconds(30))
                .pollInterval(Duration.ofMillis(200))
                .until(() -> {
                    // The restarted node should have data and NOT be leader
                    // (the new leader was already elected).
                    byte[] val = restarted.server.engine().get(StorageEngine.Cf.DEFAULT,
                            "leader:0000".getBytes());
                    return val != null;
                });

        // Verify cluster is fully functional with 3 nodes.
        assertThat(harness.stores()).hasSize(3);

        // New writes should succeed.
        for (int i = 30; i < 40; i++) {
            raw.put(String.format("leader:%04d", i).getBytes(),
                    ("v-" + i).getBytes());
        }
        for (int i = 30; i < 40; i++) {
            assertThat(raw.get(String.format("leader:%04d", i).getBytes()))
                    .as("post-recovery new write %d", i)
                    .map(String::new)
                    .contains("v-" + i);
        }
    }
}
