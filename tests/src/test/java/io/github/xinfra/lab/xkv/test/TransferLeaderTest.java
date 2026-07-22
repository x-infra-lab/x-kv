package io.github.xinfra.lab.xkv.test;

import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Exercises {@link io.github.xinfra.lab.xkv.kv.raft.RegionPeer#transferLeader}.
 *
 * <p>Production needs this for node drain (graceful maintenance) and PD
 * scheduler load balancing. Without it, leadership migration depends on
 * unplanned events (node restart / network partition).
 */
final class TransferLeaderTest {

    @TempDir Path baseDir;
    private TestCluster cluster;

    @BeforeEach
    void start() throws Exception {
        cluster = new TestCluster(baseDir).startReplicated(1, 3);
    }

    @AfterEach
    void stop() throws Exception {
        if (cluster != null) cluster.close();
    }

    @Test
    void transferLeaderMovesLeadershipToRequestedPeer() {
        long regionId = TestCluster.BOOTSTRAP_REGION_ID;
        var currentLeaderStore = cluster.leaderStoreFor(regionId);
        var currentLeaderPeer = cluster.realPeer(currentLeaderStore.storeId, regionId);

        var targetStore = cluster.followerStoresFor(regionId).get(0);
        var targetPeer = cluster.realPeer(targetStore.storeId, regionId);
        long targetPeerId = targetPeer.self().getId();

        currentLeaderPeer.transferLeader(targetPeerId);

        // The target must become leader within a few election timeouts.
        Awaitility.await().atMost(Duration.ofSeconds(5))
                .pollInterval(Duration.ofMillis(50))
                .until(targetPeer::isLeader);

        // And the previous leader must have stepped down.
        Awaitility.await().atMost(Duration.ofSeconds(5))
                .pollInterval(Duration.ofMillis(50))
                .until(() -> !currentLeaderPeer.isLeader());

        assertThat(targetPeer.isLeader()).isTrue();
    }
}
