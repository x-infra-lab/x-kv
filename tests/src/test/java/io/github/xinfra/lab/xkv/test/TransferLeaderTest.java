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
    void transferLeaderMovesLeadershipToRequestedPeer() {
        var currentLeader = harness.leader();
        var target = harness.kvNodes().stream()
                .filter(n -> n.peerId != currentLeader.peerId)
                .findFirst().orElseThrow();

        currentLeader.peer.transferLeader(target.peerId);

        // The target must become leader within a few election timeouts.
        Awaitility.await().atMost(Duration.ofSeconds(5))
                .pollInterval(Duration.ofMillis(50))
                .until(() -> target.peer.isLeader());

        // And the previous leader must have stepped down.
        Awaitility.await().atMost(Duration.ofSeconds(5))
                .pollInterval(Duration.ofMillis(50))
                .until(() -> !currentLeader.peer.isLeader());

        assertThat(target.peer.isLeader()).isTrue();
    }
}
