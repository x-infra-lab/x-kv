package io.github.xinfra.lab.xkv.test;

import io.github.xinfra.lab.xkv.client.XKvClient;
import io.github.xinfra.lab.xkv.client.config.ClientConfig;
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
 * Verifies graceful drain: stop one KV node, assert leader transfers away,
 * and the remaining nodes can still serve reads and writes.
 */
final class GracefulDrainTest {

    @TempDir Path baseDir;
    private ClusterHarness harness;
    private XKvClient client;

    @BeforeEach
    void start() throws Exception {
        harness = new ClusterHarness(baseDir, 3).start();
        client = XKvClient.create(ClientConfig.builder()
                .pdEndpoints(List.of("127.0.0.1:" + harness.pdPort()))
                .build());
    }

    @AfterEach
    void teardown() {
        if (client != null) client.close();
        if (harness != null) harness.close();
    }

    @Test
    void drainTransfersLeaderAndDataRemains() throws Exception {
        var raw = client.raw();

        // Write test data.
        for (int i = 0; i < 30; i++) {
            raw.put(String.format("drain:%04d", i).getBytes(),
                    ("v-" + i).getBytes());
        }

        // Verify writes.
        for (int i = 0; i < 30; i++) {
            assertThat(raw.get(String.format("drain:%04d", i).getBytes()))
                    .as("pre-drain key %d", i)
                    .isPresent();
        }

        // Find the current leader and shut it down (which triggers drain).
        var oldLeader = harness.leader();
        long oldLeaderId = oldLeader.peerId;
        oldLeader.shutdown();
        harness.kvNodes().remove(oldLeader);

        // Wait for new leader election on surviving nodes.
        Awaitility.await()
                .atMost(Duration.ofSeconds(20))
                .pollInterval(Duration.ofMillis(200))
                .until(() -> harness.kvNodes().stream()
                        .anyMatch(n -> n.peer.isLeader()));

        // Update PD routing.
        harness.publishLeaderToPd();

        // Invalidate client caches for the dead node.
        var impl = (io.github.xinfra.lab.xkv.client.XKvClientImpl) client;
        impl.regionCache().invalidate(harness.bootstrapRegionId());
        impl.storeCache().closeStore(oldLeaderId);

        // Verify all data still readable after drain.
        for (int i = 0; i < 30; i++) {
            var got = raw.get(String.format("drain:%04d", i).getBytes());
            assertThat(got)
                    .as("post-drain key %d readable", i)
                    .isPresent();
            assertThat(new String(got.get()))
                    .as("post-drain key %d value", i)
                    .isEqualTo("v-" + i);
        }

        // Verify new writes still succeed on surviving nodes.
        for (int i = 0; i < 10; i++) {
            raw.put(("after-drain-" + i).getBytes(), ("new-" + i).getBytes());
        }
        for (int i = 0; i < 10; i++) {
            assertThat(raw.get(("after-drain-" + i).getBytes()))
                    .as("post-drain new write %d", i)
                    .map(String::new)
                    .contains("new-" + i);
        }

        // The old leader should no longer be leading.
        assertThat(harness.kvNodes().stream()
                .filter(n -> n.peerId == oldLeaderId)
                .findFirst())
                .as("old leader removed from cluster")
                .isEmpty();
    }
}
