package io.github.xinfra.lab.xkv.test;

import io.github.xinfra.lab.xkv.client.XKvClient;
import io.github.xinfra.lab.xkv.client.config.ClientConfig;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Chaos test: kill the leader KV node, wait for re-election, then verify
 * data consistency and continued availability.
 *
 * <p>Simulates process-level failure in a 1 PD + 3 KV cluster:
 * <ol>
 *   <li>Write data</li>
 *   <li>Kill the leader KV node</li>
 *   <li>Wait for a new leader to be elected</li>
 *   <li>Verify all previously-written data is still readable</li>
 *   <li>Verify new writes succeed</li>
 * </ol>
 */
final class NodeRestartTest {

    @TempDir Path baseDir;
    private TestCluster cluster;
    private XKvClient client;

    @BeforeEach
    void start() throws Exception {
        cluster = new TestCluster(baseDir).startReplicated(1, 3);
        client = XKvClient.create(ClientConfig.builder()
                .pdEndpoints(cluster.pdEndpoints())
                .build());
    }

    @AfterEach
    void teardown() {
        if (client != null) client.close();
        if (cluster != null) cluster.close();
    }

    @Test
    void dataConsistentAfterLeaderKill() throws Exception {
        var raw = client.raw();

        // Phase 1: write test data.
        int keyCount = 50;
        for (int i = 0; i < keyCount; i++) {
            raw.put(String.format("chaos:%04d", i).getBytes(),
                    ("val-" + i).getBytes());
        }

        // Verify writes are visible.
        for (int i = 0; i < keyCount; i++) {
            assertThat(raw.get(String.format("chaos:%04d", i).getBytes()))
                    .as("pre-kill key %d", i)
                    .isPresent();
        }

        // Phase 2: kill the leader.
        long regionId = TestCluster.BOOTSTRAP_REGION_ID;
        var oldLeader = cluster.leaderStoreFor(regionId);
        long oldLeaderId = oldLeader.storeId;
        cluster.killStore(oldLeaderId);

        // Phase 3: wait for new leader election.
        cluster.waitForNewLeaderOtherThan(regionId, oldLeaderId);

        // Invalidate client caches for the dead node.
        var impl = (io.github.xinfra.lab.xkv.client.XKvClientImpl) client;
        impl.regionCache().invalidate(regionId);
        impl.storeCache().closeStore(oldLeaderId);

        // Phase 4: verify all data is still readable.
        for (int i = 0; i < keyCount; i++) {
            var got = raw.get(String.format("chaos:%04d", i).getBytes());
            assertThat(got)
                    .as("post-kill key %d readable", i)
                    .isPresent();
            assertThat(new String(got.get()))
                    .as("post-kill key %d value", i)
                    .isEqualTo("val-" + i);
        }

        // Phase 5: new writes still work.
        for (int i = 0; i < 20; i++) {
            raw.put(("newkey-" + i).getBytes(), ("newval-" + i).getBytes());
        }
        for (int i = 0; i < 20; i++) {
            assertThat(raw.get(("newkey-" + i).getBytes()))
                    .as("new write key %d", i)
                    .map(String::new)
                    .contains("newval-" + i);
        }
    }

    @Test
    void scanWorksAfterLeaderChange() throws Exception {
        var raw = client.raw();

        // Write a range of keys.
        for (int i = 0; i < 30; i++) {
            raw.put(String.format("rscan:%04d", i).getBytes(),
                    ("v" + i).getBytes());
        }

        // Kill leader.
        long regionId = TestCluster.BOOTSTRAP_REGION_ID;
        var oldLeader = cluster.leaderStoreFor(regionId);
        long oldLeaderId = oldLeader.storeId;
        cluster.killStore(oldLeaderId);

        cluster.waitForNewLeaderOtherThan(regionId, oldLeaderId);

        var impl = (io.github.xinfra.lab.xkv.client.XKvClientImpl) client;
        impl.regionCache().invalidate(regionId);
        impl.storeCache().closeStore(oldLeaderId);

        // Scan after leader change — all 30 keys must be present.
        var pairs = raw.scan("rscan:0000".getBytes(), "rscan:9999".getBytes(), 100);
        assertThat(pairs).hasSize(30);
    }
}
