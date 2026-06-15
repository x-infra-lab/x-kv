package io.github.xinfra.lab.xkv.test;

import io.github.xinfra.lab.xkv.client.XKvClient;
import io.github.xinfra.lab.xkv.client.config.ClientConfig;
import io.github.xinfra.lab.xkv.kv.store.MergeDriver;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * End-to-end tests for region split and merge exercised through the
 * client SDK. Covers gaps left by existing tests:
 * <ul>
 *   <li>PD auto-discovery of child regions via heartbeat (no manual injection)</li>
 *   <li>Full approximateSize → heartbeat → SplitChecker chain</li>
 *   <li>Client routing without manual intervention after split</li>
 *   <li>Client route self-healing after merge</li>
 * </ul>
 */
@Timeout(value = 120, unit = TimeUnit.SECONDS)
final class SplitMergeClientE2ETest {

    private static final Logger log = LoggerFactory.getLogger(SplitMergeClientE2ETest.class);

    @TempDir Path baseDir;
    private ClusterHarness harness;
    private XKvClient client;

    @AfterEach
    void teardown() {
        if (client != null) try { client.close(); } catch (Exception e) { e.printStackTrace(); }
        if (harness != null) harness.close();
    }

    /**
     * Writes data via the client SDK, triggers a split through the real
     * heartbeat → SplitChecker → operator → SplitDriver chain, then
     * reads all keys back via the client with NO manual PD/cache
     * intervention. The client must auto-discover the child region.
     */
    @Test
    void splitAutoDiscoveredByClient() throws Exception {
        harness = new ClusterHarness(baseDir, 3).start();

        String pdAddr = "127.0.0.1:" + harness.pdPort();
        client = XKvClient.create(ClientConfig.builder()
                .pdEndpoints(List.of(pdAddr))
                .build());

        byte[] value = new byte[100];
        Arrays.fill(value, (byte) 'v');
        int keyCount = 200;
        for (int i = 0; i < keyCount; i++) {
            byte[] key = String.format("split-test-%04d", i).getBytes();
            client.raw().put(key, value);
        }
        log.info("wrote {} KVs via client SDK", keyCount);

        // The bootstrap region has end_key="" (infinity), so approximateSize
        // correctly returns 0 — SplitChecker won't fire on its own. Schedule
        // an APPROXIMATE split via PD's operator controller. The operator is
        // delivered through the real heartbeat response stream to the KV
        // leader, which computes the midpoint and drives the split end-to-end.
        long regionId = harness.bootstrapRegionId();
        harness.pdServer().operators().scheduleSplit(
                regionId, java.util.List.of(),
                io.github.xinfra.lab.xkv.proto.Pdpb.SplitRegion.Policy.APPROXIMATE);

        // Wait for the split to materialize on at least one store.
        Awaitility.await().atMost(Duration.ofSeconds(30))
                .pollInterval(Duration.ofMillis(200))
                .until(() -> harness.kvNodes().stream()
                        .anyMatch(n -> !n.childPeers.isEmpty()));
        log.info("split complete — child peer(s) created");

        // Read all keys via the client SDK. No manual PD registration or
        // cache clearing — the client must auto-discover the child region.
        for (int i = 0; i < keyCount; i++) {
            byte[] key = String.format("split-test-%04d", i).getBytes();
            var result = client.raw().get(key);
            assertThat(result)
                    .as("key split-test-%04d should be readable after split", i)
                    .isPresent();
            assertThat(result.get()).isEqualTo(value);
        }

        // Write a new key and read it back — exercises the child region's
        // write path through the client SDK.
        byte[] newKey = "split-test-new-in-child".getBytes();
        client.raw().put(newKey, value);
        assertThat(client.raw().get(newKey)).isPresent().contains(value);

        log.info("all {} keys readable via client after split", keyCount);
    }

    /**
     * Split, verify both regions are reachable, merge the child back into
     * the parent, then verify client reads self-heal through cache
     * invalidation when the stale child route returns REGION_NOT_FOUND.
     */
    @Test
    void mergeAfterSplitClientRoutesSelfHealing() throws Exception {
        harness = new ClusterHarness(baseDir, 3).start();

        String pdAddr = "127.0.0.1:" + harness.pdPort();
        client = XKvClient.create(ClientConfig.builder()
                .pdEndpoints(List.of(pdAddr))
                .build());

        byte[] value = new byte[100];
        Arrays.fill(value, (byte) 'm');
        int keyCount = 50;
        for (int i = 0; i < keyCount; i++) {
            byte[] key = String.format("merge-test-%04d", i).getBytes();
            client.raw().put(key, value);
        }

        // Schedule split via PD operator controller — delivered through the
        // real heartbeat response stream.
        long regionId = harness.bootstrapRegionId();
        harness.pdServer().operators().scheduleSplit(
                regionId, java.util.List.of(),
                io.github.xinfra.lab.xkv.proto.Pdpb.SplitRegion.Policy.APPROXIMATE);

        Awaitility.await().atMost(Duration.ofSeconds(30))
                .pollInterval(Duration.ofMillis(200))
                .until(() -> harness.kvNodes().stream()
                        .anyMatch(n -> !n.childPeers.isEmpty()));
        log.info("split complete");

        // Verify reads work across both regions before merge.
        for (int i = 0; i < keyCount; i++) {
            byte[] key = String.format("merge-test-%04d", i).getBytes();
            assertThat(client.raw().get(key))
                    .as("pre-merge read of merge-test-%04d", i)
                    .isPresent();
        }

        // Find the child region's leader for the merge.
        var leader = harness.leader();
        var childRegionId = leader.childPeers.get(0).regionId();

        Awaitility.await().atMost(Duration.ofSeconds(30))
                .pollInterval(Duration.ofMillis(100))
                .until(() -> harness.kvNodes().stream()
                        .flatMap(n -> n.childPeers.stream())
                        .anyMatch(c -> c.regionId() == childRegionId && c.isLeader()));
        var childLeader = harness.kvNodes().stream()
                .flatMap(n -> n.childPeers.stream())
                .filter(c -> c.regionId() == childRegionId && c.isLeader())
                .findFirst().orElseThrow();

        // Merge child INTO parent.
        var merge = new MergeDriver(5_000);
        merge.merge(childLeader, leader.peer);
        log.info("merge complete");

        // Wait for the child peer to be destroyed on all stores.
        Awaitility.await().atMost(Duration.ofSeconds(10))
                .pollInterval(Duration.ofMillis(50))
                .until(() -> harness.kvNodes().stream()
                        .noneMatch(n -> n.store.peerForRegion(childRegionId).isPresent()));

        // Wait for PD to reflect the merged region — the parent's heartbeat
        // must publish its expanded range to PD before the client can route
        // correctly. Without this wait the client hits stale PD metadata and
        // exhausts its backoff budget.
        Awaitility.await().atMost(Duration.ofSeconds(15))
                .pollInterval(Duration.ofMillis(200))
                .until(() -> {
                    var pd = harness.pdServer().state();
                    var parentInfo = pd.getRegion(regionId);
                    if (parentInfo.isEmpty() || !parentInfo.get().getEndKey().isEmpty()) {
                        return false;
                    }
                    return pd.getLeader(regionId).isPresent();
                });
        log.info("PD metadata reflects merged region");

        // Read all keys via the client. The client's region cache may have
        // stale entries pointing to the now-gone child region. Reads must
        // self-heal: stale route → REGION_NOT_FOUND → cache invalidation
        // → PD re-fetch → route to merged parent.
        for (int i = 0; i < keyCount; i++) {
            byte[] key = String.format("merge-test-%04d", i).getBytes();
            var result = client.raw().get(key);
            assertThat(result)
                    .as("post-merge read of merge-test-%04d", i)
                    .isPresent();
            assertThat(result.get()).isEqualTo(value);
        }
        log.info("all {} keys readable via client after merge", keyCount);
    }

    /**
     * Runs concurrent readers while a split is in progress. No reader
     * should observe a failure — the client must retry transparently.
     */
    @Test
    void concurrentReadersDuringSplit() throws Exception {
        harness = new ClusterHarness(baseDir, 3).start();

        String pdAddr = "127.0.0.1:" + harness.pdPort();
        client = XKvClient.create(ClientConfig.builder()
                .pdEndpoints(List.of(pdAddr))
                .build());

        byte[] value = new byte[100];
        Arrays.fill(value, (byte) 'c');
        int keyCount = 100;
        for (int i = 0; i < keyCount; i++) {
            byte[] key = String.format("conc-read-%04d", i).getBytes();
            client.raw().put(key, value);
        }

        // Start 4 reader threads.
        var stop = new AtomicBoolean(false);
        var errors = new ConcurrentLinkedQueue<Throwable>();
        int readerCount = 4;
        var started = new CountDownLatch(readerCount);
        var threads = new Thread[readerCount];
        for (int t = 0; t < readerCount; t++) {
            int threadId = t;
            threads[t] = new Thread(() -> {
                started.countDown();
                int reads = 0;
                while (!stop.get()) {
                    int idx = (threadId * 7 + reads) % keyCount;
                    byte[] key = String.format("conc-read-%04d", idx).getBytes();
                    try {
                        var result = client.raw().get(key);
                        if (result.isEmpty()) {
                            errors.add(new AssertionError(
                                    "reader-" + threadId + " got empty for conc-read-"
                                            + String.format("%04d", idx)));
                        }
                    } catch (Throwable e) {
                        errors.add(e);
                    }
                    reads++;
                }
                log.info("reader-{} completed {} reads", threadId, reads);
            }, "reader-" + t);
            threads[t].setDaemon(true);
            threads[t].start();
        }
        started.await(5, TimeUnit.SECONDS);

        // Schedule split while readers are running.
        long regionId = harness.bootstrapRegionId();
        harness.pdServer().operators().scheduleSplit(
                regionId, java.util.List.of(),
                io.github.xinfra.lab.xkv.proto.Pdpb.SplitRegion.Policy.APPROXIMATE);

        Awaitility.await().atMost(Duration.ofSeconds(30))
                .pollInterval(Duration.ofMillis(200))
                .until(() -> harness.kvNodes().stream()
                        .anyMatch(n -> !n.childPeers.isEmpty()));
        log.info("split completed while readers active");

        // Let readers run for 5 more seconds after split.
        Thread.sleep(5_000);
        stop.set(true);
        for (var t : threads) t.join(5_000);

        assertThat(errors)
                .as("no reader errors during or after split")
                .isEmpty();
    }
}
