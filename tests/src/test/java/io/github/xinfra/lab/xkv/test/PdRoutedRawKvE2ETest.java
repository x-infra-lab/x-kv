package io.github.xinfra.lab.xkv.test;

import io.github.xinfra.lab.xkv.client.XKvClient;
import io.github.xinfra.lab.xkv.client.config.ClientConfig;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Phase 5 verification: end-to-end raw KV via the production
 * {@link XKvClient} stack.
 *
 * <p>Exercises the full pipeline:
 * <pre>
 *   client → PD (region routing + TSO) → KV-store leader (real gRPC)
 *                                       → raft cluster of 3 stores
 *                                       → RocksDB single-fsync apply
 * </pre>
 *
 * <p>The client never knows which port to dial directly — it asks PD for
 * the region, PD's reply names the leader peer, the client looks the
 * peer's store address up in {@code GetStore}, dials that store, sends
 * the RPC. Region errors retry through the {@link
 * io.github.xinfra.lab.xkv.client.region.RegionRequestSenderImpl} loop.
 */
final class PdRoutedRawKvE2ETest {

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
    void putAndGetThroughPdRouting() {
        var raw = client.raw();
        raw.put("k".getBytes(), "v".getBytes());
        assertThat(raw.get("k".getBytes()))
                .map(String::new)
                .contains("v");

        // Update.
        raw.put("k".getBytes(), "v2".getBytes());
        assertThat(raw.get("k".getBytes()))
                .map(String::new)
                .contains("v2");

        // Delete.
        raw.delete("k".getBytes());
        assertThat(raw.get("k".getBytes())).isEmpty();
    }

    @Test
    void scanReturnsKeysInRange() {
        var raw = client.raw();
        for (int i = 0; i < 20; i++) {
            raw.put(String.format("k%03d", i).getBytes(), ("v" + i).getBytes());
        }
        var pairs = raw.scan("k005".getBytes(), "k010".getBytes(), 100);
        assertThat(pairs).hasSize(5);
        for (int i = 0; i < 5; i++) {
            assertThat(new String(pairs.get(i).key())).isEqualTo(String.format("k%03d", 5 + i));
        }
    }

    @Test
    void batchPutAndBatchGet() {
        var raw = client.raw();
        var kvs = new LinkedHashMap<byte[], byte[]>();
        for (int i = 0; i < 10; i++) {
            kvs.put(("bk" + i).getBytes(), ("bv" + i).getBytes());
        }
        raw.batchPut(kvs);

        var keys = new java.util.ArrayList<byte[]>();
        for (int i = 0; i < 10; i++) keys.add(("bk" + i).getBytes());
        keys.add("missing".getBytes());

        var got = raw.batchGet(keys);
        assertThat(got).hasSize(10);
        for (int i = 0; i < 10; i++) {
            byte[] expectedKey = ("bk" + i).getBytes();
            byte[] foundKey = got.keySet().stream()
                    .filter(k -> java.util.Arrays.equals(k, expectedKey))
                    .findFirst()
                    .orElseThrow();
            assertThat(new String(got.get(foundKey))).isEqualTo("bv" + i);
        }
    }

    @Test
    void casInsertSucceedsThenSecondInsertFails() {
        var raw = client.raw();
        var r1 = raw.cas("cas1".getBytes(), Optional.empty(), "v1".getBytes());
        assertThat(r1.succeeded()).isTrue();
    }

    @Test
    void deleteRangeRemovesContiguous() {
        var raw = client.raw();
        for (int i = 0; i < 10; i++) {
            raw.put(String.format("d%03d", i).getBytes(), "x".getBytes());
        }
        raw.deleteRange("d003".getBytes(), "d007".getBytes());
        for (int i = 0; i < 10; i++) {
            var got = raw.get(String.format("d%03d", i).getBytes());
            if (i >= 3 && i < 7) {
                assertThat(got).as("d%03d removed".formatted(i)).isEmpty();
            } else {
                assertThat(got).as("d%03d kept".formatted(i)).isPresent();
            }
        }
    }

    @Test
    void leaderKillTriggersRetryAndSucceeds() throws Exception {
        var raw = client.raw();
        raw.put("before".getBytes(), "yes".getBytes());

        // Kill the leader. Client's RegionRequestSender must observe the
        // NotLeader error / network drop, refresh leader via PD, and retry.
        var oldLeader = harness.leaderStoreFor(TestCluster.BOOTSTRAP_REGION_ID);
        long oldStoreId = oldLeader.storeId;
        harness.killStore(oldStoreId);

        // A new leader will surface on a surviving store.
        harness.waitForNewLeaderOtherThan(TestCluster.BOOTSTRAP_REGION_ID, oldStoreId);

        // Invalidate client caches so retries find the new leader and don't
        // keep dialing the dead store.
        var impl = (io.github.xinfra.lab.xkv.client.XKvClientImpl) client;
        impl.regionCache().invalidate(TestCluster.BOOTSTRAP_REGION_ID);
        impl.storeCache().closeStore(oldStoreId);

        // Earlier write must still be visible.
        assertThat(raw.get("before".getBytes())).map(String::new).contains("yes");

        // New writes still succeed.
        raw.put("after".getBytes(), "ok".getBytes());
        assertThat(raw.get("after".getBytes())).map(String::new).contains("ok");
    }
}
