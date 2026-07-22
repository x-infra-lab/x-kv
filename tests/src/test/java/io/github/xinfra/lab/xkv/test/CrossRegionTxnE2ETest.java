package io.github.xinfra.lab.xkv.test;

import io.github.xinfra.lab.xkv.client.TxnClient;
import io.github.xinfra.lab.xkv.client.XKvClient;
import io.github.xinfra.lab.xkv.client.config.ClientConfig;
import io.github.xinfra.lab.xkv.client.error.KvClientException;
import io.github.xinfra.lab.xkv.kv.store.SplitDriver;
import io.github.xinfra.lab.xkv.proto.PDGrpc;
import io.grpc.ManagedChannel;
import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Validates cross-region transactions after a split.
 *
 * <p>Setup: 1 PD + 3 KV stores → split at "m" → Region 1 [, m), Region 2 [m, ).
 * Tests exercise the client SDK's multi-region 2PC path where prewrite and
 * commit must be sent to different region leaders.
 */
final class CrossRegionTxnE2ETest {

    @TempDir Path baseDir;
    private TestCluster cluster;
    private XKvClient rawClient;
    private TxnClient txnClient;

    @BeforeEach
    void start() throws Exception {
        cluster = new TestCluster(baseDir).startReplicated(1, 3);

        rawClient = XKvClient.create(ClientConfig.builder()
                .pdEndpoints(cluster.pdEndpoints())
                .build());
        txnClient = TxnClient.create(ClientConfig.builder()
                .pdEndpoints(cluster.pdEndpoints())
                .build());

        // Split at "m" to create two regions.
        var leaderNode = cluster.leaderStoreFor(TestCluster.BOOTSTRAP_REGION_ID);
        var parentPeer = cluster.realPeer(leaderNode.storeId, TestCluster.BOOTSTRAP_REGION_ID);
        var driver = new SplitDriver(cluster.pdStub(), 10_000);
        var resulting = driver.split(parentPeer, List.of("m".getBytes()));
        assertThat(resulting).hasSize(2);

        var child = resulting.get(1);

        // Wait for the child peer to be live on all stores.
        cluster.awaitResidentPeers(child.getId(), cluster.stores().size());

        // Wait for leader election on the child region.
        cluster.waitForRegionLeader(child.getId());

        // The real heartbeaters publish the split (parent's shrunk range +
        // the new child region and its leader) to PD. Wait until PD's
        // metadata reflects both regions before the client tries to route.
        Awaitility.await().atMost(Duration.ofSeconds(15))
                .pollInterval(Duration.ofMillis(200))
                .until(() -> {
                    var pd = cluster.pdLeader().server.state();
                    var parentInfo = pd.getRegion(TestCluster.BOOTSTRAP_REGION_ID);
                    var childInfo = pd.getRegion(child.getId());
                    return parentInfo.isPresent()
                            && "m".equals(parentInfo.get().getEndKey().toStringUtf8())
                            && childInfo.isPresent()
                            && pd.getLeader(TestCluster.BOOTSTRAP_REGION_ID).isPresent()
                            && pd.getLeader(child.getId()).isPresent();
                });

        // Invalidate client caches so they re-discover the split regions.
        var rawImpl = (io.github.xinfra.lab.xkv.client.XKvClientImpl) rawClient;
        rawImpl.regionCache().invalidate(TestCluster.BOOTSTRAP_REGION_ID);
        var txnImpl = (io.github.xinfra.lab.xkv.client.TxnClientImpl) txnClient;
        txnImpl.regionCache().invalidate(TestCluster.BOOTSTRAP_REGION_ID);
    }

    @AfterEach
    void teardown() {
        if (rawClient != null) rawClient.close();
        if (txnClient != null) txnClient.close();
        if (cluster != null) cluster.close();
    }

    @Test
    void crossRegionRawPutAndGet() throws Exception {
        var raw = rawClient.raw();

        // "aaa" → Region 1 [, m)
        raw.put("aaa".getBytes(), "val-aaa".getBytes());
        // "zzz" → Region 2 [m, )
        raw.put("zzz".getBytes(), "val-zzz".getBytes());

        var got1 = raw.get("aaa".getBytes());
        assertThat(got1).isPresent();
        assertThat(new String(got1.get())).isEqualTo("val-aaa");

        var got2 = raw.get("zzz".getBytes());
        assertThat(got2).isPresent();
        assertThat(new String(got2.get())).isEqualTo("val-zzz");
    }

    @Test
    void crossRegionTxnCommit() throws Exception {
        // T1: write to both regions, then commit.
        try (var txn = txnClient.begin()) {
            txn.put("apple".getBytes(), "fruit".getBytes());   // Region 1 [, m)
            txn.put("zebra".getBytes(), "animal".getBytes());  // Region 2 [m, )
            txn.commit();
        }

        // T2: read from both regions — both keys must be visible.
        try (var txn = txnClient.begin()) {
            var apple = txn.get("apple".getBytes());
            assertThat(apple)
                    .as("apple should be visible after cross-region commit")
                    .isPresent();
            assertThat(new String(apple.get())).isEqualTo("fruit");

            var zebra = txn.get("zebra".getBytes());
            assertThat(zebra)
                    .as("zebra should be visible after cross-region commit")
                    .isPresent();
            assertThat(new String(zebra.get())).isEqualTo("animal");
        }
    }

    @Test
    void crossRegionTxnRollback() throws Exception {
        // T1: write to both regions, then rollback.
        try (var txn = txnClient.begin()) {
            txn.put("banana".getBytes(), "fruit".getBytes());  // Region 1
            txn.put("yoga".getBytes(), "exercise".getBytes()); // Region 2
            txn.rollback();
        }

        // T2: neither key should be visible.
        try (var txn = txnClient.begin()) {
            assertThat(txn.get("banana".getBytes()))
                    .as("banana should not exist after rollback")
                    .isEmpty();
            assertThat(txn.get("yoga".getBytes()))
                    .as("yoga should not exist after rollback")
                    .isEmpty();
        }
    }

    @Test
    void crossRegionWriteConflict() throws Exception {
        // Seed a key in Region 1.
        try (var txn = txnClient.begin()) {
            txn.put("cat".getBytes(), "original".getBytes());
            txn.commit();
        }

        // T1: read "cat", buffer a write.
        var t1 = txnClient.begin();
        t1.get("cat".getBytes());
        t1.put("cat".getBytes(), "t1-update".getBytes());

        // T2: commit a conflicting write to "cat" before T1 commits.
        try (var t2 = txnClient.begin()) {
            t2.put("cat".getBytes(), "t2-wins".getBytes());
            t2.commit();
        }

        // T1 commit should fail (write conflict or lock conflict).
        boolean t1Failed = false;
        try {
            t1.commit();
        } catch (KvClientException e) {
            t1Failed = true;
        } finally {
            t1.close();
        }

        // Verify T2's value won.
        try (var txn = txnClient.begin()) {
            var val = txn.get("cat".getBytes());
            assertThat(val).isPresent();
            assertThat(new String(val.get())).isEqualTo("t2-wins");
        }

        assertThat(t1Failed)
                .as("T1 should fail due to write conflict")
                .isTrue();
    }

    @Test
    void crossRegionBankTransfer() throws Exception {
        // Accounts in different regions.
        byte[] aliceKey = "alice".getBytes();   // Region 1 [, m)
        byte[] mikeKey  = "mike".getBytes();    // Region 2 [m, )

        // Seed balances.
        try (var txn = txnClient.begin()) {
            txn.put(aliceKey, "1000".getBytes());
            txn.put(mikeKey, "1000".getBytes());
            txn.commit();
        }

        // Transfer alice → mike 100.
        try (var txn = txnClient.begin()) {
            int aliceBal = Integer.parseInt(new String(txn.get(aliceKey).orElseThrow()));
            int mikeBal = Integer.parseInt(new String(txn.get(mikeKey).orElseThrow()));
            txn.put(aliceKey, Integer.toString(aliceBal - 100).getBytes());
            txn.put(mikeKey, Integer.toString(mikeBal + 100).getBytes());
            txn.commit();
        }

        // Verify balances.
        try (var txn = txnClient.begin()) {
            int aliceBal = Integer.parseInt(new String(txn.get(aliceKey).orElseThrow()));
            int mikeBal = Integer.parseInt(new String(txn.get(mikeKey).orElseThrow()));
            assertThat(aliceBal).as("alice balance after transfer").isEqualTo(900);
            assertThat(mikeBal).as("mike balance after transfer").isEqualTo(1100);
            assertThat(aliceBal + mikeBal).as("total balance conserved").isEqualTo(2000);
        }
    }
}
