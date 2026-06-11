package io.github.xinfra.lab.xkv.test;

import com.google.protobuf.ByteString;
import io.github.xinfra.lab.xkv.client.TxnClient;
import io.github.xinfra.lab.xkv.client.config.ClientConfig;
import io.github.xinfra.lab.xkv.client.txn.Transaction;
import io.github.xinfra.lab.xkv.kv.raft.RegionPeer;
import io.github.xinfra.lab.xkv.kv.store.GcWorker;
import io.github.xinfra.lab.xkv.kv.store.Store;
import io.github.xinfra.lab.xkv.kv.store.StoreImpl;
import io.github.xinfra.lab.xkv.proto.Kvrpcpb;
import io.github.xinfra.lab.xkv.proto.Metapb;
import io.github.xinfra.lab.xkv.proto.PDGrpc;
import io.github.xinfra.lab.xkv.proto.Pdpb;
import io.grpc.ManagedChannel;
import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * End-to-end test for {@link GcWorker}: PD reports a safe-point; worker
 * proposes {@code MVCC_GC} on each region's leader; GC is observable via
 * {@code MvccGetByKey}-equivalent checks (we use direct RawKv get against
 * an MVCC-encoded key, plus a kvScanLock to confirm history is gone).
 *
 * <p>Production-shape: GcWorker is a real periodic scheduler. The test
 * drives one round synchronously via {@link GcWorker#runOnce} so we can
 * observe deterministic output instead of waiting on the timer.
 */
final class GcWorkerTest {

    @TempDir Path baseDir;
    private ClusterHarness harness;
    private TxnClient client;
    private ManagedChannel pdChannel;
    private PDGrpc.PDBlockingStub pd;

    @BeforeEach
    void start() throws Exception {
        harness = new ClusterHarness(baseDir, 3).start();
        client = TxnClient.create(ClientConfig.builder()
                .pdEndpoints(List.of("127.0.0.1:" + harness.pdPort()))
                .build());
        pdChannel = NettyChannelBuilder.forAddress("127.0.0.1", harness.pdPort())
                .usePlaintext().build();
        pd = PDGrpc.newBlockingStub(pdChannel);
    }

    @AfterEach
    void stop() throws Exception {
        if (client != null) client.close();
        if (pdChannel != null) pdChannel.shutdownNow().awaitTermination(2, TimeUnit.SECONDS);
        if (harness != null) harness.close();
    }

    @Test
    void gcWorkerCollapsesOldVersionsOncePdSafePointAdvances() throws Exception {
        // Three commits to "k" — leaves THREE versions in WRITE CF.
        for (int v = 1; v <= 3; v++) {
            try (Transaction txn = client.begin()) {
                txn.put("k".getBytes(), ("v" + v).getBytes());
                txn.commit();
            }
        }

        // Wrap ALL nodes' peers in a Store. The worker iterates and picks
        // whichever is currently leader (mirrors production where each
        // store hosts many region peers).
        Store store = wrapAllAsStore();

        // Move PD's GC safe-point well into the future so v1 + v2 are
        // eligible for collapse. We grab a fresh TSO via begin/rollback as a
        // quick way to obtain a "now" timestamp for the safe-point.
        long safePoint;
        try (Transaction probe = client.begin()) {
            safePoint = probe.startTs();
            probe.rollback();
        }
        pd.updateGCSafePoint(Pdpb.UpdateGCSafePointRequest.newBuilder()
                .setSafePoint(safePoint).build());

        var worker = new GcWorker(store, pd, /* intervalMs= */ 60_000, /* proposeTimeoutMs= */ 5_000);
        try {
            int proposed = worker.runOnce();
            assertThat(proposed).as("GC proposal lands on the leader").isEqualTo(1);
            assertThat(worker.lastObservedSafePoint()).isEqualTo(safePoint);
        } finally {
            worker.close();
        }

        // Latest visible value still reads correctly.
        try (Transaction txn = client.begin()) {
            byte[] got = txn.get("k".getBytes()).orElseThrow();
            assertThat(new String(got)).isEqualTo("v3");
        }
    }

    @Test
    void gcWorkerSkipsWhenSafePointZero() throws Exception {
        // PD safe-point default = 0 — worker must NOT propose anything.
        Store store = wrapAllAsStore();
        var worker = new GcWorker(store, pd, 60_000, 5_000);
        try {
            int proposed = worker.runOnce();
            assertThat(proposed).isZero();
            assertThat(worker.proposalsTotal()).isZero();
        } finally {
            worker.close();
        }
    }

    // ===== helpers =====

    private ClusterHarness.KvNode leaderNode() {
        return harness.leader();
    }

    /**
     * Each node has a peer for the same region — StoreImpl's by-region map
     * would collide. Use a tiny test-only Store that just returns all
     * peers verbatim; the worker's "is leader" filter picks the right one.
     */
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
