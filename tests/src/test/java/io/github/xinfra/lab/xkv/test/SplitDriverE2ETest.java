package io.github.xinfra.lab.xkv.test;

import io.github.xinfra.lab.xkv.client.TxnClient;
import io.github.xinfra.lab.xkv.client.config.ClientConfig;
import io.github.xinfra.lab.xkv.client.txn.Transaction;
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
 * End-to-end split: PD allocates IDs via AskBatchSplit, SplitDriver
 * orchestrates the proposal, apply path persists + fires SplitObserver.
 *
 * <p>Verifies the user-facing split flow works without yet requiring full
 * Store spawn (that's the next wiring step). The parent's in-memory
 * descriptor refresh proves the apply observer fired.
 */
final class SplitDriverE2ETest {

    @TempDir Path baseDir;
    private ClusterHarness harness;
    private ManagedChannel pdChannel;
    private PDGrpc.PDBlockingStub pd;

    @BeforeEach
    void start() throws Exception {
        harness = new ClusterHarness(baseDir, 3).start();
        pdChannel = NettyChannelBuilder.forAddress("127.0.0.1", harness.pdPort())
                .usePlaintext().build();
        pd = PDGrpc.newBlockingStub(pdChannel);
    }

    @AfterEach
    void stop() throws Exception {
        if (pdChannel != null) pdChannel.shutdownNow().awaitTermination(2, TimeUnit.SECONDS);
        if (harness != null) harness.close();
        ClusterHarness.releaseAllPorts();
    }

    @Test
    void splitRegionRpcDrivesEndToEnd() throws Exception {
        var leader = harness.leader();
        var tikv = leader.blockingStub();
        var resp = tikv.splitRegion(io.github.xinfra.lab.xkv.proto.Kvrpcpb.SplitRegionRequest.newBuilder()
                .addSplitKeys(com.google.protobuf.ByteString.copyFromUtf8("p"))
                .build());
        assertThat(resp.hasRegionError())
                .as("split through TikvService.splitRegion succeeds")
                .isFalse();
        assertThat(resp.getRegionsCount()).isEqualTo(2);
        var parent = resp.getRegions(0);
        var child = resp.getRegions(1);
        assertThat(parent.getEndKey().toStringUtf8()).isEqualTo("p");
        assertThat(child.getStartKey().toStringUtf8()).isEqualTo("p");

        // Wait for the child peer to appear locally.
        Awaitility.await().atMost(Duration.ofSeconds(5))
                .pollInterval(Duration.ofMillis(20))
                .until(() -> leader.childPeers.stream()
                        .anyMatch(c -> c.regionId() == child.getId()));
    }

    @Test
    void childPeerSpawnedAfterSplitOwnsChildRange() throws Exception {
        var leader = harness.leader();
        // Pre-split: this store hosts one peer (the parent).
        int beforeChildren = leader.childPeers.size();

        var driver = new SplitDriver(pd, /* proposeTimeoutMs= */ 5_000);
        var resulting = driver.split(leader.peer, List.of("m".getBytes()));

        // Splitter observer spawned a child peer on this store. Wait for
        // the apply to propagate (observer fires synchronously after persist).
        Awaitility.await().atMost(Duration.ofSeconds(5))
                .pollInterval(Duration.ofMillis(20))
                .until(() -> leader.childPeers.size() > beforeChildren);

        var child = resulting.get(1);
        // Store now routes "m" onwards to the child; "a" still to parent.
        var routedForChild = leader.store.peerForKey("noah".getBytes()).orElseThrow();
        assertThat(routedForChild.regionId()).isEqualTo(child.getId());
        var routedForParent = leader.store.peerForKey("alpha".getBytes()).orElseThrow();
        assertThat(routedForParent.regionId()).isEqualTo(leader.peer.regionId());
    }

    @Test
    void splitDriverShrinksParentAndAllocatesChildIds() throws Exception {
        var leader = harness.leader();
        var driver = new SplitDriver(pd, /* proposeTimeoutMs= */ 5_000);

        long beforeEpochVersion = leader.peer.region().getRegionEpoch().getVersion();

        var resulting = driver.split(leader.peer, List.of("m".getBytes()));

        assertThat(resulting).hasSize(2);
        var newParent = resulting.get(0);
        var child = resulting.get(1);

        // Parent shrunk; epoch bumped.
        assertThat(newParent.getEndKey().toStringUtf8()).isEqualTo("m");
        assertThat(newParent.getRegionEpoch().getVersion()).isGreaterThan(beforeEpochVersion);

        // Child has freshly-allocated ID and the right range.
        assertThat(child.getId()).isGreaterThan(newParent.getId());
        assertThat(child.getStartKey().toStringUtf8()).isEqualTo("m");
        assertThat(child.getPeersCount()).isEqualTo(newParent.getPeersCount());
        // Per-child peer IDs distinct from parent's.
        for (int i = 0; i < child.getPeersCount(); i++) {
            assertThat(child.getPeers(i).getId())
                    .as("child peer %d gets fresh ID", i)
                    .isNotEqualTo(newParent.getPeers(i).getId());
        }
    }
}
