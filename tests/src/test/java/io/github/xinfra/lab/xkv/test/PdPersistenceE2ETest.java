package io.github.xinfra.lab.xkv.test;

import com.google.protobuf.ByteString;
import io.github.xinfra.lab.xkv.pd.config.PdConfig;
import io.github.xinfra.lab.xkv.pd.server.PdServer;
import io.github.xinfra.lab.xkv.proto.Metapb;
import io.github.xinfra.lab.xkv.proto.PDGrpc;
import io.github.xinfra.lab.xkv.proto.Pdpb;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * Validates PD raft persistence: state survives a full server restart.
 *
 * <p>Uses a 3-node PD cluster with RocksDbStorage. Bootstraps the cluster,
 * mutates state, stops all nodes, restarts them, and verifies that all
 * state was restored from the persistent raft log + snapshot.
 */
@Timeout(value = 90, unit = TimeUnit.SECONDS)
final class PdPersistenceE2ETest {

    @TempDir Path tempDir;
    private final List<PdServer> servers = new ArrayList<>();
    private final List<ManagedChannel> channels = new ArrayList<>();

    @AfterEach
    void teardown() {
        for (var ch : channels) {
            try { ch.shutdownNow().awaitTermination(2, TimeUnit.SECONDS); } catch (Exception e) { e.printStackTrace(); }
        }
        channels.clear();
        for (var s : servers) {
            try { s.stop(); } catch (Exception e) { e.printStackTrace(); }
        }
        servers.clear();
        TestCluster.releaseAllPorts();
    }

    @Test
    void pdStateRestoredAfterFullClusterRestart() throws Exception {
        int[] clientPorts = new int[3];
        int[] raftPorts = new int[3];
        for (int i = 0; i < 3; i++) {
            clientPorts[i] = TestCluster.freePort();
            raftPorts[i] = TestCluster.freePort();
        }

        var peers = List.of(
                new PdConfig.PeerAddress(1, "127.0.0.1:" + raftPorts[0], "127.0.0.1:" + clientPorts[0]),
                new PdConfig.PeerAddress(2, "127.0.0.1:" + raftPorts[1], "127.0.0.1:" + clientPorts[1]),
                new PdConfig.PeerAddress(3, "127.0.0.1:" + raftPorts[2], "127.0.0.1:" + clientPorts[2])
        );

        // ---- Phase 1: Boot cluster and mutate state ----
        for (int i = 0; i < 3; i++) {
            startNode(i, clientPorts[i], raftPorts[i], peers);
        }
        waitForLeader();
        int leaderIdx = findLeader();

        var stub = stubs()[leaderIdx];

        // Bootstrap.
        var store = Metapb.Store.newBuilder()
                .setId(10).setAddress("127.0.0.1:20160")
                .setPeerAddress("127.0.0.1:20170")
                .setState(Metapb.StoreState.Up)
                .build();
        var region = Metapb.Region.newBuilder()
                .setId(1)
                .setStartKey(ByteString.EMPTY)
                .setEndKey(ByteString.EMPTY)
                .setRegionEpoch(Metapb.RegionEpoch.newBuilder().setConfVer(1).setVersion(1))
                .addPeers(Metapb.Peer.newBuilder().setId(1).setStoreId(10).setRole(Metapb.PeerRole.Voter))
                .build();

        stub.bootstrap(Pdpb.BootstrapRequest.newBuilder()
                .setStore(store).setRegion(region).build());

        // Add a second store.
        var store2 = Metapb.Store.newBuilder()
                .setId(20).setAddress("127.0.0.1:20161")
                .setPeerAddress("127.0.0.1:20171")
                .setState(Metapb.StoreState.Up)
                .build();
        stub.putStore(Pdpb.PutStoreRequest.newBuilder().setStore(store2).build());

        // Add a third store to exercise more mutations.
        var store3 = Metapb.Store.newBuilder()
                .setId(30).setAddress("127.0.0.1:20162")
                .setPeerAddress("127.0.0.1:20172")
                .setState(Metapb.StoreState.Up)
                .build();
        stub.putStore(Pdpb.PutStoreRequest.newBuilder().setStore(store3).build());

        // Allocate IDs to advance the allocator.
        var allocResp = stub.allocID(Pdpb.AllocIDRequest.newBuilder().setCount(100).build());
        long allocatedBase = allocResp.getId();

        // Wait for replication to all nodes.
        await().atMost(10, TimeUnit.SECONDS).pollInterval(200, TimeUnit.MILLISECONDS)
                .untilAsserted(() -> {
                    for (var s : servers) {
                        assertThat(s.state().isBootstrapped()).isTrue();
                        assertThat(s.state().getStore(20)).isPresent();
                        assertThat(s.state().getStore(30)).isPresent();
                    }
                });

        // ---- Phase 2: Stop all nodes ----
        for (var ch : channels) {
            try { ch.shutdownNow().awaitTermination(2, TimeUnit.SECONDS); } catch (Exception e) { e.printStackTrace(); }
        }
        channels.clear();
        for (var s : servers) s.stop();
        servers.clear();

        // ---- Phase 3: Restart all nodes with same dataDirs ----
        // Re-use the same ports so raft transport reconnects work.
        for (int i = 0; i < 3; i++) {
            startNode(i, clientPorts[i], raftPorts[i], peers);
        }
        waitForLeader();
        int newLeader = findLeader();

        // ---- Phase 4: Verify all state was restored ----
        var newStub = stubs()[newLeader];

        // Cluster should be bootstrapped.
        assertThat(servers.get(newLeader).state().isBootstrapped()).isTrue();

        // All three stores should be present.
        assertThat(servers.get(newLeader).state().getStore(10)).isPresent();
        assertThat(servers.get(newLeader).state().getStore(20)).isPresent();
        assertThat(servers.get(newLeader).state().getStore(30)).isPresent();

        // Bootstrap region should be present.
        assertThat(servers.get(newLeader).state().getRegion(1)).isPresent();

        // ID allocator should be at or past what was allocated before.
        var allocResp2 = newStub.allocID(Pdpb.AllocIDRequest.newBuilder().setCount(1).build());
        assertThat(allocResp2.getId()).isGreaterThanOrEqualTo(allocatedBase + 100);

        // All followers should also have restored state.
        await().atMost(10, TimeUnit.SECONDS).pollInterval(200, TimeUnit.MILLISECONDS)
                .untilAsserted(() -> {
                    for (var s : servers) {
                        assertThat(s.state().isBootstrapped())
                                .as("all nodes should be bootstrapped after restart")
                                .isTrue();
                        assertThat(s.state().getStore(10)).isPresent();
                        assertThat(s.state().getStore(20)).isPresent();
                        assertThat(s.state().getStore(30)).isPresent();
                    }
                });

        // A new mutation should work after restart.
        var store4 = Metapb.Store.newBuilder()
                .setId(40).setAddress("127.0.0.1:20163")
                .setState(Metapb.StoreState.Up)
                .build();
        var putResp = newStub.putStore(Pdpb.PutStoreRequest.newBuilder().setStore(store4).build());
        assertThat(putResp.getHeader().hasError()).isFalse();

        await().atMost(10, TimeUnit.SECONDS).pollInterval(200, TimeUnit.MILLISECONDS)
                .untilAsserted(() -> {
                    for (var s : servers) {
                        assertThat(s.state().getStore(40))
                                .as("store 40 should replicate after restart")
                                .isPresent();
                    }
                });
    }

    private void startNode(int idx, int clientPort, int raftPort,
                           List<PdConfig.PeerAddress> peers) throws Exception {
        var cfg = PdConfig.builder()
                .nodeId(idx + 1)
                .clusterId(1)
                .clientAddress("127.0.0.1:" + clientPort)
                .raftAddress("127.0.0.1:" + raftPort)
                .dataDir(tempDir.resolve("pd-" + (idx + 1)))
                .peers(peers)
                .build();
        TestCluster.releasePort(clientPort);
        TestCluster.releasePort(raftPort);
        var srv = new PdServer(cfg);
        srv.start();
        servers.add(srv);

        var ch = ManagedChannelBuilder
                .forAddress("127.0.0.1", clientPort)
                .usePlaintext()
                .build();
        channels.add(ch);
    }

    private PDGrpc.PDBlockingStub[] stubs() {
        var stubs = new PDGrpc.PDBlockingStub[channels.size()];
        for (int i = 0; i < channels.size(); i++) {
            stubs[i] = PDGrpc.newBlockingStub(channels.get(i));
        }
        return stubs;
    }

    private void waitForLeader() {
        await().atMost(15, TimeUnit.SECONDS).pollInterval(200, TimeUnit.MILLISECONDS)
                .until(() -> servers.stream().anyMatch(s ->
                        s.raftNode() != null && s.raftNode().isLeader()));
    }

    private int findLeader() {
        for (int i = 0; i < servers.size(); i++) {
            var rn = servers.get(i).raftNode();
            if (rn != null && rn.isLeader()) return i;
        }
        return -1;
    }

    private static int freePort() throws Exception {
        return TestCluster.freePort();
    }
}
