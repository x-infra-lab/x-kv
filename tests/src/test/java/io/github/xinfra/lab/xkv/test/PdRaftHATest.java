package io.github.xinfra.lab.xkv.test;

import com.google.protobuf.ByteString;
import io.github.xinfra.lab.xkv.pd.config.PdConfig;
import io.github.xinfra.lab.xkv.pd.config.PdConfig.PeerAddress;
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
 * Integration test: 3-node PD raft cluster.
 *
 * Verifies:
 * 1. Leader election converges
 * 2. Mutations (bootstrap, putStore) replicate to followers
 * 3. Leader failover: stop the leader, remaining 2 elect a new one,
 *    reads on followers reflect prior writes
 */
@Timeout(value = 60, unit = TimeUnit.SECONDS)
final class PdRaftHATest {

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
    void threeNodePdClusterReplicatesAndSurvivesLeaderLoss() throws Exception {
        int[] clientPorts = new int[3];
        int[] raftPorts = new int[3];
        for (int i = 0; i < 3; i++) {
            clientPorts[i] = TestCluster.freePort();
            raftPorts[i] = TestCluster.freePort();
        }

        var peers = List.of(
                new PeerAddress(1, "127.0.0.1:" + raftPorts[0], "127.0.0.1:" + clientPorts[0]),
                new PeerAddress(2, "127.0.0.1:" + raftPorts[1], "127.0.0.1:" + clientPorts[1]),
                new PeerAddress(3, "127.0.0.1:" + raftPorts[2], "127.0.0.1:" + clientPorts[2])
        );

        for (int i = 0; i < 3; i++) {
            var cfg = PdConfig.builder()
                    .nodeId(i + 1)
                    .clusterId(1)
                    .clientAddress("127.0.0.1:" + clientPorts[i])
                    .raftAddress("127.0.0.1:" + raftPorts[i])
                    .dataDir(tempDir.resolve("pd-" + (i + 1)))
                    .peers(peers)
                    .build();
            TestCluster.releasePort(clientPorts[i]);
            TestCluster.releasePort(raftPorts[i]);
            var srv = new PdServer(cfg);
            srv.start();
            servers.add(srv);
        }

        // Connect gRPC stubs to all 3 nodes.
        PDGrpc.PDBlockingStub[] stubs = new PDGrpc.PDBlockingStub[3];
        for (int i = 0; i < 3; i++) {
            var ch = ManagedChannelBuilder
                    .forAddress("127.0.0.1", clientPorts[i])
                    .usePlaintext()
                    .build();
            channels.add(ch);
            stubs[i] = PDGrpc.newBlockingStub(ch);
        }

        // Wait for a leader to emerge by polling isBootstrapped on each node.
        // The leader's raftNode.isLeader() must be true before proposals work.
        await().atMost(15, TimeUnit.SECONDS).pollInterval(200, TimeUnit.MILLISECONDS)
                .until(() -> {
                    for (int i = 0; i < 3; i++) {
                        if (servers.get(i).raftNode() != null && servers.get(i).raftNode().isLeader()) {
                            return true;
                        }
                    }
                    return false;
                });

        // Find the leader index.
        int leaderIdx = findLeader();
        assertThat(leaderIdx).isGreaterThanOrEqualTo(0);

        // Bootstrap through the leader.
        var store = Metapb.Store.newBuilder().setId(10).setAddress("127.0.0.1:20160").build();
        var region = Metapb.Region.newBuilder()
                .setId(1)
                .setStartKey(ByteString.EMPTY)
                .setEndKey(ByteString.EMPTY)
                .setRegionEpoch(Metapb.RegionEpoch.newBuilder().setConfVer(1).setVersion(1))
                .addPeers(Metapb.Peer.newBuilder().setId(1).setStoreId(10).setRole(Metapb.PeerRole.Voter))
                .build();

        var bootstrapResp = stubs[leaderIdx].bootstrap(Pdpb.BootstrapRequest.newBuilder()
                .setHeader(Pdpb.RequestHeader.newBuilder().setClusterId(1))
                .setStore(store)
                .setRegion(region)
                .build());
        assertThat(bootstrapResp.getHeader().hasError()).isFalse();

        // Verify all 3 nodes see the bootstrap (replicated via raft).
        await().atMost(10, TimeUnit.SECONDS).pollInterval(200, TimeUnit.MILLISECONDS)
                .untilAsserted(() -> {
                    for (int i = 0; i < 3; i++) {
                        assertThat(servers.get(i).state().isBootstrapped())
                                .as("node %d should be bootstrapped", i + 1)
                                .isTrue();
                    }
                });

        // PutStore through leader and verify replication.
        var store2 = Metapb.Store.newBuilder().setId(20).setAddress("127.0.0.1:20161").build();
        var putStoreResp = stubs[leaderIdx].putStore(Pdpb.PutStoreRequest.newBuilder()
                .setHeader(Pdpb.RequestHeader.newBuilder().setClusterId(1))
                .setStore(store2)
                .build());
        assertThat(putStoreResp.getHeader().hasError()).isFalse();

        await().atMost(10, TimeUnit.SECONDS).pollInterval(200, TimeUnit.MILLISECONDS)
                .untilAsserted(() -> {
                    for (int i = 0; i < 3; i++) {
                        assertThat(servers.get(i).state().getStore(20))
                                .as("node %d should have store 20", i + 1)
                                .isPresent();
                    }
                });

        // ---- Leader failover ----
        // Stop the current leader.
        int oldLeader = leaderIdx;
        servers.get(oldLeader).stop();

        // Wait for one of the remaining two to become leader.
        await().atMost(15, TimeUnit.SECONDS).pollInterval(300, TimeUnit.MILLISECONDS)
                .until(() -> {
                    for (int i = 0; i < 3; i++) {
                        if (i == oldLeader) continue;
                        var rn = servers.get(i).raftNode();
                        if (rn != null && rn.isLeader()) return true;
                    }
                    return false;
                });

        int newLeader = -1;
        for (int i = 0; i < 3; i++) {
            if (i == oldLeader) continue;
            var rn = servers.get(i).raftNode();
            if (rn != null && rn.isLeader()) {
                newLeader = i;
                break;
            }
        }
        assertThat(newLeader).isNotEqualTo(oldLeader).isGreaterThanOrEqualTo(0);

        // The new leader should still see all replicated state.
        assertThat(servers.get(newLeader).state().isBootstrapped()).isTrue();
        assertThat(servers.get(newLeader).state().getStore(10)).isPresent();
        assertThat(servers.get(newLeader).state().getStore(20)).isPresent();
        assertThat(servers.get(newLeader).state().regionCount()).isEqualTo(1);

        // A new mutation through the new leader should work.
        var store3 = Metapb.Store.newBuilder().setId(30).setAddress("127.0.0.1:20162").build();
        var putResp2 = stubs[newLeader].putStore(Pdpb.PutStoreRequest.newBuilder()
                .setHeader(Pdpb.RequestHeader.newBuilder().setClusterId(1))
                .setStore(store3)
                .build());
        assertThat(putResp2.getHeader().hasError()).isFalse();

        // The surviving follower should also see store 30.
        int followerIdx = -1;
        for (int i = 0; i < 3; i++) {
            if (i != oldLeader && i != newLeader) { followerIdx = i; break; }
        }
        int finalFollower = followerIdx;
        await().atMost(10, TimeUnit.SECONDS).pollInterval(200, TimeUnit.MILLISECONDS)
                .untilAsserted(() ->
                        assertThat(servers.get(finalFollower).state().getStore(30))
                                .as("follower should replicate store 30")
                                .isPresent());
    }

    private int findLeader() {
        for (int i = 0; i < servers.size(); i++) {
            var rn = servers.get(i).raftNode();
            if (rn != null && rn.isLeader()) return i;
        }
        return -1;
    }

}
