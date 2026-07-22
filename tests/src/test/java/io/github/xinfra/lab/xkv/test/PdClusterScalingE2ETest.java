package io.github.xinfra.lab.xkv.test;

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
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * Validates PD cluster dynamic scaling: 2-node cluster → 3 nodes via addMember.
 *
 * <p>Exercises the {@code addMember} RPC and {@code joinMode} workflow:
 * a 2-node PD cluster grows to 3 by adding a member dynamically,
 * verifying that the raft group, ID allocation, and leader failover
 * work correctly after scaling.
 *
 * <p>Note: We start with 2 nodes instead of 1 because x-raft-lib's
 * checkQuorum has a known issue with single-node clusters (leader
 * doesn't count itself as active and periodically steps down).
 */
@Timeout(value = 120, unit = TimeUnit.SECONDS)
final class PdClusterScalingE2ETest {

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
    void scaleFromTwoToThreeNodesAndFailover() throws Exception {
        int[] clientPorts = new int[3];
        int[] raftPorts = new int[3];
        for (int i = 0; i < 3; i++) {
            clientPorts[i] = TestCluster.freePort();
            raftPorts[i] = TestCluster.freePort();
        }

        // ====== Phase 1: Start 2-node PD cluster ======

        var initialPeers = List.of(
                new PeerAddress(1, "127.0.0.1:" + raftPorts[0], "127.0.0.1:" + clientPorts[0]),
                new PeerAddress(2, "127.0.0.1:" + raftPorts[1], "127.0.0.1:" + clientPorts[1])
        );

        for (int i = 0; i < 2; i++) {
            var cfg = PdConfig.builder()
                    .nodeId(i + 1)
                    .clusterId(1)
                    .clientAddress("127.0.0.1:" + clientPorts[i])
                    .raftAddress("127.0.0.1:" + raftPorts[i])
                    .dataDir(tempDir.resolve("pd-" + (i + 1)))
                    .peers(initialPeers)
                    .build();
            TestCluster.releasePort(clientPorts[i]);
            TestCluster.releasePort(raftPorts[i]);
            var srv = new PdServer(cfg);
            srv.start();
            servers.add(srv);
        }

        // Wait for leader.
        await().atMost(15, TimeUnit.SECONDS).pollInterval(200, TimeUnit.MILLISECONDS)
                .until(() -> servers.stream().anyMatch(s ->
                        s.raftNode() != null && s.raftNode().isLeader()));

        int leaderIdx = findLeaderIdx();
        var leaderStub = blockingStub(clientPorts[leaderIdx]);

        // Verify 2-node cluster: getMembers returns 2.
        var members = leaderStub.getMembers(Pdpb.GetMembersRequest.newBuilder().build());
        assertThat(members.getMembersCount()).isEqualTo(2);

        // Bootstrap and allocate some IDs.
        leaderStub.bootstrap(Pdpb.BootstrapRequest.newBuilder()
                .setStore(Metapb.Store.newBuilder().setId(100).setAddress("127.0.0.1:9999"))
                .setRegion(Metapb.Region.newBuilder().setId(1)
                        .setRegionEpoch(Metapb.RegionEpoch.newBuilder().setConfVer(1).setVersion(1))
                        .addPeers(Metapb.Peer.newBuilder().setId(101).setStoreId(100)))
                .build());

        var allocResp1 = leaderStub.allocID(Pdpb.AllocIDRequest.newBuilder().setCount(10).build());
        assertThat(allocResp1.getId()).isGreaterThan(0);
        long maxIdPhase1 = allocResp1.getId() + allocResp1.getCount() - 1;

        // ====== Phase 2: Add 3rd node via addMember + joinMode ======

        TestCluster.releasePort(clientPorts[2]);
        TestCluster.releasePort(raftPorts[2]);

        var addMemberResp = leaderStub.addMember(Pdpb.AddMemberRequest.newBuilder()
                .setMember(Pdpb.Member.newBuilder()
                        .setMemberId(3)
                        .setName("pd-3")
                        .addClientUrls("127.0.0.1:" + clientPorts[2])
                        .addPeerUrls("127.0.0.1:" + raftPorts[2]))
                .setRaftUrl("127.0.0.1:" + raftPorts[2])
                .build());
        assertThat(addMemberResp.getMembersCount()).isEqualTo(3);

        // Start PD node 3 with joinMode=true.
        var node3Peers = List.of(
                new PeerAddress(1, "127.0.0.1:" + raftPorts[0], "127.0.0.1:" + clientPorts[0]),
                new PeerAddress(2, "127.0.0.1:" + raftPorts[1], "127.0.0.1:" + clientPorts[1]),
                new PeerAddress(3, "127.0.0.1:" + raftPorts[2], "127.0.0.1:" + clientPorts[2])
        );
        var cfg3 = PdConfig.builder()
                .nodeId(3)
                .clusterId(1)
                .clientAddress("127.0.0.1:" + clientPorts[2])
                .raftAddress("127.0.0.1:" + raftPorts[2])
                .dataDir(tempDir.resolve("pd-3"))
                .peers(node3Peers)
                .joinMode(true)
                .build();
        var srv3 = new PdServer(cfg3);
        srv3.start();
        servers.add(srv3);

        // Wait for node 3 to sync via raft replication.
        await().atMost(20, TimeUnit.SECONDS).pollInterval(300, TimeUnit.MILLISECONDS)
                .until(() -> srv3.raftNode() != null);

        // Verify getMembers now returns 3 on the leader.
        await().atMost(10, TimeUnit.SECONDS).pollInterval(200, TimeUnit.MILLISECONDS)
                .until(() -> {
                    try {
                        var resp = leaderStub.getMembers(Pdpb.GetMembersRequest.newBuilder().build());
                        return resp.getMembersCount() == 3;
                    } catch (Exception e) { return false; }
                });

        // allocID still produces monotonically increasing IDs.
        var allocResp2 = leaderStub.allocID(Pdpb.AllocIDRequest.newBuilder().setCount(10).build());
        assertThat(allocResp2.getId()).isGreaterThan(maxIdPhase1);
        long maxIdPhase2 = allocResp2.getId() + allocResp2.getCount() - 1;

        // Verify isBootstrapped propagated to the 3-node cluster.
        var bootResp = leaderStub.isBootstrapped(Pdpb.IsBootstrappedRequest.newBuilder().build());
        assertThat(bootResp.getBootstrapped()).isTrue();

        // ====== Phase 3: Kill leader, verify failover with 3 nodes ======

        int stoppedIdx = leaderIdx;
        servers.get(stoppedIdx).stop();

        // New leader must come from the 2 surviving nodes.
        await().atMost(15, TimeUnit.SECONDS).pollInterval(300, TimeUnit.MILLISECONDS)
                .until(() -> {
                    for (int i = 0; i < servers.size(); i++) {
                        if (i == stoppedIdx) continue;
                        var rn = servers.get(i).raftNode();
                        if (rn != null && rn.isLeader()) return true;
                    }
                    return false;
                });

        int newLeaderIdx = -1;
        for (int i = 0; i < servers.size(); i++) {
            if (i == stoppedIdx) continue;
            var rn = servers.get(i).raftNode();
            if (rn != null && rn.isLeader()) { newLeaderIdx = i; break; }
        }
        assertThat(newLeaderIdx).isGreaterThanOrEqualTo(0);
        var newLeaderStub = blockingStub(clientPorts[newLeaderIdx]);

        // Verify getMembers still returns 3 (including the stopped node).
        var membersAfterFailover = newLeaderStub.getMembers(Pdpb.GetMembersRequest.newBuilder().build());
        assertThat(membersAfterFailover.getMembersCount()).isEqualTo(3);

        // allocID on new leader must produce IDs > all previous.
        var allocResp3 = newLeaderStub.allocID(Pdpb.AllocIDRequest.newBuilder().setCount(10).build());
        assertThat(allocResp3.getId())
                .as("IDs after failover must be > max previous (%d)", maxIdPhase2)
                .isGreaterThan(maxIdPhase2);

        // Verify isBootstrapped persisted across failover.
        var bootAfter = newLeaderStub.isBootstrapped(Pdpb.IsBootstrappedRequest.newBuilder().build());
        assertThat(bootAfter.getBootstrapped()).isTrue();

        // ====== Phase 4: Verify total ID uniqueness ======

        var allIds = new HashSet<Long>();
        addRange(allIds, allocResp1.getId(), allocResp1.getCount());
        addRange(allIds, allocResp2.getId(), allocResp2.getCount());
        addRange(allIds, allocResp3.getId(), allocResp3.getCount());
        assertThat(allIds).hasSize(30);
    }

    private static void addRange(HashSet<Long> set, long base, int count) {
        for (long i = base; i < base + count; i++) {
            assertThat(set.add(i)).as("duplicate ID: %d", i).isTrue();
        }
    }

    private int findLeaderIdx() {
        for (int i = 0; i < servers.size(); i++) {
            var rn = servers.get(i).raftNode();
            if (rn != null && rn.isLeader()) return i;
        }
        throw new IllegalStateException("no leader found");
    }

    private PDGrpc.PDBlockingStub blockingStub(int port) {
        var ch = ManagedChannelBuilder.forAddress("127.0.0.1", port)
                .usePlaintext().build();
        channels.add(ch);
        return PDGrpc.newBlockingStub(ch);
    }
}
