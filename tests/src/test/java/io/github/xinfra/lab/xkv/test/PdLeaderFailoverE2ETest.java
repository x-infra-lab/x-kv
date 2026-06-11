package io.github.xinfra.lab.xkv.test;

import io.github.xinfra.lab.xkv.client.pd.PdClient;
import io.github.xinfra.lab.xkv.pd.config.PdConfig;
import io.github.xinfra.lab.xkv.pd.config.PdConfig.PeerAddress;
import io.github.xinfra.lab.xkv.pd.server.PdServer;
import io.github.xinfra.lab.xkv.proto.PDGrpc;
import io.github.xinfra.lab.xkv.proto.Pdpb;
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
 * Validates the client-side {@link PdClient} leader discovery and failover.
 *
 * <p>Boots a 3-node PD raft cluster, creates a {@link PdClient} pointing at
 * all 3 endpoints, verifies it connects to the leader, then stops the leader
 * and verifies that {@link PdClient#switchLeader()} reconnects to the new
 * leader.
 */
@Timeout(value = 90, unit = TimeUnit.SECONDS)
final class PdLeaderFailoverE2ETest {

    @TempDir Path tempDir;
    private final List<PdServer> servers = new ArrayList<>();
    private PdClient pdClient;

    @AfterEach
    void teardown() {
        if (pdClient != null) {
            try { pdClient.close(); } catch (Throwable ignored) {}
        }
        for (var s : servers) {
            try { s.stop(); } catch (Exception ignored) {}
        }
        servers.clear();
        ClusterHarness.releaseAllPorts();
    }

    @Test
    void clientDiscoversLeaderAndFailsOver() throws Exception {
        int[] clientPorts = new int[3];
        int[] raftPorts = new int[3];
        for (int i = 0; i < 3; i++) {
            clientPorts[i] = ClusterHarness.freePort();
            raftPorts[i] = ClusterHarness.freePort();
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
            ClusterHarness.releasePort(clientPorts[i]);
            ClusterHarness.releasePort(raftPorts[i]);
            var srv = new PdServer(cfg);
            srv.start();
            servers.add(srv);
        }

        // Wait for a leader to emerge.
        await().atMost(15, TimeUnit.SECONDS).pollInterval(200, TimeUnit.MILLISECONDS)
                .until(() -> servers.stream().anyMatch(s ->
                        s.raftNode() != null && s.raftNode().isLeader()));

        // Create PdClient with all 3 endpoints.
        var endpoints = List.of(
                "127.0.0.1:" + clientPorts[0],
                "127.0.0.1:" + clientPorts[1],
                "127.0.0.1:" + clientPorts[2]
        );
        pdClient = new PdClient(endpoints);

        // PdClient should have connected to the current leader.
        assertThat(pdClient.leaderChannel()).isNotNull();
        assertThat(pdClient.blockingStub()).isNotNull();

        // Verify getMembers works through PdClient's stub.
        var membersResp = pdClient.blockingStub().getMembers(
                Pdpb.GetMembersRequest.newBuilder().build());
        assertThat(membersResp.getMembersCount()).isEqualTo(3);
        assertThat(membersResp.hasLeader()).isTrue();
        assertThat(membersResp.getLeader().getClientUrlsCount()).isGreaterThan(0);

        // Wait for PdClient to converge on the actual raft leader — the
        // getMembers response on a follower may briefly report a stale leader
        // right after election.
        await().atMost(30, TimeUnit.SECONDS).pollInterval(200, TimeUnit.MILLISECONDS)
                .until(() -> {
                    pdClient.switchLeader();
                    String addr = pdClient.leaderAddress();
                    for (int i = 0; i < 3; i++) {
                        var rn = servers.get(i).raftNode();
                        if (rn != null && rn.isLeader()) {
                            return ("127.0.0.1:" + clientPorts[i]).equals(addr);
                        }
                    }
                    return false;
                });

        int leaderIdx = -1;
        for (int i = 0; i < 3; i++) {
            var rn = servers.get(i).raftNode();
            if (rn != null && rn.isLeader()) {
                leaderIdx = i;
                break;
            }
        }
        assertThat(leaderIdx).isGreaterThanOrEqualTo(0);

        String oldLeaderAddr = "127.0.0.1:" + clientPorts[leaderIdx];
        assertThat(pdClient.leaderAddress()).isEqualTo(oldLeaderAddr);

        // Stop the leader.
        int stoppedIdx = leaderIdx;
        servers.get(stoppedIdx).stop();

        // Wait for a new leader to emerge among the remaining 2 nodes.
        await().atMost(15, TimeUnit.SECONDS).pollInterval(300, TimeUnit.MILLISECONDS)
                .until(() -> {
                    for (int i = 0; i < 3; i++) {
                        if (i == stoppedIdx) continue;
                        var rn = servers.get(i).raftNode();
                        if (rn != null && rn.isLeader()) return true;
                    }
                    return false;
                });

        // Wait for PdClient to discover and converge on the new leader.
        final int stopped = stoppedIdx;
        await().atMost(10, TimeUnit.SECONDS).pollInterval(300, TimeUnit.MILLISECONDS)
                .until(() -> {
                    pdClient.switchLeader();
                    String addr = pdClient.leaderAddress();
                    if (oldLeaderAddr.equals(addr)) return false;
                    for (int i = 0; i < 3; i++) {
                        if (i == stopped) continue;
                        var rn = servers.get(i).raftNode();
                        if (rn != null && rn.isLeader()) {
                            return ("127.0.0.1:" + clientPorts[i]).equals(addr);
                        }
                    }
                    return false;
                });

        // The PdClient should now point to the new leader, not the stopped one.
        assertThat(pdClient.leaderAddress()).isNotEqualTo(oldLeaderAddr);

        // Verify PdClient can still serve RPCs through the new leader.
        var membersResp2 = pdClient.blockingStub().getMembers(
                Pdpb.GetMembersRequest.newBuilder().build());
        assertThat(membersResp2.getMembersCount()).isEqualTo(3);
        assertThat(membersResp2.hasLeader()).isTrue();

        // The new leader should be one of the surviving nodes.
        int newLeaderIdx = -1;
        for (int i = 0; i < 3; i++) {
            if (i == stoppedIdx) continue;
            var rn = servers.get(i).raftNode();
            if (rn != null && rn.isLeader()) {
                newLeaderIdx = i;
                break;
            }
        }
        assertThat(newLeaderIdx).isGreaterThanOrEqualTo(0).isNotEqualTo(stoppedIdx);
        String newLeaderAddr = "127.0.0.1:" + clientPorts[newLeaderIdx];
        assertThat(pdClient.leaderAddress()).isEqualTo(newLeaderAddr);
    }

    @Test
    void getMembersReturnsAllMembersWithClientUrls() throws Exception {
        int[] clientPorts = new int[3];
        int[] raftPorts = new int[3];
        for (int i = 0; i < 3; i++) {
            clientPorts[i] = ClusterHarness.freePort();
            raftPorts[i] = ClusterHarness.freePort();
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
                    .dataDir(tempDir.resolve("pd-m-" + (i + 1)))
                    .peers(peers)
                    .build();
            ClusterHarness.releasePort(clientPorts[i]);
            ClusterHarness.releasePort(raftPorts[i]);
            var srv = new PdServer(cfg);
            srv.start();
            servers.add(srv);
        }

        // Wait for leader.
        await().atMost(15, TimeUnit.SECONDS).pollInterval(200, TimeUnit.MILLISECONDS)
                .until(() -> servers.stream().anyMatch(s ->
                        s.raftNode() != null && s.raftNode().isLeader()));

        // Query getMembers from each node.
        for (int i = 0; i < 3; i++) {
            var ch = io.grpc.ManagedChannelBuilder
                    .forAddress("127.0.0.1", clientPorts[i])
                    .usePlaintext()
                    .build();
            try {
                var stub = PDGrpc.newBlockingStub(ch);
                var resp = stub.getMembers(Pdpb.GetMembersRequest.newBuilder().build());

                assertThat(resp.getMembersCount())
                        .as("node %d should return all 3 members", i + 1)
                        .isEqualTo(3);

                // Each member should have a client URL.
                for (var member : resp.getMembersList()) {
                    assertThat(member.getClientUrlsCount())
                            .as("member %d should have client URL", member.getMemberId())
                            .isGreaterThan(0);
                }

                // Leader should be set and have a client URL.
                assertThat(resp.hasLeader()).isTrue();
                assertThat(resp.getLeader().getClientUrlsCount()).isGreaterThan(0);
            } finally {
                ch.shutdownNow().awaitTermination(2, TimeUnit.SECONDS);
            }
        }
    }

}
