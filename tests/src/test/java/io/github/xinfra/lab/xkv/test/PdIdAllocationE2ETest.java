package io.github.xinfra.lab.xkv.test;

import io.github.xinfra.lab.xkv.pd.config.PdConfig;
import io.github.xinfra.lab.xkv.pd.config.PdConfig.PeerAddress;
import io.github.xinfra.lab.xkv.pd.server.PdServer;
import io.github.xinfra.lab.xkv.proto.Metapb;
import io.github.xinfra.lab.xkv.proto.PDGrpc;
import io.github.xinfra.lab.xkv.proto.Pdpb;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.StatusRuntimeException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;

/**
 * Validates ID allocation uniqueness and monotonicity across PD leader changes.
 *
 * <p>Covers:
 * <ul>
 *   <li>allocID returns unique, monotonically-increasing IDs</li>
 *   <li>askSplit/askBatchSplit IDs are unique and raft-replicated</li>
 *   <li>After leader failover, new leader's IDs don't overlap with old leader's</li>
 *   <li>Followers reject allocID/askSplit with UNAVAILABLE</li>
 * </ul>
 */
@Timeout(value = 90, unit = TimeUnit.SECONDS)
final class PdIdAllocationE2ETest {

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
        ClusterHarness.releaseAllPorts();
    }

    @Test
    void idAllocationIsUniqueAndMonotonicAcrossLeaderChanges() throws Exception {
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

        // Wait for leader.
        await().atMost(15, TimeUnit.SECONDS).pollInterval(200, TimeUnit.MILLISECONDS)
                .until(() -> servers.stream().anyMatch(s ->
                        s.raftNode() != null && s.raftNode().isLeader()));

        int leaderIdx = findLeaderIdx();
        var leaderStub = blockingStub(clientPorts[leaderIdx]);

        // Bootstrap a region so askSplit has something to work with.
        var region = Metapb.Region.newBuilder()
                .setId(1)
                .setRegionEpoch(Metapb.RegionEpoch.newBuilder().setConfVer(1).setVersion(1))
                .addPeers(Metapb.Peer.newBuilder().setId(10).setStoreId(100))
                .addPeers(Metapb.Peer.newBuilder().setId(11).setStoreId(101))
                .addPeers(Metapb.Peer.newBuilder().setId(12).setStoreId(102))
                .build();
        leaderStub.bootstrap(Pdpb.BootstrapRequest.newBuilder()
                .setStore(Metapb.Store.newBuilder().setId(100).setAddress("127.0.0.1:9999"))
                .setRegion(region)
                .build());

        Set<Long> allIds = new HashSet<>();
        long maxId = 0;

        // ====== Phase 1: Allocate IDs on initial leader ======

        // allocID x 100
        for (int i = 0; i < 100; i++) {
            var resp = leaderStub.allocID(Pdpb.AllocIDRequest.newBuilder().setCount(1).build());
            assertThat(allIds.add(resp.getId()))
                    .as("allocID returned duplicate id=%d", resp.getId())
                    .isTrue();
            maxId = Math.max(maxId, resp.getId());
        }

        // askSplit x 10 (each returns 1 regionId + 3 peerIds = 4 IDs)
        for (int i = 0; i < 10; i++) {
            var resp = leaderStub.askSplit(Pdpb.AskSplitRequest.newBuilder()
                    .setRegion(region).build());
            assertThat(allIds.add(resp.getNewRegionId()))
                    .as("askSplit returned duplicate regionId=%d", resp.getNewRegionId())
                    .isTrue();
            maxId = Math.max(maxId, resp.getNewRegionId());
            for (long peerId : resp.getNewPeerIdsList()) {
                assertThat(allIds.add(peerId))
                        .as("askSplit returned duplicate peerId=%d", peerId)
                        .isTrue();
                maxId = Math.max(maxId, peerId);
            }
        }

        // askBatchSplit with splitCount=5 (each split = 1 regionId + 3 peerIds)
        var batchResp = leaderStub.askBatchSplit(Pdpb.AskBatchSplitRequest.newBuilder()
                .setRegion(region)
                .setSplitCount(5)
                .build());
        assertThat(batchResp.getIdsCount()).isEqualTo(5);
        for (var splitId : batchResp.getIdsList()) {
            assertThat(allIds.add(splitId.getNewRegionId()))
                    .as("askBatchSplit duplicate regionId=%d", splitId.getNewRegionId())
                    .isTrue();
            maxId = Math.max(maxId, splitId.getNewRegionId());
            for (long peerId : splitId.getNewPeerIdsList()) {
                assertThat(allIds.add(peerId))
                        .as("askBatchSplit duplicate peerId=%d", peerId)
                        .isTrue();
                maxId = Math.max(maxId, peerId);
            }
        }

        long maxIdBeforeFailover = maxId;
        int allocCountBefore = allIds.size();

        // ====== Phase 2: Kill leader, wait for new leader ======

        int stoppedIdx = leaderIdx;
        servers.get(stoppedIdx).stop();

        await().atMost(15, TimeUnit.SECONDS).pollInterval(300, TimeUnit.MILLISECONDS)
                .until(() -> {
                    for (int i = 0; i < 3; i++) {
                        if (i == stoppedIdx) continue;
                        var rn = servers.get(i).raftNode();
                        if (rn != null && rn.isLeader()) return true;
                    }
                    return false;
                });

        int newLeaderIdx = -1;
        for (int i = 0; i < 3; i++) {
            if (i == stoppedIdx) continue;
            var rn = servers.get(i).raftNode();
            if (rn != null && rn.isLeader()) { newLeaderIdx = i; break; }
        }
        assertThat(newLeaderIdx).isGreaterThanOrEqualTo(0);
        var newLeaderStub = blockingStub(clientPorts[newLeaderIdx]);

        // ====== Phase 3: Allocate IDs on new leader ======

        // allocID x 100 on new leader
        for (int i = 0; i < 100; i++) {
            var resp = newLeaderStub.allocID(Pdpb.AllocIDRequest.newBuilder().setCount(1).build());
            assertThat(resp.getId())
                    .as("New leader ID must be > all previous IDs (got %d, max was %d)",
                            resp.getId(), maxIdBeforeFailover)
                    .isGreaterThan(maxIdBeforeFailover);
            assertThat(allIds.add(resp.getId()))
                    .as("allocID on new leader returned duplicate id=%d", resp.getId())
                    .isTrue();
            maxId = Math.max(maxId, resp.getId());
        }

        // askSplit x 10 on new leader
        for (int i = 0; i < 10; i++) {
            var resp = newLeaderStub.askSplit(Pdpb.AskSplitRequest.newBuilder()
                    .setRegion(region).build());
            assertThat(resp.getNewRegionId()).isGreaterThan(maxIdBeforeFailover);
            assertThat(allIds.add(resp.getNewRegionId())).isTrue();
            for (long peerId : resp.getNewPeerIdsList()) {
                assertThat(peerId).isGreaterThan(maxIdBeforeFailover);
                assertThat(allIds.add(peerId)).isTrue();
            }
        }

        // Total uniqueness: no duplicates across all allocations.
        int totalExpected = allocCountBefore + 100 + 10 * 4;
        assertThat(allIds).hasSize(totalExpected);

        // ====== Phase 4: Follower rejection ======

        int followerIdx = -1;
        for (int i = 0; i < 3; i++) {
            if (i == stoppedIdx || i == newLeaderIdx) continue;
            followerIdx = i;
            break;
        }
        assertThat(followerIdx).isGreaterThanOrEqualTo(0);
        var followerStub = blockingStub(clientPorts[followerIdx]);

        assertThatThrownBy(() -> followerStub.allocID(
                Pdpb.AllocIDRequest.newBuilder().setCount(1).build()))
                .isInstanceOf(StatusRuntimeException.class)
                .hasMessageContaining("UNAVAILABLE");

        assertThatThrownBy(() -> followerStub.askSplit(
                Pdpb.AskSplitRequest.newBuilder().setRegion(region).build()))
                .isInstanceOf(StatusRuntimeException.class)
                .hasMessageContaining("UNAVAILABLE");

        assertThatThrownBy(() -> followerStub.askBatchSplit(
                Pdpb.AskBatchSplitRequest.newBuilder()
                        .setRegion(region).setSplitCount(1).build()))
                .isInstanceOf(StatusRuntimeException.class)
                .hasMessageContaining("UNAVAILABLE");
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
