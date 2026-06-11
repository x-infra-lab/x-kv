package io.github.xinfra.lab.xkv.test;

import io.github.xinfra.lab.xkv.proto.Metapb;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Verifies that PD-scheduled operators reach the leader peer via the
 * region heartbeat stream — the channel TiKV uses for change-peer /
 * transfer-leader / split / merge.
 *
 * <p>For now only transfer-leader is plumbed end-to-end (the smallest
 * operator). Change-peer / split / merge are gated on the multi-region
 * scheduler work.
 */
final class PdOperatorDispatchTest {

    @TempDir Path baseDir;
    private ClusterHarness harness;

    @BeforeEach
    void start() throws Exception {
        harness = new ClusterHarness(baseDir, 3).start();
    }

    @AfterEach
    void stop() throws Exception {
        if (harness != null) harness.close();
    }

    @Test
    void pdChangePeerOperatorAddsLearnerToRegion() throws Exception {
        var leader = harness.leader();
        // Allocate a fresh peer ID via PD, then enqueue AddLearnerNode
        // (learners don't change quorum, so this works in a 3-peer cluster
        // even though the new "peer" isn't actually running).
        var pdChannel = io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder
                .forAddress("127.0.0.1", harness.pdPort()).usePlaintext().build();
        long newPeerId;
        try {
            var pd = io.github.xinfra.lab.xkv.proto.PDGrpc.newBlockingStub(pdChannel);
            newPeerId = pd.allocID(io.github.xinfra.lab.xkv.proto.Pdpb.AllocIDRequest
                    .newBuilder().setCount(1).build()).getId();
        } finally {
            pdChannel.shutdownNow().awaitTermination(2, java.util.concurrent.TimeUnit.SECONDS);
        }
        var newPeer = io.github.xinfra.lab.xkv.proto.Metapb.Peer.newBuilder()
                .setId(newPeerId).setStoreId(99).setRole(io.github.xinfra.lab.xkv.proto.Metapb.PeerRole.Learner)
                .build();
        harness.pdServer().operators().scheduleChangePeer(
                /* regionId= */ 1, newPeer,
                io.github.xinfra.lab.xkv.proto.Pdpb.ConfChangeType.AddLearnerNode);

        org.awaitility.Awaitility.await().atMost(java.time.Duration.ofSeconds(15))
                .pollInterval(java.time.Duration.ofMillis(100))
                .until(() -> leader.peer.region().getPeersList().stream()
                        .anyMatch(p -> p.getId() == newPeer.getId()
                                && p.getRole() == io.github.xinfra.lab.xkv.proto.Metapb.PeerRole.Learner));
    }

    @Test
    void pdSplitOperatorReachesLeaderAndTriggersSplit() throws Exception {
        var leader = harness.leader();
        // Enqueue a split operator on PD. The leader's heartbeat picks it up
        // and triggers the local SplitDriver.
        harness.pdServer().operators().scheduleSplit(
                /* regionId= */ 1,
                java.util.List.of("m".getBytes()),
                io.github.xinfra.lab.xkv.proto.Pdpb.SplitRegion.Policy.USER_KEY);

        org.awaitility.Awaitility.await().atMost(java.time.Duration.ofSeconds(15))
                .pollInterval(java.time.Duration.ofMillis(100))
                .until(() -> !leader.childPeers.isEmpty());

        assertThat(leader.peer.region().getEndKey().toStringUtf8())
                .as("parent shrunk after PD-driven split").isEqualTo("m");
    }

    @Test
    void pdTransferLeaderOperatorReachesLeader() throws Exception {
        var currentLeader = harness.leader();
        var target = harness.kvNodes().stream()
                .filter(n -> n.peerId != currentLeader.peerId)
                .findFirst().orElseThrow();
        var targetPeer = Metapb.Peer.newBuilder()
                .setId(target.peerId).setStoreId(target.peerId).build();

        // Enqueue the operator on PD. The leader's heartbeat (default ~1s)
        // will pull it down and call peer.transferLeader().
        harness.pdServer().operators()
                .scheduleTransferLeader(/* regionId= */ 1, targetPeer);

        Awaitility.await().atMost(Duration.ofSeconds(10))
                .pollInterval(Duration.ofMillis(100))
                .until(() -> target.peer.isLeader());

        assertThat(target.peer.isLeader()).isTrue();
    }
}
