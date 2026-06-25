package io.github.xinfra.lab.xkv.test;

import io.github.xinfra.lab.xkv.pd.state.Operator;
import io.github.xinfra.lab.xkv.pd.state.OperatorSteps;
import io.github.xinfra.lab.xkv.pd.state.SimpleOperator;
import io.github.xinfra.lab.xkv.proto.Metapb;
import io.github.xinfra.lab.xkv.proto.Pdpb;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

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
        var pdChannel = io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder
                .forAddress("127.0.0.1", harness.pdPort()).usePlaintext().build();
        long newPeerId;
        try {
            var pd = io.github.xinfra.lab.xkv.proto.PDGrpc.newBlockingStub(pdChannel);
            newPeerId = pd.allocID(Pdpb.AllocIDRequest
                    .newBuilder().setCount(1).build()).getId();
        } finally {
            pdChannel.shutdownNow().awaitTermination(2, java.util.concurrent.TimeUnit.SECONDS);
        }
        var newPeer = Metapb.Peer.newBuilder()
                .setId(newPeerId).setStoreId(99).setRole(Metapb.PeerRole.Learner)
                .build();
        var resp = Pdpb.RegionHeartbeatResponse.newBuilder()
                .setRegionId(1)
                .setChangePeer(newPeer)
                .addChangePeerV2(Pdpb.ChangePeer.newBuilder()
                        .setPeer(newPeer).setChangeType(Pdpb.ConfChangeType.AddLearnerNode).build())
                .build();
        var op = new SimpleOperator(System.nanoTime(), 1, Operator.Kind.ADD_PEER,
                "test: add learner", resp, Set.of(99L),
                List.of(new OperatorSteps.AddLearnerStep(newPeer)),
                Operator.PRIORITY_ADMIN);
        harness.pdServer().operatorController().addOperator(op);

        Awaitility.await().atMost(Duration.ofSeconds(15))
                .pollInterval(Duration.ofMillis(100))
                .until(() -> leader.peer.region().getPeersList().stream()
                        .anyMatch(p -> p.getId() == newPeer.getId()
                                && p.getRole() == Metapb.PeerRole.Learner));
    }

    @Test
    void pdSplitOperatorReachesLeaderAndTriggersSplit() throws Exception {
        var leader = harness.leader();
        var region = harness.pdServer().state().getRegion(1).orElseThrow();
        long currentVersion = region.getRegionEpoch().getVersion();

        var sr = Pdpb.SplitRegion.newBuilder()
                .setPolicy(Pdpb.SplitRegion.Policy.USER_KEY)
                .addKeys(com.google.protobuf.ByteString.copyFrom("m".getBytes()))
                .build();
        var resp = Pdpb.RegionHeartbeatResponse.newBuilder()
                .setRegionId(1)
                .setSplitRegion(sr)
                .build();
        var storeIds = new java.util.HashSet<Long>();
        for (var p : region.getPeersList()) storeIds.add(p.getStoreId());
        var op = new SimpleOperator(System.nanoTime(), 1, Operator.Kind.SPLIT,
                "test: split at m", resp, storeIds,
                List.of(new OperatorSteps.SplitRegionStep(currentVersion + 1)),
                Operator.PRIORITY_ADMIN);
        harness.pdServer().operatorController().addOperator(op);

        Awaitility.await().atMost(Duration.ofSeconds(15))
                .pollInterval(Duration.ofMillis(100))
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

        var resp = Pdpb.RegionHeartbeatResponse.newBuilder()
                .setRegionId(1)
                .setTransferLeader(targetPeer)
                .build();
        var op = new SimpleOperator(System.nanoTime(), 1, Operator.Kind.TRANSFER_LEADER,
                "test: transfer leader", resp, Set.of(targetPeer.getStoreId()),
                List.of(new OperatorSteps.TransferLeaderStep(targetPeer)),
                Operator.PRIORITY_ADMIN);
        harness.pdServer().operatorController().addOperator(op);

        Awaitility.await().atMost(Duration.ofSeconds(10))
                .pollInterval(Duration.ofMillis(100))
                .until(() -> target.peer.isLeader());

        assertThat(target.peer.isLeader()).isTrue();
    }
}
