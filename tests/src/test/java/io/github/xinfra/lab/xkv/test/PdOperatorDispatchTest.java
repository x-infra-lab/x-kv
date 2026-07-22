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
    private TestCluster cluster;

    @BeforeEach
    void start() throws Exception {
        cluster = new TestCluster(baseDir).startReplicated(1, 3);
    }

    @AfterEach
    void stop() throws Exception {
        if (cluster != null) cluster.close();
    }

    @Test
    void pdChangePeerOperatorAddsLearnerToRegion() throws Exception {
        var leaderNode = cluster.leaderStoreFor(TestCluster.BOOTSTRAP_REGION_ID);
        var leaderPeer = cluster.realPeer(leaderNode.storeId, TestCluster.BOOTSTRAP_REGION_ID);
        long newPeerId = cluster.pdStub().allocID(Pdpb.AllocIDRequest
                .newBuilder().setCount(1).build()).getId();
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
        cluster.pdLeader().server.operatorController().addOperator(op);

        Awaitility.await().atMost(Duration.ofSeconds(15))
                .pollInterval(Duration.ofMillis(100))
                .until(() -> leaderPeer.region().getPeersList().stream()
                        .anyMatch(p -> p.getId() == newPeer.getId()
                                && p.getRole() == Metapb.PeerRole.Learner));
    }

    @Test
    void pdSplitOperatorReachesLeaderAndTriggersSplit() throws Exception {
        var leaderNode = cluster.leaderStoreFor(TestCluster.BOOTSTRAP_REGION_ID);
        var leaderPeer = cluster.realPeer(leaderNode.storeId, TestCluster.BOOTSTRAP_REGION_ID);
        var region = cluster.pdLeader().server.state().getRegion(1).orElseThrow();
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
        cluster.pdLeader().server.operatorController().addOperator(op);

        Awaitility.await().atMost(Duration.ofSeconds(15))
                .pollInterval(Duration.ofMillis(100))
                .until(() -> leaderNode.store().peers().size() > 1);

        assertThat(leaderPeer.region().getEndKey().toStringUtf8())
                .as("parent shrunk after PD-driven split").isEqualTo("m");
    }

    @Test
    void pdTransferLeaderOperatorReachesLeader() throws Exception {
        var targetNode = cluster.followerStoresFor(TestCluster.BOOTSTRAP_REGION_ID).get(0);
        var targetRegionPeer = cluster.realPeer(targetNode.storeId, TestCluster.BOOTSTRAP_REGION_ID);
        var targetPeer = targetRegionPeer.self();

        var resp = Pdpb.RegionHeartbeatResponse.newBuilder()
                .setRegionId(1)
                .setTransferLeader(targetPeer)
                .build();
        var op = new SimpleOperator(System.nanoTime(), 1, Operator.Kind.TRANSFER_LEADER,
                "test: transfer leader", resp, Set.of(targetPeer.getStoreId()),
                List.of(new OperatorSteps.TransferLeaderStep(targetPeer)),
                Operator.PRIORITY_ADMIN);
        cluster.pdLeader().server.operatorController().addOperator(op);

        Awaitility.await().atMost(Duration.ofSeconds(10))
                .pollInterval(Duration.ofMillis(100))
                .until(() -> targetRegionPeer.isLeader());

        assertThat(targetRegionPeer.isLeader()).isTrue();
    }
}
