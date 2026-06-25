package io.github.xinfra.lab.xkv.pd.state;

import io.github.xinfra.lab.xkv.proto.Metapb;
import io.github.xinfra.lab.xkv.proto.Pdpb;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

final class OperatorControllerImplTest {

    private OperatorControllerImpl controller;

    @BeforeEach
    void setUp() {
        controller = new OperatorControllerImpl(/* maxPerStore= */ 2, /* timeoutMs= */ 60_000);
    }

    @Test
    void addOperator_succeeds_when_under_store_limit() {
        var op = transferLeaderOp(1, 100, Set.of(2L));
        assertThat(controller.addOperator(op)).isTrue();
        assertThat(controller.totalInFlight()).isEqualTo(1);
        assertThat(controller.storeInFlightCount(2)).isEqualTo(1);
    }

    @Test
    void addOperator_rejects_when_store_limit_exceeded() {
        controller.addOperator(transferLeaderOp(1, 100, Set.of(2L)));
        controller.addOperator(transferLeaderOp(2, 101, Set.of(2L)));
        assertThat(controller.storeInFlightCount(2)).isEqualTo(2);

        var op3 = transferLeaderOp(3, 102, Set.of(2L));
        assertThat(controller.addOperator(op3)).isFalse();
        assertThat(controller.totalInFlight()).isEqualTo(2);
    }

    @Test
    void addOperator_rejects_duplicate_region_same_priority() {
        controller.addOperator(transferLeaderOp(1, 100, Set.of(2L)));

        var dup = transferLeaderOp(2, 100, Set.of(3L));
        assertThat(controller.addOperator(dup)).isFalse();
        assertThat(controller.totalInFlight()).isEqualTo(1);
    }

    @Test
    void addOperator_replaces_lower_priority_operator() {
        var lowPri = transferLeaderOp(1, 100, Set.of(2L), Operator.PRIORITY_BALANCE);
        controller.addOperator(lowPri);
        assertThat(controller.totalInFlight()).isEqualTo(1);

        var highPri = transferLeaderOp(2, 100, Set.of(3L), Operator.PRIORITY_RULE_FIX);
        assertThat(controller.addOperator(highPri)).isTrue();
        assertThat(controller.totalInFlight()).isEqualTo(1);
        assertThat(controller.getOperator(100).get().id()).isEqualTo(2);
        assertThat(controller.storeInFlightCount(2)).isEqualTo(0);
        assertThat(controller.storeInFlightCount(3)).isEqualTo(1);
        assertThat(controller.opsReplaced()).isEqualTo(1);
    }

    @Test
    void removeOperator_decrements_store_counter() {
        controller.addOperator(transferLeaderOp(1, 100, Set.of(2L)));
        assertThat(controller.storeInFlightCount(2)).isEqualTo(1);

        assertThat(controller.removeOperator(100)).isTrue();
        assertThat(controller.storeInFlightCount(2)).isEqualTo(0);
        assertThat(controller.totalInFlight()).isEqualTo(0);
    }

    @Test
    void removeOperator_returns_false_for_unknown_region() {
        assertThat(controller.removeOperator(999)).isFalse();
    }

    @Test
    void getOperator_returns_inflight() {
        var op = transferLeaderOp(1, 100, Set.of(2L));
        controller.addOperator(op);

        assertThat(controller.getOperator(100)).isPresent();
        assertThat(controller.getOperator(100).get().id()).isEqualTo(1);
    }

    @Test
    void getOperator_returns_empty_for_unknown() {
        assertThat(controller.getOperator(999)).isEmpty();
    }

    @Test
    void getOperators_returns_all_inflight() {
        controller.addOperator(transferLeaderOp(1, 100, Set.of(2L)));
        controller.addOperator(transferLeaderOp(2, 101, Set.of(3L)));

        assertThat(controller.getOperators()).hasSize(2);
    }

    @Test
    void dispatch_returns_response_when_pending() {
        var target = Metapb.Peer.newBuilder()
                .setId(10).setStoreId(2).setRole(Metapb.PeerRole.Voter).build();
        controller.addOperator(transferLeaderOp(1, 100, Set.of(2L)));

        // Heartbeat where leader is NOT the target → step not satisfied → PENDING → resend
        var hb = Pdpb.RegionHeartbeatRequest.newBuilder()
                .setRegion(Metapb.Region.newBuilder().setId(100)
                        .addPeers(Metapb.Peer.newBuilder().setId(10).setStoreId(2)))
                .setLeader(Metapb.Peer.newBuilder().setId(99).setStoreId(1))
                .build();
        var result = controller.dispatch(hb);
        assertThat(result).isPresent();
        assertThat(result.get().hasTransferLeader()).isTrue();
        assertThat(controller.totalInFlight()).isEqualTo(1);
    }

    @Test
    void dispatch_returns_empty_and_removes_when_finished() {
        var target = Metapb.Peer.newBuilder()
                .setId(10).setStoreId(2).setRole(Metapb.PeerRole.Voter).build();
        controller.addOperator(transferLeaderOp(1, 100, Set.of(2L)));

        // Heartbeat where leader IS the target → step satisfied → FINISHED
        var hb = Pdpb.RegionHeartbeatRequest.newBuilder()
                .setRegion(Metapb.Region.newBuilder().setId(100)
                        .addPeers(target))
                .setLeader(target)
                .build();
        var result = controller.dispatch(hb);
        assertThat(result).isEmpty();
        assertThat(controller.totalInFlight()).isEqualTo(0);
        assertThat(controller.storeInFlightCount(2)).isEqualTo(0);
        assertThat(controller.opsSuccess()).isEqualTo(1);
    }

    @Test
    void dispatch_returns_empty_when_no_operator() {
        var hb = Pdpb.RegionHeartbeatRequest.newBuilder()
                .setRegion(Metapb.Region.newBuilder().setId(999))
                .build();
        assertThat(controller.dispatch(hb)).isEmpty();
    }

    @Test
    void dispatch_evicts_expired_operators() {
        var shortTimeout = new OperatorControllerImpl(5, /* timeoutMs= */ 1);
        shortTimeout.addOperator(transferLeaderOp(1, 100, Set.of(2L)));

        try { Thread.sleep(10); } catch (InterruptedException ignored) {}

        var hb = Pdpb.RegionHeartbeatRequest.newBuilder()
                .setRegion(Metapb.Region.newBuilder().setId(100))
                .build();
        shortTimeout.dispatch(hb);

        assertThat(shortTimeout.totalInFlight()).isEqualTo(0);
        assertThat(shortTimeout.storeInFlightCount(2)).isEqualTo(0);
        assertThat(shortTimeout.opsTimeout()).isEqualTo(1);
    }

    @Test
    void history_tracks_completed_operators() {
        controller.addOperator(transferLeaderOp(1, 100, Set.of(2L)));

        var target = Metapb.Peer.newBuilder()
                .setId(10).setStoreId(2).setRole(Metapb.PeerRole.Voter).build();
        var hb = Pdpb.RegionHeartbeatRequest.newBuilder()
                .setRegion(Metapb.Region.newBuilder().setId(100).addPeers(target))
                .setLeader(target)
                .build();
        controller.dispatch(hb);

        assertThat(controller.history()).hasSize(1);
        assertThat(controller.history().get(0).id()).isEqualTo(1);
    }

    @Test
    void shutdown_clears_all_state() {
        controller.addOperator(transferLeaderOp(1, 100, Set.of(2L)));
        controller.addOperator(transferLeaderOp(2, 101, Set.of(3L)));

        controller.shutdown();
        assertThat(controller.totalInFlight()).isEqualTo(0);
        assertThat(controller.getOperators()).isEmpty();
    }

    private static SimpleOperator transferLeaderOp(long id, long regionId, Set<Long> targetStores) {
        return transferLeaderOp(id, regionId, targetStores, Operator.PRIORITY_DEFAULT);
    }

    private static SimpleOperator transferLeaderOp(long id, long regionId,
                                                    Set<Long> targetStores, int priority) {
        var target = Metapb.Peer.newBuilder()
                .setId(id * 10)
                .setStoreId(targetStores.iterator().next())
                .setRole(Metapb.PeerRole.Voter)
                .build();
        var resp = Pdpb.RegionHeartbeatResponse.newBuilder()
                .setRegionId(regionId)
                .setTransferLeader(target)
                .build();
        return new SimpleOperator(id, regionId, Operator.Kind.TRANSFER_LEADER,
                "test-transfer-leader", resp, targetStores,
                List.of(new OperatorSteps.TransferLeaderStep(target)),
                priority);
    }
}
