package io.github.xinfra.lab.xkv.pd.state;

import io.github.xinfra.lab.xkv.proto.Metapb;
import io.github.xinfra.lab.xkv.proto.Pdpb;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

final class OperatorControllerImplTest {

    private OperatorQueue queue;
    private OperatorControllerImpl controller;

    @BeforeEach
    void setUp() {
        queue = new OperatorQueue();
        controller = new OperatorControllerImpl(queue, /* maxPerStore= */ 2, /* timeoutMs= */ 60_000);
    }

    @Test
    void addOperator_succeeds_when_under_store_limit() {
        var op = transferLeaderOp(1, 100, Set.of(2L));
        assertThat(controller.addOperator(op)).isTrue();
        assertThat(controller.totalInFlight()).isEqualTo(1);
        assertThat(controller.storeInFlightCount(2)).isEqualTo(1);
        assertThat(queue.size(100)).isEqualTo(1);
    }

    @Test
    void addOperator_rejects_when_store_limit_exceeded() {
        controller.addOperator(transferLeaderOp(1, 100, Set.of(2L)));
        controller.addOperator(transferLeaderOp(2, 101, Set.of(2L)));
        assertThat(controller.storeInFlightCount(2)).isEqualTo(2);

        // Third operator targeting same store should be rejected.
        var op3 = transferLeaderOp(3, 102, Set.of(2L));
        assertThat(controller.addOperator(op3)).isFalse();
        assertThat(controller.totalInFlight()).isEqualTo(2);
    }

    @Test
    void addOperator_rejects_duplicate_region() {
        controller.addOperator(transferLeaderOp(1, 100, Set.of(2L)));

        var dup = transferLeaderOp(2, 100, Set.of(3L));
        assertThat(controller.addOperator(dup)).isFalse();
        assertThat(controller.totalInFlight()).isEqualTo(1);
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
    void dispatch_returns_response_for_inflight_region() {
        controller.addOperator(transferLeaderOp(1, 100, Set.of(2L)));

        var hb = Pdpb.RegionHeartbeatRequest.newBuilder()
                .setRegion(Metapb.Region.newBuilder().setId(100))
                .build();
        var result = controller.dispatch(hb);
        assertThat(result).isPresent();
        assertThat(result.get().hasTransferLeader()).isTrue();
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
        var shortTimeout = new OperatorControllerImpl(queue, 5, /* timeoutMs= */ 1);
        shortTimeout.addOperator(transferLeaderOp(1, 100, Set.of(2L)));

        // Wait for expiry.
        try { Thread.sleep(10); } catch (InterruptedException ignored) {}

        var hb = Pdpb.RegionHeartbeatRequest.newBuilder()
                .setRegion(Metapb.Region.newBuilder().setId(100))
                .build();
        // dispatch should evict the expired operator, then return the
        // next response (empty since operator was evicted and removed).
        shortTimeout.dispatch(hb);

        assertThat(shortTimeout.totalInFlight()).isEqualTo(0);
        assertThat(shortTimeout.storeInFlightCount(2)).isEqualTo(0);
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
                "test-transfer-leader", resp, targetStores);
    }
}
