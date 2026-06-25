package io.github.xinfra.lab.xkv.test;

import com.google.protobuf.ByteString;
import io.github.xinfra.lab.xkv.pd.state.InMemoryPdStateMachine;
import io.github.xinfra.lab.xkv.pd.state.LeaderBalanceScheduler;
import io.github.xinfra.lab.xkv.pd.state.OperatorControllerImpl;
import io.github.xinfra.lab.xkv.pd.state.SimpleOperator;
import io.github.xinfra.lab.xkv.proto.Metapb;
import org.junit.jupiter.api.Test;

import java.util.HashMap;

import static org.assertj.core.api.Assertions.assertThat;

final class LeaderBalanceSchedulerTest {

    @Test
    void enqueuesTransferLeadersUntilLoadIsWithinOne() {
        var state = new InMemoryPdStateMachine();
        for (long s = 1; s <= 3; s++) {
            state.putStore(Metapb.Store.newBuilder().setId(s).build());
        }
        state.bootstrap(
                Metapb.Store.newBuilder().setId(1).build(),
                bootstrapRegion());

        for (long i = 0; i < 6; i++) {
            long regionId = 100 + i;
            byte[] start = ("k" + i).getBytes();
            byte[] end = i + 1 < 6 ? ("k" + (i + 1)).getBytes() : new byte[0];
            state.updateRegion(Metapb.Region.newBuilder()
                    .setId(regionId)
                    .setStartKey(ByteString.copyFrom(start))
                    .setEndKey(ByteString.copyFrom(end))
                    .setRegionEpoch(Metapb.RegionEpoch.newBuilder().setConfVer(1).setVersion(1))
                    .addPeers(peer(10 + i, 1))
                    .addPeers(peer(20 + i, 2))
                    .addPeers(peer(30 + i, 3))
                    .build());
        }

        var controller = new OperatorControllerImpl(64, 600_000);
        var scheduler = new LeaderBalanceScheduler(state, controller, 60_000);
        try {
            int round1 = scheduler.runOnce();
            assertThat(round1).isGreaterThan(0);
            assertThat(round1).isLessThanOrEqualTo(LeaderBalanceScheduler.MAX_OPERATORS_PER_TICK);

            // Apply scheduled transfers: read operator responses, update state, then remove operators.
            applyTransfers(state, controller);

            for (int i = 0; i < 5; i++) {
                int extra = scheduler.runOnce();
                if (extra == 0) break;
                applyTransfers(state, controller);
            }

            var counts = new HashMap<Long, Integer>();
            for (var r : state.allRegions()) {
                if (r.getId() < 100) continue;
                if (r.getPeersCount() == 0) continue;
                counts.merge(r.getPeers(0).getStoreId(), 1, Integer::sum);
            }
            int max = counts.values().stream().mapToInt(Integer::intValue).max().orElse(0);
            int min = counts.values().stream().mapToInt(Integer::intValue).min().orElse(0);
            assertThat(max - min)
                    .as("after convergence, leader spread is <= 1 (counts=%s)", counts)
                    .isLessThanOrEqualTo(1);
            assertThat(counts.keySet()).contains(1L, 2L, 3L);
        } finally {
            scheduler.close();
        }
    }

    private void applyTransfers(InMemoryPdStateMachine state, OperatorControllerImpl controller) {
        for (long regionId = 100; regionId < 106; regionId++) {
            var op = controller.getOperator(regionId);
            if (op.isEmpty()) continue;
            var resp = ((SimpleOperator) op.get()).response();
            if (!resp.hasTransferLeader()) continue;
            long target = resp.getTransferLeader().getId();
            var region = state.getRegion(regionId).orElseThrow();
            var b = region.toBuilder().clearPeers();
            region.getPeersList().stream()
                    .filter(p -> p.getId() == target).forEach(b::addPeers);
            region.getPeersList().stream()
                    .filter(p -> p.getId() != target).forEach(b::addPeers);
            state.updateRegion(b.build());
            controller.removeOperator(regionId);
        }
    }

    private Metapb.Region bootstrapRegion() {
        return Metapb.Region.newBuilder()
                .setId(1)
                .setStartKey(ByteString.EMPTY)
                .setEndKey(ByteString.copyFromUtf8("k0"))
                .setRegionEpoch(Metapb.RegionEpoch.newBuilder().setConfVer(1).setVersion(1))
                .addPeers(peer(1, 1))
                .build();
    }

    private static Metapb.Peer peer(long peerId, long storeId) {
        return Metapb.Peer.newBuilder().setId(peerId).setStoreId(storeId)
                .setRole(Metapb.PeerRole.Voter).build();
    }
}
