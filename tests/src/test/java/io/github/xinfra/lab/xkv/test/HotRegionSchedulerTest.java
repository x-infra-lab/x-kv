package io.github.xinfra.lab.xkv.test;

import com.google.protobuf.ByteString;
import io.github.xinfra.lab.xkv.pd.state.HotRegionScheduler;
import io.github.xinfra.lab.xkv.pd.state.InMemoryPdStateMachine;
import io.github.xinfra.lab.xkv.pd.state.OperatorControllerImpl;
import io.github.xinfra.lab.xkv.pd.state.SimpleOperator;
import io.github.xinfra.lab.xkv.pd.state.StoreStatsCache;
import io.github.xinfra.lab.xkv.proto.Metapb;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

final class HotRegionSchedulerTest {

    @Test
    void transfersHotRegionLeadersToLessHotStores() {
        var state = new InMemoryPdStateMachine();
        for (long s = 1; s <= 3; s++) {
            state.putStore(Metapb.Store.newBuilder().setId(s).build());
        }
        state.bootstrap(
                Metapb.Store.newBuilder().setId(1).build(),
                Metapb.Region.newBuilder()
                        .setId(1)
                        .setStartKey(ByteString.EMPTY)
                        .setEndKey(ByteString.copyFromUtf8("k0"))
                        .setRegionEpoch(Metapb.RegionEpoch.newBuilder().setConfVer(1).setVersion(1))
                        .addPeers(peer(1, 1))
                        .build());

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

        for (long i = 0; i < 6; i++) {
            long regionId = 100 + i;
            long keys = i < 2 ? 10_000 : 10;
            state.updateRegionStats(regionId, 1024, keys);
        }

        var controller = new OperatorControllerImpl(5, 60_000);
        var storeStats = new StoreStatsCache();
        var scheduler = new HotRegionScheduler(state, controller, storeStats, 60_000);
        try {
            int scheduled = scheduler.runOnce();

            assertThat(scheduled).isGreaterThan(0);
            assertThat(scheduled).isLessThanOrEqualTo(HotRegionScheduler.MAX_OPERATORS_PER_TICK);

            int transfers = 0;
            for (long regionId = 100; regionId < 106; regionId++) {
                var op = controller.getOperator(regionId);
                if (op.isEmpty()) continue;
                var resp = ((SimpleOperator) op.get()).response();
                assertThat(resp.hasTransferLeader()).isTrue();
                assertThat(resp.getTransferLeader().getStoreId())
                        .as("transfer target should not be store 1 (the hot store)")
                        .isNotEqualTo(1L);
                transfers++;
            }
            assertThat(transfers).isEqualTo(scheduled);
        } finally {
            scheduler.close();
        }
    }

    @Test
    void noTransfersWhenNoHotRegions() {
        var state = new InMemoryPdStateMachine();
        for (long s = 1; s <= 3; s++) {
            state.putStore(Metapb.Store.newBuilder().setId(s).build());
        }
        state.bootstrap(
                Metapb.Store.newBuilder().setId(1).build(),
                Metapb.Region.newBuilder()
                        .setId(1)
                        .setStartKey(ByteString.EMPTY)
                        .setEndKey(ByteString.EMPTY)
                        .setRegionEpoch(Metapb.RegionEpoch.newBuilder().setConfVer(1).setVersion(1))
                        .addPeers(peer(1, 1))
                        .build());

        state.updateRegionStats(1, 1024, 100);

        var controller = new OperatorControllerImpl(5, 60_000);
        var scheduler = new HotRegionScheduler(state, controller, new StoreStatsCache(), 60_000);
        try {
            assertThat(scheduler.runOnce()).isEqualTo(0);
        } finally {
            scheduler.close();
        }
    }

    private static Metapb.Peer peer(long peerId, long storeId) {
        return Metapb.Peer.newBuilder().setId(peerId).setStoreId(storeId)
                .setRole(Metapb.PeerRole.Voter).build();
    }
}
