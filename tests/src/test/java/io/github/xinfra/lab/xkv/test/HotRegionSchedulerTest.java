package io.github.xinfra.lab.xkv.test;

import com.google.protobuf.ByteString;
import io.github.xinfra.lab.xkv.pd.state.HotRegionScheduler;
import io.github.xinfra.lab.xkv.pd.state.InMemoryPdStateMachine;
import io.github.xinfra.lab.xkv.pd.state.OperatorControllerImpl;
import io.github.xinfra.lab.xkv.pd.state.OperatorQueue;
import io.github.xinfra.lab.xkv.pd.state.StoreStatsCache;
import io.github.xinfra.lab.xkv.proto.Metapb;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit-level test for {@link HotRegionScheduler}.
 *
 * <p>Sets up 3 stores with 6 regions. Three regions on store 1 are
 * configured as "hot" (high approximate keys). The scheduler should
 * transfer leadership of hot regions to less-hot stores.
 */
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

        // 6 regions, all led by store 1 with followers on stores 2 and 3.
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

        // Mark 2 regions as hot (high keys), rest as cold.
        // With 2 hot at 10_000 and 4 cold at 10:
        // avg = (2*10000 + 4*10) / 6 = 20040/6 ≈ 3340, threshold ≈ 6680.
        // Hot regions at 10_000 > 6680 — clearly hot.
        for (long i = 0; i < 6; i++) {
            long regionId = 100 + i;
            long keys = i < 2 ? 10_000 : 10;
            state.updateRegionStats(regionId, 1024, keys);
        }

        var ops = new OperatorQueue();
        var controller = new OperatorControllerImpl(ops, 5, 60_000);
        var storeStats = new StoreStatsCache();
        var scheduler = new HotRegionScheduler(state, controller, ops, storeStats, 60_000);
        try {
            int scheduled = scheduler.runOnce();

            // Should have scheduled at least one transfer for hot regions.
            assertThat(scheduled).isGreaterThan(0);
            assertThat(scheduled).isLessThanOrEqualTo(HotRegionScheduler.MAX_OPERATORS_PER_TICK);

            // Verify the operators are TransferLeader and target non-store-1 peers.
            int transfers = 0;
            for (long regionId = 100; regionId < 106; regionId++) {
                while (true) {
                    var op = ops.poll(regionId);
                    if (op.isEmpty()) break;
                    assertThat(op.get().hasTransferLeader()).isTrue();
                    assertThat(op.get().getTransferLeader().getStoreId())
                            .as("transfer target should not be store 1 (the hot store)")
                            .isNotEqualTo(1L);
                    transfers++;
                }
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

        // All regions have same load — no hot regions.
        state.updateRegionStats(1, 1024, 100);

        var ops = new OperatorQueue();
        var controller = new OperatorControllerImpl(ops, 5, 60_000);
        var scheduler = new HotRegionScheduler(state, controller, ops, new StoreStatsCache(), 60_000);
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
