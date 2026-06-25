package io.github.xinfra.lab.xkv.test;

import com.google.protobuf.ByteString;
import io.github.xinfra.lab.xkv.pd.state.InMemoryPdStateMachine;
import io.github.xinfra.lab.xkv.pd.state.MergeCheckerScheduler;
import io.github.xinfra.lab.xkv.pd.state.OperatorControllerImpl;
import io.github.xinfra.lab.xkv.pd.state.SimpleOperator;
import io.github.xinfra.lab.xkv.proto.Metapb;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

final class MergeCheckerSchedulerTest {

    private static final long MERGE_THRESHOLD = 1024 * 1024;

    @Test
    void mergesAdjacentSmallRegions() {
        var state = new InMemoryPdStateMachine();
        for (long s = 1; s <= 3; s++) {
            state.putStore(Metapb.Store.newBuilder().setId(s).build());
        }
        state.bootstrap(
                Metapb.Store.newBuilder().setId(1).build(),
                Metapb.Region.newBuilder()
                        .setId(1)
                        .setStartKey(ByteString.EMPTY)
                        .setEndKey(ByteString.copyFromUtf8("a"))
                        .setRegionEpoch(Metapb.RegionEpoch.newBuilder().setConfVer(1).setVersion(1))
                        .addPeers(peer(1, 1))
                        .build());

        state.updateRegion(Metapb.Region.newBuilder()
                .setId(100)
                .setStartKey(ByteString.copyFromUtf8("a"))
                .setEndKey(ByteString.copyFromUtf8("m"))
                .setRegionEpoch(Metapb.RegionEpoch.newBuilder().setConfVer(1).setVersion(1))
                .addPeers(peer(10, 1))
                .addPeers(peer(20, 2))
                .addPeers(peer(30, 3))
                .build());
        state.updateRegion(Metapb.Region.newBuilder()
                .setId(101)
                .setStartKey(ByteString.copyFromUtf8("m"))
                .setEndKey(ByteString.copyFromUtf8("z"))
                .setRegionEpoch(Metapb.RegionEpoch.newBuilder().setConfVer(1).setVersion(1))
                .addPeers(peer(11, 1))
                .addPeers(peer(21, 2))
                .addPeers(peer(31, 3))
                .build());

        state.updateRegionStats(100, 100, 10);
        state.updateRegionStats(101, 200, 20);

        var controller = new OperatorControllerImpl(64, 600_000);
        var scheduler = new MergeCheckerScheduler(state, controller, MERGE_THRESHOLD, 60_000);
        try {
            int scheduled = scheduler.runOnce();
            assertThat(scheduled).isEqualTo(1);

            var op = controller.getOperator(100);
            assertThat(op).isPresent();
            var resp = ((SimpleOperator) op.get()).response();
            assertThat(resp.hasMerge()).isTrue();
            assertThat(resp.getMerge().getTarget().getId()).isEqualTo(101);
        } finally {
            scheduler.close();
        }
    }

    @Test
    void noMergeWhenRegionsAboveThreshold() {
        var state = new InMemoryPdStateMachine();
        state.putStore(Metapb.Store.newBuilder().setId(1).build());
        state.bootstrap(
                Metapb.Store.newBuilder().setId(1).build(),
                Metapb.Region.newBuilder()
                        .setId(1)
                        .setStartKey(ByteString.EMPTY)
                        .setEndKey(ByteString.copyFromUtf8("a"))
                        .setRegionEpoch(Metapb.RegionEpoch.newBuilder().setConfVer(1).setVersion(1))
                        .addPeers(peer(1, 1))
                        .build());

        state.updateRegion(Metapb.Region.newBuilder()
                .setId(100)
                .setStartKey(ByteString.copyFromUtf8("a"))
                .setEndKey(ByteString.copyFromUtf8("m"))
                .setRegionEpoch(Metapb.RegionEpoch.newBuilder().setConfVer(1).setVersion(1))
                .addPeers(peer(10, 1))
                .build());
        state.updateRegion(Metapb.Region.newBuilder()
                .setId(101)
                .setStartKey(ByteString.copyFromUtf8("m"))
                .setEndKey(ByteString.copyFromUtf8("z"))
                .setRegionEpoch(Metapb.RegionEpoch.newBuilder().setConfVer(1).setVersion(1))
                .addPeers(peer(11, 1))
                .build());

        state.updateRegionStats(100, MERGE_THRESHOLD + 1, 1000);
        state.updateRegionStats(101, MERGE_THRESHOLD + 1, 1000);

        var controller = new OperatorControllerImpl(64, 600_000);
        var scheduler = new MergeCheckerScheduler(state, controller, MERGE_THRESHOLD, 60_000);
        try {
            assertThat(scheduler.runOnce()).isEqualTo(0);
        } finally {
            scheduler.close();
        }
    }

    @Test
    void noMergeWhenRegionsNotAdjacent() {
        var state = new InMemoryPdStateMachine();
        state.putStore(Metapb.Store.newBuilder().setId(1).build());
        state.bootstrap(
                Metapb.Store.newBuilder().setId(1).build(),
                Metapb.Region.newBuilder()
                        .setId(1)
                        .setStartKey(ByteString.EMPTY)
                        .setEndKey(ByteString.copyFromUtf8("a"))
                        .setRegionEpoch(Metapb.RegionEpoch.newBuilder().setConfVer(1).setVersion(1))
                        .addPeers(peer(1, 1))
                        .build());

        state.updateRegion(Metapb.Region.newBuilder()
                .setId(100)
                .setStartKey(ByteString.copyFromUtf8("a"))
                .setEndKey(ByteString.copyFromUtf8("m"))
                .setRegionEpoch(Metapb.RegionEpoch.newBuilder().setConfVer(1).setVersion(1))
                .addPeers(peer(10, 1))
                .build());
        state.updateRegion(Metapb.Region.newBuilder()
                .setId(101)
                .setStartKey(ByteString.copyFromUtf8("p"))
                .setEndKey(ByteString.copyFromUtf8("z"))
                .setRegionEpoch(Metapb.RegionEpoch.newBuilder().setConfVer(1).setVersion(1))
                .addPeers(peer(11, 1))
                .build());

        state.updateRegionStats(100, 100, 10);
        state.updateRegionStats(101, 100, 10);

        var controller = new OperatorControllerImpl(64, 600_000);
        var scheduler = new MergeCheckerScheduler(state, controller, MERGE_THRESHOLD, 60_000);
        try {
            assertThat(scheduler.runOnce()).isEqualTo(0);
        } finally {
            scheduler.close();
        }
    }

    @Test
    void noMergeWhenDifferentStores() {
        var state = new InMemoryPdStateMachine();
        for (long s = 1; s <= 3; s++) {
            state.putStore(Metapb.Store.newBuilder().setId(s).build());
        }
        state.bootstrap(
                Metapb.Store.newBuilder().setId(1).build(),
                Metapb.Region.newBuilder()
                        .setId(1)
                        .setStartKey(ByteString.EMPTY)
                        .setEndKey(ByteString.copyFromUtf8("a"))
                        .setRegionEpoch(Metapb.RegionEpoch.newBuilder().setConfVer(1).setVersion(1))
                        .addPeers(peer(1, 1))
                        .build());

        state.updateRegion(Metapb.Region.newBuilder()
                .setId(100)
                .setStartKey(ByteString.copyFromUtf8("a"))
                .setEndKey(ByteString.copyFromUtf8("m"))
                .setRegionEpoch(Metapb.RegionEpoch.newBuilder().setConfVer(1).setVersion(1))
                .addPeers(peer(10, 1))
                .addPeers(peer(20, 2))
                .build());
        state.updateRegion(Metapb.Region.newBuilder()
                .setId(101)
                .setStartKey(ByteString.copyFromUtf8("m"))
                .setEndKey(ByteString.copyFromUtf8("z"))
                .setRegionEpoch(Metapb.RegionEpoch.newBuilder().setConfVer(1).setVersion(1))
                .addPeers(peer(11, 1))
                .addPeers(peer(31, 3))
                .build());

        state.updateRegionStats(100, 100, 10);
        state.updateRegionStats(101, 100, 10);

        var controller = new OperatorControllerImpl(64, 600_000);
        var scheduler = new MergeCheckerScheduler(state, controller, MERGE_THRESHOLD, 60_000);
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
