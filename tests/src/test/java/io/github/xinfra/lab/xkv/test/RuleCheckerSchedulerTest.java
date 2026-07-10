package io.github.xinfra.lab.xkv.test;

import com.google.protobuf.ByteString;
import io.github.xinfra.lab.xkv.pd.state.OperatorControllerImpl;
import io.github.xinfra.lab.xkv.pd.state.RocksDbPdStateMachine;
import io.github.xinfra.lab.xkv.pd.state.RuleCheckerScheduler;
import io.github.xinfra.lab.xkv.pd.state.SimpleOperator;
import io.github.xinfra.lab.xkv.pd.state.StoreStatsCache;
import io.github.xinfra.lab.xkv.proto.Metapb;
import io.github.xinfra.lab.xkv.proto.Pdpb;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThat;

final class RuleCheckerSchedulerTest {

    @TempDir
    Path tempDir;

    private RocksDbPdStateMachine state;

    @BeforeEach
    void setUp() {
        state = new RocksDbPdStateMachine(tempDir.resolve("pd-state"));
    }

    @AfterEach
    void tearDown() {
        state.close();
    }

    @Test
    void addsReplicaForUnderReplicatedRegion() {
        for (long s = 1; s <= 3; s++) {
            state.putStore(Metapb.Store.newBuilder()
                    .setId(s).setState(Metapb.StoreState.Up).build());
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
                .setEndKey(ByteString.EMPTY)
                .setRegionEpoch(Metapb.RegionEpoch.newBuilder().setConfVer(1).setVersion(1))
                .addPeers(peer(10, 1))
                .addPeers(peer(20, 2))
                .build());

        var controller = new OperatorControllerImpl(64, 600_000);
        var scheduler = new RuleCheckerScheduler(state, controller, new StoreStatsCache(), 60_000);
        try {
            int scheduled = scheduler.runOnce();
            assertThat(scheduled).isGreaterThan(0);

            var op = controller.getOperator(100);
            assertThat(op).isPresent();
            var resp = ((SimpleOperator) op.get()).response();
            assertThat(resp.hasChangePeer()).isTrue();
            assertThat(resp.getChangePeer().getStoreId()).isEqualTo(3L);

            assertThat(resp.getChangePeerV2Count()).isGreaterThan(0);
            assertThat(resp.getChangePeerV2(0).getChangeType())
                    .isEqualTo(Pdpb.ConfChangeType.AddNode);
        } finally {
            scheduler.close();
        }
    }

    @Test
    void removesPeerForOverReplicatedRegion() {
        for (long s = 1; s <= 4; s++) {
            state.putStore(Metapb.Store.newBuilder()
                    .setId(s).setState(Metapb.StoreState.Up).build());
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
                .setEndKey(ByteString.EMPTY)
                .setRegionEpoch(Metapb.RegionEpoch.newBuilder().setConfVer(1).setVersion(1))
                .addPeers(peer(10, 1))
                .addPeers(peer(20, 2))
                .addPeers(peer(30, 3))
                .addPeers(peer(40, 4))
                .build());

        var controller = new OperatorControllerImpl(64, 600_000);
        var scheduler = new RuleCheckerScheduler(state, controller, new StoreStatsCache(), 60_000);
        try {
            int scheduled = scheduler.runOnce();
            assertThat(scheduled).isGreaterThan(0);

            var op = controller.getOperator(100);
            assertThat(op).isPresent();
            var resp = ((SimpleOperator) op.get()).response();
            assertThat(resp.hasChangePeer()).isTrue();
            assertThat(resp.getChangePeerV2(0).getChangeType())
                    .isEqualTo(Pdpb.ConfChangeType.RemoveNode);
        } finally {
            scheduler.close();
        }
    }

    @Test
    void removesDownStorePeer() {
        state.putStore(Metapb.Store.newBuilder()
                .setId(1).setState(Metapb.StoreState.Up).build());
        state.putStore(Metapb.Store.newBuilder()
                .setId(2).setState(Metapb.StoreState.Up).build());
        state.putStore(Metapb.Store.newBuilder()
                .setId(3).setState(Metapb.StoreState.Down).build());

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
                .setEndKey(ByteString.EMPTY)
                .setRegionEpoch(Metapb.RegionEpoch.newBuilder().setConfVer(1).setVersion(1))
                .addPeers(peer(10, 1))
                .addPeers(peer(20, 2))
                .addPeers(peer(30, 3))
                .build());

        var controller = new OperatorControllerImpl(64, 600_000);
        var scheduler = new RuleCheckerScheduler(state, controller, new StoreStatsCache(), 60_000);
        try {
            int scheduled = scheduler.runOnce();
            assertThat(scheduled).isGreaterThan(0);

            var op = controller.getOperator(100);
            assertThat(op).isPresent();
            var resp = ((SimpleOperator) op.get()).response();
            assertThat(resp.hasChangePeer()).isTrue();
            assertThat(resp.getChangePeer().getStoreId()).isEqualTo(3L);
            assertThat(resp.getChangePeerV2(0).getChangeType())
                    .isEqualTo(Pdpb.ConfChangeType.RemoveNode);
        } finally {
            scheduler.close();
        }
    }

    @Test
    void noActionWhenProperlyreplicated() {
        for (long s = 1; s <= 3; s++) {
            state.putStore(Metapb.Store.newBuilder()
                    .setId(s).setState(Metapb.StoreState.Up).build());
        }
        state.bootstrap(
                Metapb.Store.newBuilder().setId(1).build(),
                Metapb.Region.newBuilder()
                        .setId(1)
                        .setStartKey(ByteString.EMPTY)
                        .setEndKey(ByteString.EMPTY)
                        .setRegionEpoch(Metapb.RegionEpoch.newBuilder().setConfVer(1).setVersion(1))
                        .addPeers(peer(1, 1))
                        .addPeers(peer(2, 2))
                        .addPeers(peer(3, 3))
                        .build());

        var controller = new OperatorControllerImpl(64, 600_000);
        var scheduler = new RuleCheckerScheduler(state, controller, new StoreStatsCache(), 60_000);
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
