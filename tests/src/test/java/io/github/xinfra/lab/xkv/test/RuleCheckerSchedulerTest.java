package io.github.xinfra.lab.xkv.test;

import com.google.protobuf.ByteString;
import io.github.xinfra.lab.xkv.pd.state.InMemoryPdStateMachine;
import io.github.xinfra.lab.xkv.pd.state.OperatorQueue;
import io.github.xinfra.lab.xkv.pd.state.RuleCheckerScheduler;
import io.github.xinfra.lab.xkv.pd.state.StoreStatsCache;
import io.github.xinfra.lab.xkv.proto.Metapb;
import io.github.xinfra.lab.xkv.proto.Pdpb;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit-level test for {@link RuleCheckerScheduler}.
 */
final class RuleCheckerSchedulerTest {

    @Test
    void addsReplicaForUnderReplicatedRegion() {
        var state = new InMemoryPdStateMachine();
        // 3 healthy stores.
        for (long s = 1; s <= 3; s++) {
            state.putStore(Metapb.Store.newBuilder()
                    .setId(s).setState(Metapb.StoreState.Up).build());
        }
        // Bootstrap with max_peer_count = 3.
        state.bootstrap(
                Metapb.Store.newBuilder().setId(1).build(),
                Metapb.Region.newBuilder()
                        .setId(1)
                        .setStartKey(ByteString.EMPTY)
                        .setEndKey(ByteString.copyFromUtf8("a"))
                        .setRegionEpoch(Metapb.RegionEpoch.newBuilder().setConfVer(1).setVersion(1))
                        .addPeers(peer(1, 1))
                        .build());

        // Under-replicated region: only 2 peers (max_peer_count = 3).
        state.updateRegion(Metapb.Region.newBuilder()
                .setId(100)
                .setStartKey(ByteString.copyFromUtf8("a"))
                .setEndKey(ByteString.EMPTY)
                .setRegionEpoch(Metapb.RegionEpoch.newBuilder().setConfVer(1).setVersion(1))
                .addPeers(peer(10, 1))
                .addPeers(peer(20, 2))
                .build());

        var ops = new OperatorQueue();
        var scheduler = new RuleCheckerScheduler(state, ops, new StoreStatsCache(), 60_000);
        try {
            int scheduled = scheduler.runOnce();
            assertThat(scheduled).isGreaterThan(0);

            // Should have added a peer on store 3 (the only store missing).
            var op = ops.poll(100);
            assertThat(op).isPresent();
            assertThat(op.get().hasChangePeer()).isTrue();
            assertThat(op.get().getChangePeer().getStoreId()).isEqualTo(3L);

            // Verify the ChangePeerV2 entry has AddNode type.
            assertThat(op.get().getChangePeerV2Count()).isGreaterThan(0);
            assertThat(op.get().getChangePeerV2(0).getChangeType())
                    .isEqualTo(Pdpb.ConfChangeType.AddNode);
        } finally {
            scheduler.close();
        }
    }

    @Test
    void removesPeerForOverReplicatedRegion() {
        var state = new InMemoryPdStateMachine();
        for (long s = 1; s <= 4; s++) {
            state.putStore(Metapb.Store.newBuilder()
                    .setId(s).setState(Metapb.StoreState.Up).build());
        }
        // Bootstrap with max_peer_count = 3 (default).
        state.bootstrap(
                Metapb.Store.newBuilder().setId(1).build(),
                Metapb.Region.newBuilder()
                        .setId(1)
                        .setStartKey(ByteString.EMPTY)
                        .setEndKey(ByteString.copyFromUtf8("a"))
                        .setRegionEpoch(Metapb.RegionEpoch.newBuilder().setConfVer(1).setVersion(1))
                        .addPeers(peer(1, 1))
                        .build());

        // Over-replicated: 4 peers with max_peer_count = 3.
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

        var ops = new OperatorQueue();
        var scheduler = new RuleCheckerScheduler(state, ops, new StoreStatsCache(), 60_000);
        try {
            int scheduled = scheduler.runOnce();
            assertThat(scheduled).isGreaterThan(0);

            var op = ops.poll(100);
            assertThat(op).isPresent();
            assertThat(op.get().hasChangePeer()).isTrue();
            assertThat(op.get().getChangePeerV2(0).getChangeType())
                    .isEqualTo(Pdpb.ConfChangeType.RemoveNode);
        } finally {
            scheduler.close();
        }
    }

    @Test
    void removesDownStorePeer() {
        var state = new InMemoryPdStateMachine();
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

        // Region with a peer on a Down store.
        state.updateRegion(Metapb.Region.newBuilder()
                .setId(100)
                .setStartKey(ByteString.copyFromUtf8("a"))
                .setEndKey(ByteString.EMPTY)
                .setRegionEpoch(Metapb.RegionEpoch.newBuilder().setConfVer(1).setVersion(1))
                .addPeers(peer(10, 1))
                .addPeers(peer(20, 2))
                .addPeers(peer(30, 3))
                .build());

        var ops = new OperatorQueue();
        var scheduler = new RuleCheckerScheduler(state, ops, new StoreStatsCache(), 60_000);
        try {
            int scheduled = scheduler.runOnce();
            assertThat(scheduled).isGreaterThan(0);

            var op = ops.poll(100);
            assertThat(op).isPresent();
            assertThat(op.get().hasChangePeer()).isTrue();
            // Should remove the peer on the Down store (store 3).
            assertThat(op.get().getChangePeer().getStoreId()).isEqualTo(3L);
            assertThat(op.get().getChangePeerV2(0).getChangeType())
                    .isEqualTo(Pdpb.ConfChangeType.RemoveNode);
        } finally {
            scheduler.close();
        }
    }

    @Test
    void noActionWhenProperlyreplicated() {
        var state = new InMemoryPdStateMachine();
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

        var ops = new OperatorQueue();
        var scheduler = new RuleCheckerScheduler(state, ops, new StoreStatsCache(), 60_000);
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
