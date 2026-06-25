package io.github.xinfra.lab.xkv.test;

import com.google.protobuf.ByteString;
import io.github.xinfra.lab.xkv.pd.state.InMemoryPdStateMachine;
import io.github.xinfra.lab.xkv.pd.state.OperatorControllerImpl;
import io.github.xinfra.lab.xkv.pd.state.RegionBalanceScheduler;
import io.github.xinfra.lab.xkv.pd.state.SimpleOperator;
import io.github.xinfra.lab.xkv.proto.Metapb;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

final class RegionBalanceSchedulerTest {

    @Test
    void enqueuesAddPeerForUnderLoadedStore() {
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

        for (long i = 0; i < 4; i++) {
            state.updateRegion(Metapb.Region.newBuilder()
                    .setId(100 + i)
                    .setStartKey(ByteString.copyFromUtf8("k" + i))
                    .setEndKey(i + 1 < 4 ? ByteString.copyFromUtf8("k" + (i + 1)) : ByteString.EMPTY)
                    .setRegionEpoch(Metapb.RegionEpoch.newBuilder().setConfVer(1).setVersion(1))
                    .addPeers(peer(10 + i, 1))
                    .addPeers(peer(20 + i, 2))
                    .build());
        }

        var controller = new OperatorControllerImpl(64, 600_000);
        var scheduler = new RegionBalanceScheduler(state, controller, 60_000);
        try {
            int round1 = scheduler.runOnce();
            assertThat(round1).isGreaterThan(0);

            int targetingStore3 = 0;
            for (long regionId : new long[]{ 1, 100, 101, 102, 103 }) {
                var op = controller.getOperator(regionId);
                if (op.isEmpty()) continue;
                var resp = ((SimpleOperator) op.get()).response();
                assertThat(resp.hasChangePeer()).isTrue();
                var newPeer = resp.getChangePeer();
                assertThat(newPeer.getStoreId())
                        .as("AddPeer must target the under-loaded store")
                        .isEqualTo(3L);
                targetingStore3++;
            }
            assertThat(targetingStore3).isEqualTo(round1);
        } finally {
            scheduler.close();
        }
    }

    private static Metapb.Peer peer(long peerId, long storeId) {
        return Metapb.Peer.newBuilder().setId(peerId).setStoreId(storeId)
                .setRole(Metapb.PeerRole.Voter).build();
    }
}
