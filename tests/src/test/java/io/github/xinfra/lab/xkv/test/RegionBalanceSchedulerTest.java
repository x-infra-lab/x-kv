package io.github.xinfra.lab.xkv.test;

import com.google.protobuf.ByteString;
import io.github.xinfra.lab.xkv.pd.state.InMemoryPdStateMachine;
import io.github.xinfra.lab.xkv.pd.state.OperatorControllerImpl;
import io.github.xinfra.lab.xkv.pd.state.OperatorQueue;
import io.github.xinfra.lab.xkv.pd.state.RegionBalanceScheduler;
import io.github.xinfra.lab.xkv.proto.Metapb;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit-level test for {@link RegionBalanceScheduler}: regions are skewed
 * (all on stores 1+2, none on store 3); scheduler enqueues AddPeer
 * operators targeting store 3.
 */
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

        // 4 regions hosted on stores 1+2 only — store 3 has zero replicas.
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

        var ops = new OperatorQueue();
        var controller = new OperatorControllerImpl(ops, 64, 600_000);
        var scheduler = new RegionBalanceScheduler(state, controller, /* intervalMs= */ 60_000);
        try {
            int round1 = scheduler.runOnce();
            assertThat(round1).isGreaterThan(0);
            // Drain every region's queue — the bootstrap region (id=1) is
            // also a balance candidate since it currently sits only on
            // store 1.
            int targetingStore3 = 0;
            for (long regionId : new long[]{ 1, 100, 101, 102, 103 }) {
                while (true) {
                    var op = ops.poll(regionId);
                    if (op.isEmpty()) break;
                    assertThat(op.get().hasChangePeer()).isTrue();
                    var newPeer = op.get().getChangePeer();
                    assertThat(newPeer.getStoreId())
                            .as("AddPeer must target the under-loaded store")
                            .isEqualTo(3L);
                    targetingStore3++;
                }
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
