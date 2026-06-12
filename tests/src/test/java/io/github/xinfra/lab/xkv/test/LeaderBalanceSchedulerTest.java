package io.github.xinfra.lab.xkv.test;

import com.google.protobuf.ByteString;
import io.github.xinfra.lab.xkv.pd.state.InMemoryPdStateMachine;
import io.github.xinfra.lab.xkv.pd.state.LeaderBalanceScheduler;
import io.github.xinfra.lab.xkv.pd.state.OperatorControllerImpl;
import io.github.xinfra.lab.xkv.pd.state.OperatorQueue;
import io.github.xinfra.lab.xkv.proto.Metapb;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.HashSet;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit-level test for {@link LeaderBalanceScheduler}.
 *
 * <p>Drives the scheduler against a hand-built {@link InMemoryPdStateMachine}
 * holding regions whose first peer (== leader by convention) is heavily
 * skewed toward one store. The scheduler must enqueue TransferLeader
 * operators that move leadership toward the under-loaded store.
 */
final class LeaderBalanceSchedulerTest {

    @Test
    void enqueuesTransferLeadersUntilLoadIsWithinOne() {
        var state = new InMemoryPdStateMachine();
        // 3 stores.
        for (long s = 1; s <= 3; s++) {
            state.putStore(Metapb.Store.newBuilder().setId(s).build());
        }
        // Bootstrap the cluster so subsequent updateRegion isn't rejected.
        state.bootstrap(
                Metapb.Store.newBuilder().setId(1).build(),
                bootstrapRegion());

        // 6 child regions, ALL led by store 1 (skewed). Each has peers on
        // all 3 stores so the scheduler has somewhere to move leadership.
        for (long i = 0; i < 6; i++) {
            long regionId = 100 + i;
            byte[] start = ("k" + i).getBytes();
            byte[] end = i + 1 < 6 ? ("k" + (i + 1)).getBytes() : new byte[0];
            state.updateRegion(Metapb.Region.newBuilder()
                    .setId(regionId)
                    .setStartKey(ByteString.copyFrom(start))
                    .setEndKey(ByteString.copyFrom(end))
                    .setRegionEpoch(Metapb.RegionEpoch.newBuilder().setConfVer(1).setVersion(1))
                    // peer[0] is leader by convention.
                    .addPeers(peer(/* id= */ 10 + i, /* storeId= */ 1))
                    .addPeers(peer(/* id= */ 20 + i, /* storeId= */ 2))
                    .addPeers(peer(/* id= */ 30 + i, /* storeId= */ 3))
                    .build());
        }

        var ops = new OperatorQueue();
        var controller = new OperatorControllerImpl(ops, 64, 600_000);
        var scheduler = new LeaderBalanceScheduler(state, controller, /* intervalMs= */ 60_000);
        try {
            // First round: caps at MAX_OPERATORS_PER_TICK=4 transfers.
            int round1 = scheduler.runOnce();
            assertThat(round1).isGreaterThan(0);
            assertThat(round1).isLessThanOrEqualTo(LeaderBalanceScheduler.MAX_OPERATORS_PER_TICK);

            // Each transfer was enqueued targeting a peer on the under-loaded
            // store(s). Pop them, verify their shape, then apply them
            // manually to the state (simulating what RegionHeartbeater +
            // raft would do in a live cluster).
            for (long regionId = 100; regionId < 106; regionId++) {
                while (true) {
                    var op = ops.poll(regionId);
                    if (op.isEmpty()) break;
                    var hbResp = op.get();
                    assertThat(hbResp.hasTransferLeader()).isTrue();
                    long target = hbResp.getTransferLeader().getId();
                    // Apply: reorder region's peers so the target is first
                    // (== new leader by our convention).
                    var region = state.getRegion(regionId).orElseThrow();
                    var b = region.toBuilder().clearPeers();
                    region.getPeersList().stream()
                            .filter(p -> p.getId() == target)
                            .forEach(b::addPeers);
                    region.getPeersList().stream()
                            .filter(p -> p.getId() != target)
                            .forEach(b::addPeers);
                    state.updateRegion(b.build());
                }
            }

            // After 2-3 rounds the cluster should be balanced (each store
            // hosts 2 leaders).
            for (int i = 0; i < 5; i++) {
                int extra = scheduler.runOnce();
                if (extra == 0) break;
                for (long regionId = 100; regionId < 106; regionId++) {
                    while (true) {
                        var op = ops.poll(regionId);
                        if (op.isEmpty()) break;
                        long target = op.get().getTransferLeader().getId();
                        var region = state.getRegion(regionId).orElseThrow();
                        var b = region.toBuilder().clearPeers();
                        region.getPeersList().stream()
                                .filter(p -> p.getId() == target).forEach(b::addPeers);
                        region.getPeersList().stream()
                                .filter(p -> p.getId() != target).forEach(b::addPeers);
                        state.updateRegion(b.build());
                    }
                }
            }

            // Final distribution: max-min should be 0 or 1.
            var counts = new HashMap<Long, Integer>();
            for (var r : state.allRegions()) {
                if (r.getId() < 100) continue;   // ignore bootstrap region
                if (r.getPeersCount() == 0) continue;
                counts.merge(r.getPeers(0).getStoreId(), 1, Integer::sum);
            }
            int max = counts.values().stream().mapToInt(Integer::intValue).max().orElse(0);
            int min = counts.values().stream().mapToInt(Integer::intValue).min().orElse(0);
            assertThat(max - min)
                    .as("after convergence, leader spread is ≤ 1 (counts=%s)", counts)
                    .isLessThanOrEqualTo(1);
            // All 3 stores host at least one leader (i.e., no store starved).
            assertThat(counts.keySet()).contains(1L, 2L, 3L);
        } finally {
            scheduler.close();
        }
    }

    private Metapb.Region bootstrapRegion() {
        return Metapb.Region.newBuilder()
                .setId(1)
                .setStartKey(ByteString.EMPTY)
                .setEndKey(ByteString.copyFromUtf8("k0"))     // adjacent to child range start
                .setRegionEpoch(Metapb.RegionEpoch.newBuilder().setConfVer(1).setVersion(1))
                .addPeers(peer(1, 1))
                .build();
    }

    private static Metapb.Peer peer(long peerId, long storeId) {
        return Metapb.Peer.newBuilder().setId(peerId).setStoreId(storeId)
                .setRole(Metapb.PeerRole.Voter).build();
    }

    /** Suppress unused warning. */
    @SuppressWarnings("unused")
    private static HashSet<Long> ids() { return new HashSet<>(); }
}
