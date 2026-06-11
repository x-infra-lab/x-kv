package io.github.xinfra.lab.xkv.test;

import io.github.xinfra.lab.xkv.kv.raft.MergeProtocol;
import io.github.xinfra.lab.xkv.kv.raft.RegionPeer;
import io.github.xinfra.lab.xkv.kv.store.MergeProtocolImpl;
import io.github.xinfra.lab.xkv.proto.Metapb;
import io.github.xinfra.lab.xkv.proto.PDGrpc;
import io.github.xinfra.lab.xkv.proto.Pdpb;
import io.grpc.ManagedChannel;
import io.grpc.Server;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.stub.StreamObserver;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Validates the critical safety invariant of MergeProtocolImpl:
 * rollback MUST be blocked when PD shows the target already committed
 * the merge (epoch version advanced).
 */
final class MergeProtocolSafetyTest {

    private Server fakePdServer;
    private ManagedChannel pdChannel;
    private final AtomicReference<Metapb.Region> pdTargetRegion = new AtomicReference<>();
    private String serverName;

    @BeforeEach
    void start() throws Exception {
        serverName = "merge-safety-" + UUID.randomUUID();
        fakePdServer = InProcessServerBuilder.forName(serverName)
                .directExecutor()
                .addService(new PDGrpc.PDImplBase() {
                    @Override
                    public void getRegionByID(Pdpb.GetRegionByIDRequest req,
                                              StreamObserver<Pdpb.GetRegionResponse> resp) {
                        var region = pdTargetRegion.get();
                        var rb = Pdpb.GetRegionResponse.newBuilder()
                                .setHeader(Pdpb.ResponseHeader.newBuilder());
                        if (region != null && region.getId() == req.getRegionId()) {
                            rb.setRegion(region);
                        }
                        resp.onNext(rb.build());
                        resp.onCompleted();
                    }
                })
                .build()
                .start();
        pdChannel = InProcessChannelBuilder.forName(serverName).directExecutor().build();
    }

    @AfterEach
    void teardown() {
        if (pdChannel != null) pdChannel.shutdownNow();
        if (fakePdServer != null) {
            fakePdServer.shutdownNow();
            try { fakePdServer.awaitTermination(2, TimeUnit.SECONDS); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
        }
    }

    @Test
    void rollbackBlockedWhenTargetEpochAdvanced() throws Exception {
        var sourceRegion = Metapb.Region.newBuilder()
                .setId(10).setStartKey(com.google.protobuf.ByteString.copyFromUtf8("a"))
                .setEndKey(com.google.protobuf.ByteString.copyFromUtf8("m"))
                .setRegionEpoch(Metapb.RegionEpoch.newBuilder().setConfVer(1).setVersion(1))
                .addPeers(Metapb.Peer.newBuilder().setId(10).setStoreId(1))
                .build();
        var targetRegion = Metapb.Region.newBuilder()
                .setId(20).setStartKey(com.google.protobuf.ByteString.copyFromUtf8("m"))
                .setEndKey(com.google.protobuf.ByteString.copyFromUtf8("z"))
                .setRegionEpoch(Metapb.RegionEpoch.newBuilder().setConfVer(1).setVersion(1))
                .addPeers(Metapb.Peer.newBuilder().setId(20).setStoreId(1))
                .build();

        // Simulate: after prepare, target's epoch is version=1.
        pdTargetRegion.set(targetRegion);

        var sourcePeer = new FakeRegionPeer(10, sourceRegion, true);
        var targetPeer = new FakeRegionPeer(20, targetRegion, true);

        MergeProtocolImpl.RegionPeerLocator locator = regionId -> {
            if (regionId == 10) return sourcePeer;
            if (regionId == 20) return targetPeer;
            return null;
        };

        var pdStub = PDGrpc.newBlockingStub(pdChannel);
        var protocol = new MergeProtocolImpl(locator, pdStub, 1, 5_000);

        // Prepare as source succeeds.
        protocol.prepareAsSource(10, 20).get(5, TimeUnit.SECONDS);
        assertThat(protocol.state(10)).isEqualTo(MergeProtocol.State.PREPARED_AS_SOURCE);

        // Simulate: target already committed (epoch version advanced to 2).
        pdTargetRegion.set(targetRegion.toBuilder()
                .setRegionEpoch(Metapb.RegionEpoch.newBuilder().setConfVer(1).setVersion(2))
                .build());

        // Rollback should be blocked — target already committed.
        assertThatThrownBy(() -> protocol.rollbackAsSource(10).get(5, TimeUnit.SECONDS))
                .isInstanceOf(ExecutionException.class)
                .rootCause()
                .hasMessageContaining("cannot rollback")
                .hasMessageContaining("already committed");

        // State should NOT advance to ROLLED_BACK — still PREPARED_AS_SOURCE.
        assertThat(protocol.state(10)).isEqualTo(MergeProtocol.State.PREPARED_AS_SOURCE);
    }

    @Test
    void rollbackSucceedsWhenTargetEpochUnchanged() throws Exception {
        var sourceRegion = Metapb.Region.newBuilder()
                .setId(30).setStartKey(com.google.protobuf.ByteString.copyFromUtf8("a"))
                .setEndKey(com.google.protobuf.ByteString.copyFromUtf8("m"))
                .setRegionEpoch(Metapb.RegionEpoch.newBuilder().setConfVer(1).setVersion(1))
                .addPeers(Metapb.Peer.newBuilder().setId(30).setStoreId(1))
                .build();
        var targetRegion = Metapb.Region.newBuilder()
                .setId(40).setStartKey(com.google.protobuf.ByteString.copyFromUtf8("m"))
                .setEndKey(com.google.protobuf.ByteString.copyFromUtf8("z"))
                .setRegionEpoch(Metapb.RegionEpoch.newBuilder().setConfVer(1).setVersion(1))
                .addPeers(Metapb.Peer.newBuilder().setId(40).setStoreId(1))
                .build();

        // Target epoch stays at version=1.
        pdTargetRegion.set(targetRegion);

        var sourcePeer = new FakeRegionPeer(30, sourceRegion, true);
        var targetPeer = new FakeRegionPeer(40, targetRegion, true);

        MergeProtocolImpl.RegionPeerLocator locator = regionId -> {
            if (regionId == 30) return sourcePeer;
            if (regionId == 40) return targetPeer;
            return null;
        };

        var pdStub = PDGrpc.newBlockingStub(pdChannel);
        var protocol = new MergeProtocolImpl(locator, pdStub, 1, 5_000);

        protocol.prepareAsSource(30, 40).get(5, TimeUnit.SECONDS);
        assertThat(protocol.state(30)).isEqualTo(MergeProtocol.State.PREPARED_AS_SOURCE);

        // Rollback should succeed because target epoch is unchanged.
        protocol.rollbackAsSource(30).get(5, TimeUnit.SECONDS);
        assertThat(protocol.state(30)).isEqualTo(MergeProtocol.State.ROLLED_BACK);
    }

    /**
     * Minimal fake RegionPeer that always succeeds proposals.
     */
    private static class FakeRegionPeer implements RegionPeer {
        private final long regionId;
        private final Metapb.Region region;
        private final boolean leader;

        FakeRegionPeer(long regionId, Metapb.Region region, boolean leader) {
            this.regionId = regionId;
            this.region = region;
            this.leader = leader;
        }

        @Override public long regionId() { return regionId; }
        @Override public Metapb.Region region() { return region; }
        @Override public Metapb.Peer self() { return region.getPeers(0); }
        @Override public void updateRegion(Metapb.Region r) {}
        @Override public boolean isLeader() { return leader; }
        @Override public boolean isDestroyed() { return false; }
        @Override public long firstIndex() { return 1; }
        @Override public long appliedIndex() { return 1; }
        @Override public void maybeGenerateSnapshot() {}

        @Override
        public CompletableFuture<ApplyResult> propose(Proposal p) {
            return CompletableFuture.completedFuture(ApplyResult.ok(new byte[0]));
        }

        @Override
        public CompletableFuture<ApplyResult> proposeAdmin(AdminProposal p) {
            return CompletableFuture.completedFuture(ApplyResult.ok(new byte[0]));
        }

        @Override
        public CompletableFuture<ApplyResult> proposeConfChange(
                io.github.xinfra.lab.raft.proto.Eraftpb.ConfChangeV2 cc) {
            return CompletableFuture.completedFuture(ApplyResult.ok(new byte[0]));
        }

        @Override public CompletableFuture<Void> readIndex() {
            return CompletableFuture.completedFuture(null);
        }

        @Override public void shutdown() {}
        @Override public void transferLeader(long targetPeerId) {}
    }
}
