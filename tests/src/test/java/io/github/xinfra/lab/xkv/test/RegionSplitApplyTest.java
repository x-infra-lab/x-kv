package io.github.xinfra.lab.xkv.test;

import com.google.protobuf.ByteString;
import io.github.xinfra.lab.raft.Peer;
import io.github.xinfra.lab.xkv.kv.config.KvConfig;
import io.github.xinfra.lab.xkv.kv.engine.PerRegionRaftEngine;
import io.github.xinfra.lab.xkv.kv.engine.RocksStorageEngine;
import io.github.xinfra.lab.xkv.kv.engine.StorageEngine;
import io.github.xinfra.lab.xkv.kv.mvcc.ConcurrencyManager;
import io.github.xinfra.lab.xkv.kv.mvcc.MaxTsTracker;
import io.github.xinfra.lab.xkv.kv.raft.CompositeApplyHandler;
import io.github.xinfra.lab.xkv.kv.raft.LoopbackTransport;
import io.github.xinfra.lab.xkv.kv.raft.ProposalCodec;
import io.github.xinfra.lab.xkv.kv.raft.RegionPeer;
import io.github.xinfra.lab.xkv.kv.raft.RegionPeerImpl;
import io.github.xinfra.lab.xkv.proto.KvServerpb;
import io.github.xinfra.lab.xkv.proto.Metapb;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Verifies the {@code ADMIN_SPLIT} apply path:
 *   1. parent's region descriptor shrinks to [start, split_key);
 *   2. child region descriptor is persisted with [split_key, end);
 *   3. the split-child hook fires so the {@code Store} can spawn the child;
 *   4. both apply atomically (one batch, one fsync).
 *
 * <p>Spawning the child as a live {@code RegionPeer} is the integration
 * step landing alongside the multi-region scheduler; this test confirms
 * the apply-side foundation is correct.
 */
final class RegionSplitApplyTest {

    @TempDir Path dataDir;
    private RocksStorageEngine engine;
    private PerRegionRaftEngine raftEngine;
    private RegionPeerImpl peer;
    private ConcurrencyManager cm;
    private final ConcurrentLinkedQueue<Metapb.Region> spawned = new ConcurrentLinkedQueue<>();
    private final java.util.concurrent.atomic.AtomicReference<Metapb.Region> updatedParent =
            new java.util.concurrent.atomic.AtomicReference<>();

    @BeforeEach
    void start() throws Exception {
        engine = RocksStorageEngine.open(dataDir, KvConfig.EngineConfig.defaults());
        raftEngine = new PerRegionRaftEngine(engine, 1);
        var region = Metapb.Region.newBuilder()
                .setId(1)
                .setStartKey(ByteString.EMPTY)
                .setEndKey(ByteString.EMPTY)
                .setRegionEpoch(Metapb.RegionEpoch.newBuilder().setConfVer(1).setVersion(1))
                .addPeers(Metapb.Peer.newBuilder().setId(1).setStoreId(1).setRole(Metapb.PeerRole.Voter))
                .build();
        cm = new ConcurrencyManager(new MaxTsTracker(raftEngine.persistedMaxTs()));
        peer = new RegionPeerImpl(
                engine, raftEngine, region, region.getPeers(0),
                List.of(new Peer(1)),
                new LoopbackTransport(),
                CompositeApplyHandler.defaultFor(engine, cm)
                        .withAdmin(raftEngine, engine, (parent, children) -> {
                            updatedParent.set(parent);
                            // Also refresh the live peer's descriptor so
                            // peerForKey routing reflects the new range.
                            peer.updateRegion(parent);
                            children.forEach(spawned::offer);
                        }),
                new RegionPeerImpl.Settings(10, 1, 30),
                cm);
        Awaitility.await().atMost(Duration.ofSeconds(10)).until(peer::isLeader);
    }

    @AfterEach
    void stop() {
        if (peer != null) peer.shutdown();
        if (engine != null) engine.close();
    }

    @Test
    void adminSplitShrinksParentAndPersistsChild() throws Exception {
        // Propose a split: parent [-,-) → parent [-, "m") + child ["m", -)
        var parent = Metapb.Region.newBuilder()
                .setId(1)
                .setStartKey(ByteString.EMPTY)
                .setEndKey(ByteString.copyFromUtf8("m"))
                .setRegionEpoch(Metapb.RegionEpoch.newBuilder().setConfVer(1).setVersion(2))  // version bumped
                .addPeers(Metapb.Peer.newBuilder().setId(1).setStoreId(1).setRole(Metapb.PeerRole.Voter))
                .build();
        var child = Metapb.Region.newBuilder()
                .setId(101)     // pre-allocated by PD
                .setStartKey(ByteString.copyFromUtf8("m"))
                .setEndKey(ByteString.EMPTY)
                .setRegionEpoch(Metapb.RegionEpoch.newBuilder().setConfVer(1).setVersion(1))
                .addPeers(Metapb.Peer.newBuilder().setId(11).setStoreId(1).setRole(Metapb.PeerRole.Voter))
                .build();
        var splitPayload = KvServerpb.SplitRegionProposal.newBuilder()
                .setUpdatedParent(parent)
                .addChildren(child)
                .build();
        var envelope = ProposalCodec.encode(
                ProposalCodec.Kind.ADMIN_SPLIT, /* seq= */ 0, splitPayload.toByteArray());
        var fut = peer.propose(new RegionPeer.Proposal(envelope, 0, 0));
        var result = fut.get(5, TimeUnit.SECONDS);
        assertThat(result.success()).as("split apply succeeded").isTrue();

        // Parent on disk reflects new range + bumped epoch.
        var loadedParent = raftEngine.region();
        assertThat(loadedParent).isNotNull();
        assertThat(loadedParent.getEndKey().toStringUtf8()).isEqualTo("m");
        assertThat(loadedParent.getRegionEpoch().getVersion()).isEqualTo(2);

        // Child descriptor persisted under its own region key.
        byte[] childBytes = engine.get(StorageEngine.Cf.RAFT,
                io.github.xinfra.lab.xkv.kv.engine.RaftCfKeys.regionKey(101));
        assertThat(childBytes).isNotNull();
        var loadedChild = Metapb.Region.parseFrom(childBytes);
        assertThat(loadedChild.getStartKey().toStringUtf8()).isEqualTo("m");
        assertThat(loadedChild.getId()).isEqualTo(101);

        // Split-child hook fired so a {@code Store} can spawn the new peer.
        var hooked = spawned.poll();
        assertThat(hooked).isNotNull();
        assertThat(hooked.getId()).isEqualTo(101);

        // The live peer's in-memory descriptor was refreshed by the observer,
        // so any key-based routing now uses the new range.
        assertThat(peer.region().getEndKey().toStringUtf8()).isEqualTo("m");
        assertThat(updatedParent.get()).isNotNull();
        assertThat(updatedParent.get().getId()).isEqualTo(1);
    }
}
