package io.github.xinfra.lab.xkv.test;

import com.google.protobuf.ByteString;
import io.github.xinfra.lab.raft.Peer;
import io.github.xinfra.lab.raft.proto.Eraftpb;
import io.github.xinfra.lab.xkv.kv.config.KvConfig;
import io.github.xinfra.lab.xkv.kv.engine.PerRegionRaftEngine;
import io.github.xinfra.lab.xkv.kv.engine.RocksStorageEngine;
import io.github.xinfra.lab.xkv.kv.mvcc.ConcurrencyManager;
import io.github.xinfra.lab.xkv.kv.mvcc.MaxTsTracker;
import io.github.xinfra.lab.xkv.kv.raft.CompositeApplyHandler;
import io.github.xinfra.lab.xkv.kv.raft.LoopbackTransport;
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
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Verifies the conf-change apply path: propose AddNode → region descriptor
 * grows a peer, conf_ver bumps, observer fires with the new peer.
 *
 * <p>Single-peer test (no real distributed conf-change) — proves the
 * descriptor / observer / future plumbing without needing the second peer
 * to actually exist (which would require cross-store coordination).
 */
final class ChangePeerApplyTest {

    @TempDir Path dataDir;
    private RocksStorageEngine engine;
    private PerRegionRaftEngine raftEngine;
    private RegionPeerImpl peer;
    private final AtomicReference<Metapb.Peer> observedNewPeer = new AtomicReference<>();
    private final AtomicReference<Eraftpb.ConfChangeType> observedType = new AtomicReference<>();

    @BeforeEach
    void start() throws Exception {
        engine = RocksStorageEngine.open(dataDir, KvConfig.EngineConfig.defaults());
        raftEngine = new PerRegionRaftEngine(engine, 1);
        var region = Metapb.Region.newBuilder()
                .setId(1)
                .setRegionEpoch(Metapb.RegionEpoch.newBuilder().setConfVer(1).setVersion(1))
                .addPeers(Metapb.Peer.newBuilder().setId(1).setStoreId(1).setRole(Metapb.PeerRole.Voter))
                .build();
        var cm = new ConcurrencyManager(new MaxTsTracker(raftEngine.persistedMaxTs()));
        peer = new RegionPeerImpl(
                engine, raftEngine, region, region.getPeers(0),
                List.of(new Peer(1)),
                new LoopbackTransport(),
                CompositeApplyHandler.defaultFor(engine, cm).withAdmin(raftEngine),
                new RegionPeerImpl.Settings(10, 1, 30),
                cm);
        peer.setChangePeerObserver((type, p, updatedRegion) -> {
            observedType.set(type);
            observedNewPeer.set(p);
        });
        Awaitility.await().atMost(Duration.ofSeconds(10)).until(peer::isLeader);
    }

    @AfterEach
    void stop() {
        if (peer != null) peer.shutdown();
        if (engine != null) engine.close();
    }

    @Test
    void addNodeUpdatesRegionDescriptorAndFiresObserver() throws Exception {
        // Propose AddNode for a new peer (id=99) on storeId=99.
        var newPeer = Metapb.Peer.newBuilder()
                .setId(99).setStoreId(99).setRole(Metapb.PeerRole.Voter).build();
        var ctx = KvServerpb.ConfChangeContext.newBuilder().addPeers(newPeer).build();
        var cc = Eraftpb.ConfChangeV2.newBuilder()
                .addChanges(Eraftpb.ConfChangeSingle.newBuilder()
                        .setType(Eraftpb.ConfChangeType.ConfChangeAddNode)
                        .setNodeId(99))
                .setContext(ByteString.copyFrom(ctx.toByteArray()))
                .build();
        long beforeConfVer = peer.region().getRegionEpoch().getConfVer();

        var fut = peer.proposeConfChange(cc);
        var result = fut.get(5, TimeUnit.SECONDS);
        assertThat(result.success()).isTrue();

        // Region descriptor grew the new peer + conf_ver bumped.
        var after = peer.region();
        assertThat(after.getPeersCount()).isEqualTo(2);
        assertThat(after.getPeersList().stream().anyMatch(p -> p.getId() == 99)).isTrue();
        assertThat(after.getRegionEpoch().getConfVer()).isEqualTo(beforeConfVer + 1);

        // Observer fired with the right peer + change type.
        assertThat(observedType.get()).isEqualTo(Eraftpb.ConfChangeType.ConfChangeAddNode);
        assertThat(observedNewPeer.get().getId()).isEqualTo(99);
        assertThat(observedNewPeer.get().getStoreId()).isEqualTo(99);
    }

    @Test
    void addLearnerNodeMarksLearnerRole() throws Exception {
        // Adding a LEARNER doesn't affect quorum, so this works in a
        // single-peer test cluster without a real second peer responding.
        var learnerPeer = Metapb.Peer.newBuilder()
                .setId(77).setStoreId(77).setRole(Metapb.PeerRole.Learner).build();
        var ctx = KvServerpb.ConfChangeContext.newBuilder().addPeers(learnerPeer).build();
        var cc = Eraftpb.ConfChangeV2.newBuilder()
                .addChanges(Eraftpb.ConfChangeSingle.newBuilder()
                        .setType(Eraftpb.ConfChangeType.ConfChangeAddLearnerNode).setNodeId(77))
                .setContext(ByteString.copyFrom(ctx.toByteArray()))
                .build();
        var result = peer.proposeConfChange(cc).get(5, TimeUnit.SECONDS);
        assertThat(result.success()).isTrue();

        var added = peer.region().getPeersList().stream()
                .filter(p -> p.getId() == 77).findFirst().orElseThrow();
        assertThat(added.getRole()).isEqualTo(Metapb.PeerRole.Learner);
        assertThat(observedType.get()).isEqualTo(Eraftpb.ConfChangeType.ConfChangeAddLearnerNode);
    }
}
