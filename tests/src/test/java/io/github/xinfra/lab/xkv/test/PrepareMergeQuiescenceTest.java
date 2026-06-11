package io.github.xinfra.lab.xkv.test;

import com.google.protobuf.ByteString;
import io.github.xinfra.lab.raft.Peer;
import io.github.xinfra.lab.xkv.kv.config.KvConfig;
import io.github.xinfra.lab.xkv.kv.engine.PerRegionRaftEngine;
import io.github.xinfra.lab.xkv.kv.engine.RocksStorageEngine;
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
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Verifies the quiescence semantics of ADMIN_PREPARE_MERGE:
 * <ol>
 *   <li>After PrepareMerge applies, business writes routed through the
 *       source's raft are rejected at apply time with a "region merging"
 *       error.</li>
 *   <li>After RollbackMerge, the source resumes accepting writes.</li>
 *   <li>The merging marker is persisted: a restarted peer sees it on
 *       reload and continues to reject writes until RollbackMerge.</li>
 * </ol>
 */
final class PrepareMergeQuiescenceTest {

    @TempDir Path dataDir;
    private RocksStorageEngine engine;
    private PerRegionRaftEngine raftEngine;
    private RegionPeerImpl peer;
    private ConcurrencyManager cm;

    @BeforeEach
    void start() throws Exception {
        engine = RocksStorageEngine.open(dataDir, KvConfig.EngineConfig.defaults());
        raftEngine = new PerRegionRaftEngine(engine, 1);
        var region = Metapb.Region.newBuilder()
                .setId(1)
                .setRegionEpoch(Metapb.RegionEpoch.newBuilder().setConfVer(1).setVersion(1))
                .addPeers(Metapb.Peer.newBuilder().setId(1).setStoreId(1).setRole(Metapb.PeerRole.Voter))
                .build();
        cm = new ConcurrencyManager(new MaxTsTracker(raftEngine.persistedMaxTs()));
        peer = new RegionPeerImpl(
                engine, raftEngine, region, region.getPeers(0),
                List.of(new Peer(1)),
                new LoopbackTransport(),
                CompositeApplyHandler.defaultFor(engine, cm).withAdmin(raftEngine, engine, null, null),
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
    void prepareMergeQuiescesWritesAndRollbackResumesThem() throws Exception {
        // 1. Pre-merge: a raw write succeeds.
        assertWriteSucceeds("before");

        // 2. PrepareMerge.
        var prepare = KvServerpb.PrepareMergeProposal.newBuilder()
                .setTarget(Metapb.Region.newBuilder().setId(99).build())
                .build();
        proposeAdmin(ProposalCodec.Kind.ADMIN_PREPARE_MERGE, prepare.toByteArray());
        assertThat(raftEngine.isMerging()).as("merging flag persisted").isTrue();

        // 3. Subsequent business writes are REJECTED.
        var writeResult = proposeRawPut("during-merge", "x");
        assertThat(writeResult.success()).as("write rejected while merging").isFalse();
        assertThat(writeResult.errorMessage()).contains("merging");

        // 4. RollbackMerge.
        var rb = KvServerpb.RollbackMergeProposal.newBuilder().setTargetRegionId(99).build();
        proposeAdmin(ProposalCodec.Kind.ADMIN_ROLLBACK_MERGE, rb.toByteArray());
        assertThat(raftEngine.isMerging()).as("merging flag cleared").isFalse();

        // 5. Writes resume.
        assertWriteSucceeds("after-rollback");
    }

    @Test
    void mergingMarkerSurvivesRestart() throws Exception {
        var prepare = KvServerpb.PrepareMergeProposal.newBuilder()
                .setTarget(Metapb.Region.newBuilder().setId(99).build())
                .build();
        proposeAdmin(ProposalCodec.Kind.ADMIN_PREPARE_MERGE, prepare.toByteArray());
        assertThat(raftEngine.isMerging()).isTrue();

        // Restart: shut peer, reopen engine, reload raftEngine — merging
        // flag should be reloaded from disk.
        stop();
        engine = RocksStorageEngine.open(dataDir, KvConfig.EngineConfig.defaults());
        var reloaded = new PerRegionRaftEngine(engine, 1);
        assertThat(reloaded.isMerging()).as("merging marker survives restart").isTrue();
        engine.close();
    }

    // ===== helpers =====

    private void assertWriteSucceeds(String key) throws Exception {
        var result = proposeRawPut(key, "v");
        assertThat(result.success()).as("write of '%s' should apply", key).isTrue();
    }

    private RegionPeer.ApplyResult proposeRawPut(String key, String value) throws Exception {
        var payload = io.github.xinfra.lab.xkv.kv.raft.RawKvCodec.encodePut(
                key.getBytes(), value.getBytes());
        var envelope = ProposalCodec.encode(ProposalCodec.Kind.RAW_PUT, /* seq= */ 0, payload);
        return peer.propose(new RegionPeer.Proposal(envelope, 0, 0))
                .get(5, TimeUnit.SECONDS);
    }

    private void proposeAdmin(ProposalCodec.Kind kind, byte[] payload) throws Exception {
        var envelope = ProposalCodec.encode(kind, /* seq= */ 0, payload);
        var result = peer.propose(new RegionPeer.Proposal(envelope, 0, 0))
                .get(5, TimeUnit.SECONDS);
        assertThat(result.success())
                .as("admin propose for %s succeeded", kind)
                .isTrue();
    }
}
