package io.github.xinfra.lab.xkv.kv.raft;

import io.github.xinfra.lab.raft.Peer;
import io.github.xinfra.lab.xkv.kv.config.KvConfig;
import io.github.xinfra.lab.xkv.kv.engine.PerRegionRaftEngine;
import io.github.xinfra.lab.xkv.kv.engine.RocksStorageEngine;
import io.github.xinfra.lab.xkv.kv.engine.StorageEngine;
import io.github.xinfra.lab.xkv.proto.Metapb;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.time.Duration;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Phase 1 verification: a single-peer Raft group serves raw KV puts/deletes
 * end-to-end through the {@link BatchRegionPeer} apply loop.
 *
 * <p>Validates the v2 atomicity contract empirically: after every {@code
 * propose}, the data CF and the applied-index in the RAFT CF are visible
 * in the same RocksDB.
 */
final class SingleRegionRawKvTest {

    @TempDir Path dataDir;
    private RocksStorageEngine engine;
    private BatchRegionPeer peer;

    @BeforeEach
    void open() throws Exception {
        engine = RocksStorageEngine.open(dataDir, KvConfig.EngineConfig.defaults());
        peer = startPeer(engine, /* peerId= */ 1, /* regionId= */ 1);
        // Wait for the single-peer raft to elect itself.
        Awaitility.await()
                .atMost(Duration.ofSeconds(10))
                .until(peer::isLeader);
    }

    @AfterEach
    void close() {
        if (peer != null) peer.shutdown();
        if (engine != null) engine.close();
    }

    @Test
    void putAndReadBack() throws Exception {
        propose(peer, ProposalCodec.Kind.RAW_PUT, RawKvCodec.encodePut("k1".getBytes(), "v1".getBytes()));
        assertThat(engine.get(StorageEngine.Cf.DEFAULT, "k1".getBytes()))
                .isEqualTo("v1".getBytes());
    }

    @Test
    void putThenDelete() throws Exception {
        propose(peer, ProposalCodec.Kind.RAW_PUT, RawKvCodec.encodePut("k".getBytes(), "v".getBytes()));
        assertThat(engine.get(StorageEngine.Cf.DEFAULT, "k".getBytes())).isNotNull();

        propose(peer, ProposalCodec.Kind.RAW_DELETE, RawKvCodec.encodeDelete("k".getBytes()));
        assertThat(engine.get(StorageEngine.Cf.DEFAULT, "k".getBytes())).isNull();
    }

    @Test
    void appliedIndexAdvancesWithEveryProposal() throws Exception {
        var raft = new PerRegionRaftEngine(engine, 1);
        long startApplied = raft.appliedIndex();
        for (int i = 0; i < 10; i++) {
            propose(peer, ProposalCodec.Kind.RAW_PUT,
                    RawKvCodec.encodePut(("k" + i).getBytes(), ("v" + i).getBytes()));
        }
        // After all 10 proposals applied, applied-index has advanced and
        // the data is consistently in default CF.
        var raft2 = new PerRegionRaftEngine(engine, 1);   // reload from disk
        assertThat(raft2.appliedIndex()).isGreaterThanOrEqualTo(startApplied + 10);
        for (int i = 0; i < 10; i++) {
            assertThat(engine.get(StorageEngine.Cf.DEFAULT, ("k" + i).getBytes()))
                    .isEqualTo(("v" + i).getBytes());
        }
    }

    @Test
    void deleteRangeDropsContiguousKeys() throws Exception {
        for (int i = 0; i < 50; i++) {
            propose(peer, ProposalCodec.Kind.RAW_PUT,
                    RawKvCodec.encodePut(String.format("k%03d", i).getBytes(),
                            "v".getBytes()));
        }
        propose(peer, ProposalCodec.Kind.RAW_DELETE_RANGE,
                RawKvCodec.encodeDeleteRange("k010".getBytes(), "k040".getBytes()));

        assertThat(engine.get(StorageEngine.Cf.DEFAULT, "k009".getBytes())).isNotNull();
        assertThat(engine.get(StorageEngine.Cf.DEFAULT, "k010".getBytes())).isNull();
        assertThat(engine.get(StorageEngine.Cf.DEFAULT, "k039".getBytes())).isNull();
        assertThat(engine.get(StorageEngine.Cf.DEFAULT, "k040".getBytes())).isNotNull();
    }

    @Test
    void crashRecoveryPreservesAppliedData() throws Exception {
        // Apply 5 puts.
        for (int i = 0; i < 5; i++) {
            propose(peer, ProposalCodec.Kind.RAW_PUT,
                    RawKvCodec.encodePut(("k" + i).getBytes(), ("v" + i).getBytes()));
        }

        // Simulate crash: shut down peer + close engine without flushing.
        peer.shutdown();
        engine.close();

        // Reopen.
        engine = RocksStorageEngine.open(dataDir, KvConfig.EngineConfig.defaults());
        peer = startPeer(engine, 1, 1);
        Awaitility.await().atMost(Duration.ofSeconds(10)).until(peer::isLeader);

        // All five values must still be there (sync=true at apply time).
        for (int i = 0; i < 5; i++) {
            assertThat(engine.get(StorageEngine.Cf.DEFAULT, ("k" + i).getBytes()))
                    .as("key k" + i + " after restart")
                    .isEqualTo(("v" + i).getBytes());
        }

        // And new puts continue to work.
        propose(peer, ProposalCodec.Kind.RAW_PUT,
                RawKvCodec.encodePut("after-restart".getBytes(), "ok".getBytes()));
        assertThat(engine.get(StorageEngine.Cf.DEFAULT, "after-restart".getBytes()))
                .isEqualTo("ok".getBytes());
    }

    // ---- helpers ----

    private static BatchRegionPeer startPeer(RocksStorageEngine engine, long peerId, long regionId) {
        var raft = new PerRegionRaftEngine(engine, regionId);
        var region = Metapb.Region.newBuilder()
                .setId(regionId)
                .setStartKey(com.google.protobuf.ByteString.EMPTY)
                .setEndKey(com.google.protobuf.ByteString.EMPTY)
                .setRegionEpoch(Metapb.RegionEpoch.newBuilder().setConfVer(1).setVersion(1))
                .addPeers(Metapb.Peer.newBuilder().setId(peerId).setStoreId(1).setRole(Metapb.PeerRole.Voter))
                .build();
        var self = region.getPeers(0);

        return BatchRegionPeer.standalone(
                engine, raft, region, self,
                List.of(new Peer(peerId)),
                new LoopbackTransport(),
                new RawKvApplyHandler(),
                new RegionPeer.Settings(/* electionTick= */ 10, /* heartbeatTick= */ 1, /* heartbeatTickMs= */ 30));
    }

    private static void propose(BatchRegionPeer peer, ProposalCodec.Kind kind, byte[] payload) throws Exception {
        // Encode with proposeSeq=0; the peer rewrites those bytes with
        // the actual seq before calling node.propose.
        var envelope = ProposalCodec.encode(kind, 0, payload);
        var future = peer.propose(new RegionPeer.Proposal(envelope, 0, 0));
        var result = future.get(10, java.util.concurrent.TimeUnit.SECONDS);
        if (!result.success()) {
            throw new AssertionError("apply failed: " + result.errorMessage());
        }
    }
}
