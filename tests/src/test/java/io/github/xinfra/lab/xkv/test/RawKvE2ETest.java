package io.github.xinfra.lab.xkv.test;

import com.google.protobuf.ByteString;
import io.github.xinfra.lab.raft.Peer;
import io.github.xinfra.lab.xkv.kv.config.KvConfig;
import io.github.xinfra.lab.xkv.kv.engine.PerRegionRaftEngine;
import io.github.xinfra.lab.xkv.kv.engine.RocksStorageEngine;
import io.github.xinfra.lab.xkv.kv.raft.LoopbackTransport;
import io.github.xinfra.lab.xkv.kv.raft.RawKvApplyHandler;
import io.github.xinfra.lab.xkv.kv.raft.RegionPeer;
import io.github.xinfra.lab.xkv.kv.raft.RegionPeerImpl;
import io.github.xinfra.lab.xkv.kv.server.RawKvService;
import io.github.xinfra.lab.xkv.kv.server.TikvServiceImpl;
import io.github.xinfra.lab.xkv.proto.Kvrpcpb;
import io.github.xinfra.lab.xkv.proto.Metapb;
import io.github.xinfra.lab.xkv.proto.TikvGrpc;
import io.grpc.ManagedChannel;
import io.grpc.Server;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Phase 1 end-to-end verification: a client speaking the
 * {@code Tikv} gRPC service against a single-region single-peer KV node
 * sees:
 * <ul>
 *   <li>Linearizable RawPut → RawGet</li>
 *   <li>RawScan returns inserted keys in order</li>
 *   <li>RawDelete + RawDeleteRange remove their target keys</li>
 *   <li>BatchGet / BatchPut / BatchDelete</li>
 *   <li>Crash recovery preserves applied state</li>
 * </ul>
 *
 * <p>This is the load-bearing Phase 1 verification — every layer from the
 * client gRPC stub down through the apply loop down through the WriteBatch
 * is exercised end-to-end.
 */
final class RawKvE2ETest {

    @TempDir Path dataDir;

    private RocksStorageEngine engine;
    private RegionPeerImpl peer;
    private Server grpcServer;
    private ManagedChannel channel;
    private TikvGrpc.TikvBlockingStub tikv;
    private String channelName;

    private void start() throws Exception {
        engine = RocksStorageEngine.open(dataDir, KvConfig.EngineConfig.defaults());
        peer = startPeer(engine);
        Awaitility.await().atMost(Duration.ofSeconds(10)).until(peer::isLeader);

        var rawKv = new RawKvService(engine, key -> peer, /* timeoutMs= */ 5_000);

        channelName = "test-" + UUID.randomUUID();
        grpcServer = InProcessServerBuilder.forName(channelName)
                .directExecutor()
                .addService(new TikvServiceImpl(rawKv))
                .build()
                .start();
        channel = InProcessChannelBuilder.forName(channelName).directExecutor().build();
        tikv = TikvGrpc.newBlockingStub(channel);
    }

    @AfterEach
    void teardown() {
        if (channel != null) channel.shutdownNow();
        if (grpcServer != null) {
            grpcServer.shutdownNow();
            try { grpcServer.awaitTermination(2, TimeUnit.SECONDS); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
        }
        if (peer != null) peer.shutdown();
        if (engine != null) engine.close();
    }

    @org.junit.jupiter.api.Test
    void putGetDeleteRoundTrip() throws Exception {
        start();

        // Put k → v
        var putResp = tikv.rawPut(Kvrpcpb.RawPutRequest.newBuilder()
                .setKey(ByteString.copyFromUtf8("k"))
                .setValue(ByteString.copyFromUtf8("v"))
                .build());
        assertThat(putResp.getError()).isEmpty();

        // Get k → v
        var getResp = tikv.rawGet(Kvrpcpb.RawGetRequest.newBuilder()
                .setKey(ByteString.copyFromUtf8("k")).build());
        assertThat(getResp.getNotFound()).isFalse();
        assertThat(getResp.getValue().toStringUtf8()).isEqualTo("v");

        // Delete k
        var delResp = tikv.rawDelete(Kvrpcpb.RawDeleteRequest.newBuilder()
                .setKey(ByteString.copyFromUtf8("k")).build());
        assertThat(delResp.getError()).isEmpty();

        // Get k → not found
        var getResp2 = tikv.rawGet(Kvrpcpb.RawGetRequest.newBuilder()
                .setKey(ByteString.copyFromUtf8("k")).build());
        assertThat(getResp2.getNotFound()).isTrue();
    }

    @Test
    void scanReturnsKeysInOrder() throws Exception {
        start();

        for (int i = 0; i < 20; i++) {
            tikv.rawPut(Kvrpcpb.RawPutRequest.newBuilder()
                    .setKey(ByteString.copyFromUtf8(String.format("k%03d", i)))
                    .setValue(ByteString.copyFromUtf8("v" + i))
                    .build());
        }

        var scanResp = tikv.rawScan(Kvrpcpb.RawScanRequest.newBuilder()
                .setStartKey(ByteString.copyFromUtf8("k005"))
                .setEndKey(ByteString.copyFromUtf8("k010"))
                .setLimit(100)
                .build());
        assertThat(scanResp.getKvsCount()).isEqualTo(5);
        for (int i = 0; i < 5; i++) {
            assertThat(scanResp.getKvs(i).getKey().toStringUtf8())
                    .isEqualTo(String.format("k00%d", 5 + i));
        }
    }

    @Test
    void batchGetAndBatchPut() throws Exception {
        start();

        var batchPut = Kvrpcpb.RawBatchPutRequest.newBuilder();
        for (int i = 0; i < 5; i++) {
            batchPut.addPairs(Kvrpcpb.KvPair.newBuilder()
                    .setKey(ByteString.copyFromUtf8("bk" + i))
                    .setValue(ByteString.copyFromUtf8("bv" + i))
                    .build());
        }
        var bpr = tikv.rawBatchPut(batchPut.build());
        assertThat(bpr.getError()).isEmpty();

        var bgr = tikv.rawBatchGet(Kvrpcpb.RawBatchGetRequest.newBuilder()
                .addKeys(ByteString.copyFromUtf8("bk0"))
                .addKeys(ByteString.copyFromUtf8("bk2"))
                .addKeys(ByteString.copyFromUtf8("bk4"))
                .addKeys(ByteString.copyFromUtf8("missing"))
                .build());
        assertThat(bgr.getPairsCount()).isEqualTo(3);
    }

    @Test
    void deleteRangeRemovesContiguousKeys() throws Exception {
        start();

        for (int i = 0; i < 10; i++) {
            tikv.rawPut(Kvrpcpb.RawPutRequest.newBuilder()
                    .setKey(ByteString.copyFromUtf8(String.format("d%03d", i)))
                    .setValue(ByteString.copyFromUtf8("x"))
                    .build());
        }

        var resp = tikv.rawDeleteRange(Kvrpcpb.RawDeleteRangeRequest.newBuilder()
                .setStartKey(ByteString.copyFromUtf8("d003"))
                .setEndKey(ByteString.copyFromUtf8("d007"))
                .build());
        assertThat(resp.getError()).isEmpty();

        for (int i = 0; i < 10; i++) {
            var got = tikv.rawGet(Kvrpcpb.RawGetRequest.newBuilder()
                    .setKey(ByteString.copyFromUtf8(String.format("d%03d", i)))
                    .build());
            if (i >= 3 && i < 7) {
                assertThat(got.getNotFound()).as("d%03d should be gone".formatted(i)).isTrue();
            } else {
                assertThat(got.getNotFound()).as("d%03d should still exist".formatted(i)).isFalse();
            }
        }
    }

    @Test
    void rawCasMatchesAndSwaps() throws Exception {
        start();
        // Initialise key.
        tikv.rawPut(Kvrpcpb.RawPutRequest.newBuilder()
                .setKey(ByteString.copyFromUtf8("cas-k"))
                .setValue(ByteString.copyFromUtf8("v0"))
                .build());

        // Successful CAS: previous_value matches.
        var ok = tikv.rawCAS(Kvrpcpb.RawCASRequest.newBuilder()
                .setKey(ByteString.copyFromUtf8("cas-k"))
                .setPreviousValue(ByteString.copyFromUtf8("v0"))
                .setValue(ByteString.copyFromUtf8("v1"))
                .build());
        assertThat(ok.getError()).isEmpty();
        assertThat(ok.getSucceed()).isTrue();
        assertThat(ok.getPreviousValue().toStringUtf8()).isEqualTo("v0");

        // Verify swap landed.
        var got = tikv.rawGet(Kvrpcpb.RawGetRequest.newBuilder()
                .setKey(ByteString.copyFromUtf8("cas-k")).build());
        assertThat(got.getValue().toStringUtf8()).isEqualTo("v1");

        // Failed CAS: previous_value mismatch — value must NOT change.
        var fail = tikv.rawCAS(Kvrpcpb.RawCASRequest.newBuilder()
                .setKey(ByteString.copyFromUtf8("cas-k"))
                .setPreviousValue(ByteString.copyFromUtf8("v0"))   // stale expectation
                .setValue(ByteString.copyFromUtf8("v2"))
                .build());
        assertThat(fail.getSucceed()).isFalse();
        assertThat(fail.getPreviousValue().toStringUtf8()).isEqualTo("v1");
        var stillV1 = tikv.rawGet(Kvrpcpb.RawGetRequest.newBuilder()
                .setKey(ByteString.copyFromUtf8("cas-k")).build());
        assertThat(stillV1.getValue().toStringUtf8()).isEqualTo("v1");
    }

    @Test
    void rawCasInsertWhenAbsent() throws Exception {
        start();
        // No prior put: CAS with previous_not_exist=true must succeed.
        var ok = tikv.rawCAS(Kvrpcpb.RawCASRequest.newBuilder()
                .setKey(ByteString.copyFromUtf8("cas-new"))
                .setPreviousNotExist(true)
                .setValue(ByteString.copyFromUtf8("first"))
                .build());
        assertThat(ok.getSucceed()).isTrue();
        assertThat(ok.getPreviousNotExist()).isTrue();

        // Second time, previous_not_exist=true must FAIL because the key now exists.
        var fail = tikv.rawCAS(Kvrpcpb.RawCASRequest.newBuilder()
                .setKey(ByteString.copyFromUtf8("cas-new"))
                .setPreviousNotExist(true)
                .setValue(ByteString.copyFromUtf8("second"))
                .build());
        assertThat(fail.getSucceed()).isFalse();
        assertThat(fail.getPreviousValue().toStringUtf8()).isEqualTo("first");
    }

    @Test
    void crashRecoveryE2E() throws Exception {
        start();
        for (int i = 0; i < 10; i++) {
            tikv.rawPut(Kvrpcpb.RawPutRequest.newBuilder()
                    .setKey(ByteString.copyFromUtf8("ck" + i))
                    .setValue(ByteString.copyFromUtf8("cv" + i))
                    .build());
        }

        // Hard restart: tear it all down and bring it back up against the same
        // data directory.
        teardown();
        start();

        for (int i = 0; i < 10; i++) {
            var got = tikv.rawGet(Kvrpcpb.RawGetRequest.newBuilder()
                    .setKey(ByteString.copyFromUtf8("ck" + i)).build());
            assertThat(got.getNotFound()).as("ck%d should survive restart".formatted(i)).isFalse();
            assertThat(got.getValue().toStringUtf8()).isEqualTo("cv" + i);
        }

        // New writes work too.
        tikv.rawPut(Kvrpcpb.RawPutRequest.newBuilder()
                .setKey(ByteString.copyFromUtf8("after"))
                .setValue(ByteString.copyFromUtf8("ok")).build());
        var got = tikv.rawGet(Kvrpcpb.RawGetRequest.newBuilder()
                .setKey(ByteString.copyFromUtf8("after")).build());
        assertThat(got.getValue().toStringUtf8()).isEqualTo("ok");
    }

    // ---- helpers ----

    private static RegionPeerImpl startPeer(RocksStorageEngine engine) {
        var raftEngine = new PerRegionRaftEngine(engine, 1);
        var region = Metapb.Region.newBuilder()
                .setId(1)
                .setRegionEpoch(Metapb.RegionEpoch.newBuilder().setConfVer(1).setVersion(1))
                .addPeers(Metapb.Peer.newBuilder().setId(1).setStoreId(1).setRole(Metapb.PeerRole.Voter))
                .build();
        return new RegionPeerImpl(
                engine, raftEngine, region, region.getPeers(0),
                List.of(new Peer(1)),
                new LoopbackTransport(),
                new RawKvApplyHandler(engine),
                new RegionPeerImpl.Settings(10, 1, 30));
    }
}
