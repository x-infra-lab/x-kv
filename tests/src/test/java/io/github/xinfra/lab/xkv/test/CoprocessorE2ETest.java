package io.github.xinfra.lab.xkv.test;

import com.google.protobuf.ByteString;
import io.github.xinfra.lab.raft.Peer;
import io.github.xinfra.lab.xkv.kv.coprocessor.TableScanCoprocessor;
import io.github.xinfra.lab.xkv.kv.config.KvConfig;
import io.github.xinfra.lab.xkv.kv.engine.PerRegionRaftEngine;
import io.github.xinfra.lab.xkv.kv.engine.RocksStorageEngine;
import io.github.xinfra.lab.xkv.kv.mvcc.ConcurrencyManager;
import io.github.xinfra.lab.xkv.kv.mvcc.MaxTsTracker;
import io.github.xinfra.lab.xkv.kv.raft.CompositeApplyHandler;
import io.github.xinfra.lab.xkv.kv.raft.LoopbackTransport;
import io.github.xinfra.lab.xkv.kv.raft.RegionPeerImpl;
import io.github.xinfra.lab.xkv.kv.server.CoprocessorService;
import io.github.xinfra.lab.xkv.kv.server.RawKvService;
import io.github.xinfra.lab.xkv.kv.server.TikvServiceImpl;
import io.github.xinfra.lab.xkv.kv.server.TransactionService;
import io.github.xinfra.lab.xkv.proto.Coprocessor;
import io.github.xinfra.lab.xkv.proto.Kvrpcpb;
import io.github.xinfra.lab.xkv.proto.Metapb;
import io.github.xinfra.lab.xkv.proto.TikvGrpc;
import io.grpc.ManagedChannel;
import io.grpc.Server;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * E2E test: coprocessor table-scan over MVCC data.
 *
 * <p>Writes data through the txn path (Prewrite + Commit), then uses the
 * coprocessor RPC to scan it back. Verifies MVCC snapshot isolation and
 * paging.
 */
final class CoprocessorE2ETest {

    @TempDir Path dataDir;
    private RocksStorageEngine engine;
    private RegionPeerImpl peer;
    private Server grpcServer;
    private ManagedChannel channel;
    private TikvGrpc.TikvBlockingStub tikv;

    @BeforeEach
    void start() throws Exception {
        engine = RocksStorageEngine.open(dataDir, KvConfig.EngineConfig.defaults());
        var raftEngine = new PerRegionRaftEngine(engine, 1);
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
        Awaitility.await().atMost(Duration.ofSeconds(10)).until(peer::isLeader);

        var rawKv = new RawKvService(engine, key -> peer, 5_000);
        var txn = new TransactionService(engine, key -> peer, 5_000, cm);

        var copService = new CoprocessorService();
        copService.register(new TableScanCoprocessor(engine));

        var name = "cop-test-" + UUID.randomUUID();
        grpcServer = InProcessServerBuilder.forName(name)
                .directExecutor()
                .addService(new TikvServiceImpl(rawKv, txn, copService, null, null))
                .build()
                .start();
        channel = InProcessChannelBuilder.forName(name).directExecutor().build();
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

    @Test
    void coprocessorScanReturnsMvccData() throws Exception {
        long startTs = 10;
        long commitTs = 20;

        // Write 5 keys via 2PC.
        var mutations = new ArrayList<Kvrpcpb.Mutation>();
        for (int i = 0; i < 5; i++) {
            mutations.add(Kvrpcpb.Mutation.newBuilder()
                    .setOp(Kvrpcpb.Op.Put)
                    .setKey(ByteString.copyFromUtf8(String.format("ck%03d", i)))
                    .setValue(ByteString.copyFromUtf8("cv" + i))
                    .build());
        }

        var prewriteResp = tikv.kvPrewrite(Kvrpcpb.PrewriteRequest.newBuilder()
                .setStartVersion(startTs)
                .setPrimaryLock(ByteString.copyFromUtf8("ck000"))
                .addAllMutations(mutations)
                .setLockTtl(5000)
                .build());
        assertThat(prewriteResp.getErrorsCount()).isZero();

        var commitResp = tikv.kvCommit(Kvrpcpb.CommitRequest.newBuilder()
                .setStartVersion(startTs)
                .setCommitVersion(commitTs)
                .addAllKeys(mutations.stream()
                        .map(Kvrpcpb.Mutation::getKey).toList())
                .build());
        assertThat(commitResp.hasError()).isFalse();

        // Coprocessor scan at commitTs should see all 5 keys.
        var copResp = tikv.coprocessor(Coprocessor.Request.newBuilder()
                .setTp(0)
                .setStartTs(commitTs)
                .addRanges(Coprocessor.KeyRange.newBuilder()
                        .setStart(ByteString.copyFromUtf8("ck000"))
                        .setEnd(ByteString.copyFromUtf8("ck999")))
                .build());
        assertThat(copResp.getOtherError()).isEmpty();
        assertThat(copResp.hasLocked()).isFalse();

        var pairs = decodeKvPairs(copResp.getData().toByteArray());
        assertThat(pairs).hasSize(5);
        for (int i = 0; i < 5; i++) {
            assertThat(new String(pairs.get(i).key)).isEqualTo(String.format("ck%03d", i));
            assertThat(new String(pairs.get(i).value)).isEqualTo("cv" + i);
        }
    }

    @Test
    void coprocessorPagingLimitsResults() throws Exception {
        long startTs = 100;
        long commitTs = 200;

        var mutations = new ArrayList<Kvrpcpb.Mutation>();
        for (int i = 0; i < 10; i++) {
            mutations.add(Kvrpcpb.Mutation.newBuilder()
                    .setOp(Kvrpcpb.Op.Put)
                    .setKey(ByteString.copyFromUtf8(String.format("pk%03d", i)))
                    .setValue(ByteString.copyFromUtf8("pv" + i))
                    .build());
        }

        tikv.kvPrewrite(Kvrpcpb.PrewriteRequest.newBuilder()
                .setStartVersion(startTs)
                .setPrimaryLock(ByteString.copyFromUtf8("pk000"))
                .addAllMutations(mutations)
                .setLockTtl(5000).build());
        tikv.kvCommit(Kvrpcpb.CommitRequest.newBuilder()
                .setStartVersion(startTs).setCommitVersion(commitTs)
                .addAllKeys(mutations.stream().map(Kvrpcpb.Mutation::getKey).toList())
                .build());

        // Request with paging_size = 3: should return at most 3 rows.
        var copResp = tikv.coprocessor(Coprocessor.Request.newBuilder()
                .setTp(0).setStartTs(commitTs).setPagingSize(3)
                .addRanges(Coprocessor.KeyRange.newBuilder()
                        .setStart(ByteString.copyFromUtf8("pk000"))
                        .setEnd(ByteString.copyFromUtf8("pk999")))
                .build());
        assertThat(copResp.getOtherError()).isEmpty();

        var pairs = decodeKvPairs(copResp.getData().toByteArray());
        assertThat(pairs).hasSize(3);
        assertThat(new String(pairs.get(0).key)).isEqualTo("pk000");
        assertThat(new String(pairs.get(2).key)).isEqualTo("pk002");
    }

    @Test
    void coprocessorScanAtOlderTimestampSeesNothing() throws Exception {
        long startTs = 300;
        long commitTs = 400;

        tikv.kvPrewrite(Kvrpcpb.PrewriteRequest.newBuilder()
                .setStartVersion(startTs)
                .setPrimaryLock(ByteString.copyFromUtf8("tk001"))
                .addMutations(Kvrpcpb.Mutation.newBuilder()
                        .setOp(Kvrpcpb.Op.Put)
                        .setKey(ByteString.copyFromUtf8("tk001"))
                        .setValue(ByteString.copyFromUtf8("tv1")))
                .setLockTtl(5000).build());
        tikv.kvCommit(Kvrpcpb.CommitRequest.newBuilder()
                .setStartVersion(startTs).setCommitVersion(commitTs)
                .addKeys(ByteString.copyFromUtf8("tk001")).build());

        // Scan at ts=200 (before commitTs=400) should see nothing.
        var copResp = tikv.coprocessor(Coprocessor.Request.newBuilder()
                .setTp(0).setStartTs(200)
                .addRanges(Coprocessor.KeyRange.newBuilder()
                        .setStart(ByteString.copyFromUtf8("tk000"))
                        .setEnd(ByteString.copyFromUtf8("tk999")))
                .build());
        assertThat(copResp.getOtherError()).isEmpty();
        var pairs = decodeKvPairs(copResp.getData().toByteArray());
        assertThat(pairs).isEmpty();
    }

    private record KvPair(byte[] key, byte[] value) {}

    private static List<KvPair> decodeKvPairs(byte[] data) {
        if (data == null || data.length < 4) return List.of();
        var bb = ByteBuffer.wrap(data);
        int count = bb.getInt();
        var out = new ArrayList<KvPair>(count);
        for (int i = 0; i < count; i++) {
            int kLen = bb.getInt();
            byte[] k = new byte[kLen];
            bb.get(k);
            int vLen = bb.getInt();
            byte[] v = new byte[vLen];
            bb.get(v);
            out.add(new KvPair(k, v));
        }
        return out;
    }
}
