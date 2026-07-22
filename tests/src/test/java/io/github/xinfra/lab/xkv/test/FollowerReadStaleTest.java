package io.github.xinfra.lab.xkv.test;

import com.google.protobuf.ByteString;
import io.github.xinfra.lab.raft.Peer;
import io.github.xinfra.lab.xkv.kv.cdc.CdcEventBus;
import io.github.xinfra.lab.xkv.kv.config.KvConfig;
import io.github.xinfra.lab.xkv.kv.engine.PerRegionRaftEngine;
import io.github.xinfra.lab.xkv.kv.engine.RocksStorageEngine;
import io.github.xinfra.lab.xkv.kv.mvcc.ConcurrencyManager;
import io.github.xinfra.lab.xkv.kv.mvcc.MaxTsTracker;
import io.github.xinfra.lab.xkv.kv.raft.CompositeApplyHandler;
import io.github.xinfra.lab.xkv.kv.raft.LoopbackTransport;
import io.github.xinfra.lab.xkv.kv.raft.BatchRegionPeer;
import io.github.xinfra.lab.xkv.kv.raft.RegionPeer;
import io.github.xinfra.lab.xkv.kv.server.RawKvService;
import io.github.xinfra.lab.xkv.kv.server.TikvServiceImpl;
import io.github.xinfra.lab.xkv.kv.server.TransactionService;
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

import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static org.assertj.core.api.Assertions.assertThat;

final class FollowerReadStaleTest {

    @TempDir Path dataDir;

    private RocksStorageEngine engine;
    private BatchRegionPeer peer;
    private Server grpcServer;
    private ManagedChannel channel;
    private TikvGrpc.TikvBlockingStub tikv;
    private ConcurrencyManager cm;

    private final AtomicLong tso = new AtomicLong(System.currentTimeMillis() << 18);

    private long nextTs() {
        long now = System.currentTimeMillis();
        return tso.updateAndGet(prev -> {
            long prevPhy = prev >>> 18;
            if (now > prevPhy) return now << 18;
            return prev + 1;
        });
    }

    @BeforeEach
    void start() throws Exception {
        engine = RocksStorageEngine.open(dataDir, KvConfig.EngineConfig.defaults());
        var raftEngine = new PerRegionRaftEngine(engine, 1);
        var region = Metapb.Region.newBuilder()
                .setId(1)
                .setRegionEpoch(Metapb.RegionEpoch.newBuilder().setConfVer(1).setVersion(1))
                .addPeers(Metapb.Peer.newBuilder().setId(1).setStoreId(1)
                        .setRole(Metapb.PeerRole.Voter))
                .build();

        cm = new ConcurrencyManager(new MaxTsTracker(raftEngine.persistedMaxTs()));
        var cdcEventBus = new CdcEventBus();

        peer = BatchRegionPeer.standalone(
                engine, raftEngine, region, region.getPeers(0),
                List.of(new Peer(1)),
                new LoopbackTransport(),
                CompositeApplyHandler.defaultFor(engine, cm, 1, cdcEventBus),
                new RegionPeer.Settings(10, 1, 30),
                cm);
        Awaitility.await().atMost(Duration.ofSeconds(10)).until(peer::isLeader);

        var rawKv = new RawKvService(engine, key -> peer, 5_000);
        var txn = new TransactionService(engine, key -> peer, 5_000, cm);

        var name = "stale-read-" + UUID.randomUUID();
        grpcServer = InProcessServerBuilder.forName(name)
                .addService(new TikvServiceImpl(rawKv, txn))
                .build()
                .start();
        channel = InProcessChannelBuilder.forName(name).build();
        tikv = TikvGrpc.newBlockingStub(channel);
    }

    @AfterEach
    void teardown() {
        if (channel != null) channel.shutdownNow();
        if (grpcServer != null) {
            grpcServer.shutdownNow();
            try { grpcServer.awaitTermination(2, TimeUnit.SECONDS); }
            catch (InterruptedException e) { Thread.currentThread().interrupt(); }
        }
        if (peer != null) peer.shutdown();
        if (engine != null) engine.close();
    }

    @Test
    void dataIsNotReadyWhenSafeTsExceedsMaxTs() {
        // Write via MVCC so maxTs gets bumped.
        long startTs = nextTs();
        long commitTs = nextTs();
        prewrite("stale-k1", "v1", startTs);
        commit("stale-k1", startTs, commitTs);

        // Now peer.maxTs() >= commitTs.
        assertThat(peer.maxTs()).isGreaterThanOrEqualTo(commitTs);

        // Follower read with safe_ts = far-future → DataIsNotReady.
        long farFuture = (System.currentTimeMillis() + 3_600_000L) << 18;
        var staleResp = tikv.rawGet(Kvrpcpb.RawGetRequest.newBuilder()
                .setKey(ByteString.copyFromUtf8("stale-k1"))
                .setContext(Kvrpcpb.Context.newBuilder()
                        .setReplicaRead(Kvrpcpb.ReplicaReadType.FOLLOWER)
                        .setStaleReadSafeTs(farFuture))
                .build());
        assertThat(staleResp.hasRegionError()).isTrue();
        assertThat(staleResp.getRegionError().hasDataIsNotReady()).isTrue();
        assertThat(staleResp.getRegionError().getDataIsNotReady().getSafeTs())
                .isEqualTo(farFuture);
    }

    @Test
    void succeedsWhenSafeTsBelowMaxTs() {
        long startTs = nextTs();
        long commitTs = nextTs();
        prewrite("stale-k2", "v2", startTs);
        commit("stale-k2", startTs, commitTs);

        // safe_ts = 1 (ancient) → should succeed.
        var resp = tikv.rawGet(Kvrpcpb.RawGetRequest.newBuilder()
                .setKey(ByteString.copyFromUtf8("stale-k2"))
                .setContext(Kvrpcpb.Context.newBuilder()
                        .setReplicaRead(Kvrpcpb.ReplicaReadType.FOLLOWER)
                        .setStaleReadSafeTs(1))
                .build());
        assertThat(resp.hasRegionError()).isFalse();
    }

    @Test
    void noCheckWhenSafeTsIsZero() {
        // No MVCC writes, peer maxTs = 0.
        // safe_ts = 0 means no stale-read check — should succeed.
        var resp = tikv.rawGet(Kvrpcpb.RawGetRequest.newBuilder()
                .setKey(ByteString.copyFromUtf8("no-key"))
                .setContext(Kvrpcpb.Context.newBuilder()
                        .setReplicaRead(Kvrpcpb.ReplicaReadType.FOLLOWER)
                        .setStaleReadSafeTs(0))
                .build());
        assertThat(resp.hasRegionError()).isFalse();
        assertThat(resp.getNotFound()).isTrue();
    }

    // ---- helpers ----

    private void prewrite(String key, String value, long startTs) {
        var resp = tikv.kvPrewrite(Kvrpcpb.PrewriteRequest.newBuilder()
                .setStartVersion(startTs)
                .setPrimaryLock(ByteString.copyFromUtf8(key))
                .setLockTtl(5_000)
                .addMutations(Kvrpcpb.Mutation.newBuilder()
                        .setOp(Kvrpcpb.Op.Put)
                        .setKey(ByteString.copyFromUtf8(key))
                        .setValue(ByteString.copyFromUtf8(value)))
                .build());
        assertThat(resp.getErrorsCount()).isZero();
    }

    private void commit(String key, long startTs, long commitTs) {
        var resp = tikv.kvCommit(Kvrpcpb.CommitRequest.newBuilder()
                .setStartVersion(startTs)
                .setCommitVersion(commitTs)
                .addKeys(ByteString.copyFromUtf8(key))
                .build());
        assertThat(resp.getError().getSerializedSize()).isZero();
    }
}
