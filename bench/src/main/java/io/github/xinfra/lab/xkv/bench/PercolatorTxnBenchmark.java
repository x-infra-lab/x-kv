package io.github.xinfra.lab.xkv.bench;

import com.google.protobuf.ByteString;
import io.github.xinfra.lab.raft.Peer;
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
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
@Warmup(iterations = 3, time = 3)
@Measurement(iterations = 5, time = 5)
@Fork(1)
@State(Scope.Benchmark)
public class PercolatorTxnBenchmark {

    private static final int SEED_KEYS = 1000;
    private static final byte[] VALUE_64B = new byte[64];
    static { java.util.Arrays.fill(VALUE_64B, (byte) 'x'); }

    private RocksStorageEngine engine;
    private BatchRegionPeer peer;
    private Server grpcServer;
    private ManagedChannel channel;
    private TikvGrpc.TikvBlockingStub tikv;
    private Path dataDir;

    private final AtomicLong keyCounter = new AtomicLong();
    private final AtomicLong tso = new AtomicLong(System.currentTimeMillis() << 18);

    private long nextTs() {
        long now = System.currentTimeMillis();
        return tso.updateAndGet(prev -> {
            long prevPhy = prev >>> 18;
            if (now > prevPhy) return now << 18;
            return prev + 1;
        });
    }

    @Setup(Level.Trial)
    public void setup() throws Exception {
        dataDir = Files.createTempDirectory("xkv-txn-bench-");
        engine = RocksStorageEngine.open(dataDir, KvConfig.EngineConfig.defaults());
        var raftEngine = new PerRegionRaftEngine(engine, 1);
        var region = Metapb.Region.newBuilder()
                .setId(1)
                .setRegionEpoch(Metapb.RegionEpoch.newBuilder().setConfVer(1).setVersion(1))
                .addPeers(Metapb.Peer.newBuilder().setId(1).setStoreId(1).setRole(Metapb.PeerRole.Voter))
                .build();
        var cm = new ConcurrencyManager(new MaxTsTracker(raftEngine.persistedMaxTs()));
        peer = BatchRegionPeer.standalone(
                engine, raftEngine, region, region.getPeers(0),
                List.of(new Peer(1)),
                new LoopbackTransport(),
                CompositeApplyHandler.defaultFor(engine, cm),
                new RegionPeer.Settings(10, 1, 30),
                cm);
        Awaitility.await().atMost(Duration.ofSeconds(10)).until(peer::isLeader);

        var rawKv = new RawKvService(engine, key -> peer, 5_000);
        var txn = new TransactionService(engine, key -> peer, 5_000, cm);
        var name = "txn-bench-" + UUID.randomUUID();
        grpcServer = InProcessServerBuilder.forName(name)
                .addService(new TikvServiceImpl(rawKv, txn))
                .build()
                .start();
        channel = InProcessChannelBuilder.forName(name).build();
        tikv = TikvGrpc.newBlockingStub(channel);

        for (int i = 0; i < SEED_KEYS; i++) {
            long startTs = nextTs();
            long commitTs = nextTs();
            ByteString key = seedKey(i);
            tikv.kvPrewrite(Kvrpcpb.PrewriteRequest.newBuilder()
                    .setStartVersion(startTs)
                    .setLockTtl(5000)
                    .setPrimaryLock(key)
                    .addMutations(Kvrpcpb.Mutation.newBuilder()
                            .setOp(Kvrpcpb.Op.Put)
                            .setKey(key)
                            .setValue(ByteString.copyFrom(VALUE_64B)))
                    .build());
            tikv.kvCommit(Kvrpcpb.CommitRequest.newBuilder()
                    .setStartVersion(startTs)
                    .setCommitVersion(commitTs)
                    .addKeys(key)
                    .build());
        }
    }

    @TearDown(Level.Trial)
    public void teardown() {
        if (channel != null) channel.shutdownNow();
        if (grpcServer != null) {
            grpcServer.shutdownNow();
            try { grpcServer.awaitTermination(2, TimeUnit.SECONDS); }
            catch (InterruptedException ignored) { Thread.currentThread().interrupt(); }
        }
        if (peer != null) peer.shutdown();
        if (engine != null) engine.close();
    }

    @Benchmark
    @Threads(1)
    public void benchTxnPutGet() {
        long k = keyCounter.incrementAndGet();
        ByteString key = freshKey(k);
        long startTs = nextTs();
        long commitTs = nextTs();

        tikv.kvPrewrite(Kvrpcpb.PrewriteRequest.newBuilder()
                .setStartVersion(startTs)
                .setLockTtl(5000)
                .setPrimaryLock(key)
                .addMutations(Kvrpcpb.Mutation.newBuilder()
                        .setOp(Kvrpcpb.Op.Put)
                        .setKey(key)
                        .setValue(ByteString.copyFrom(VALUE_64B)))
                .build());
        tikv.kvCommit(Kvrpcpb.CommitRequest.newBuilder()
                .setStartVersion(startTs)
                .setCommitVersion(commitTs)
                .addKeys(key)
                .build());

        long readTs = nextTs();
        tikv.kvGet(Kvrpcpb.GetRequest.newBuilder()
                .setKey(key)
                .setVersion(readTs)
                .build());
    }

    @Benchmark
    @Threads(1)
    public void benchTxnTransfer() {
        long idx = keyCounter.incrementAndGet();
        int from = (int) (idx % SEED_KEYS);
        int to = (int) ((idx + 1) % SEED_KEYS);
        ByteString fromKey = seedKey(from);
        ByteString toKey = seedKey(to);
        ByteString primary = fromKey.toStringUtf8().compareTo(toKey.toStringUtf8()) < 0
                ? fromKey : toKey;

        long readTs = nextTs();
        tikv.kvGet(Kvrpcpb.GetRequest.newBuilder().setKey(fromKey).setVersion(readTs).build());
        tikv.kvGet(Kvrpcpb.GetRequest.newBuilder().setKey(toKey).setVersion(readTs).build());

        long startTs = nextTs();
        long commitTs = nextTs();
        var prewrite = tikv.kvPrewrite(Kvrpcpb.PrewriteRequest.newBuilder()
                .setStartVersion(startTs)
                .setLockTtl(5000)
                .setPrimaryLock(primary)
                .addMutations(Kvrpcpb.Mutation.newBuilder()
                        .setOp(Kvrpcpb.Op.Put)
                        .setKey(fromKey)
                        .setValue(ByteString.copyFrom(VALUE_64B)))
                .addMutations(Kvrpcpb.Mutation.newBuilder()
                        .setOp(Kvrpcpb.Op.Put)
                        .setKey(toKey)
                        .setValue(ByteString.copyFrom(VALUE_64B)))
                .build());
        if (prewrite.getErrorsCount() == 0) {
            tikv.kvCommit(Kvrpcpb.CommitRequest.newBuilder()
                    .setStartVersion(startTs)
                    .setCommitVersion(commitTs)
                    .addKeys(fromKey)
                    .addKeys(toKey)
                    .build());
        }
    }

    @Benchmark
    @Threads(1)
    public void benchTxnScan() {
        long readTs = nextTs();
        tikv.kvScan(Kvrpcpb.ScanRequest.newBuilder()
                .setStartKey(seedKey(0))
                .setEndKey(seedKey(10))
                .setVersion(readTs)
                .setLimit(10)
                .build());
    }

    @Benchmark
    @Threads(4)
    public void benchTxnPutGetConcurrent() {
        long k = keyCounter.incrementAndGet();
        ByteString key = freshKey(k);
        long startTs = nextTs();
        long commitTs = nextTs();

        var prewrite = tikv.kvPrewrite(Kvrpcpb.PrewriteRequest.newBuilder()
                .setStartVersion(startTs)
                .setLockTtl(5000)
                .setPrimaryLock(key)
                .addMutations(Kvrpcpb.Mutation.newBuilder()
                        .setOp(Kvrpcpb.Op.Put)
                        .setKey(key)
                        .setValue(ByteString.copyFrom(VALUE_64B)))
                .build());
        if (prewrite.getErrorsCount() == 0) {
            tikv.kvCommit(Kvrpcpb.CommitRequest.newBuilder()
                    .setStartVersion(startTs)
                    .setCommitVersion(commitTs)
                    .addKeys(key)
                    .build());
        }

        long readTs = nextTs();
        tikv.kvGet(Kvrpcpb.GetRequest.newBuilder()
                .setKey(key)
                .setVersion(readTs)
                .build());
    }

    private static ByteString seedKey(int i) {
        return ByteString.copyFrom(String.format("seed-%04d", i), StandardCharsets.UTF_8);
    }

    private static ByteString freshKey(long k) {
        return ByteString.copyFrom(("txn-" + k).getBytes(StandardCharsets.UTF_8));
    }
}
