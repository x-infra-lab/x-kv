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
import io.github.xinfra.lab.xkv.kv.raft.RegionPeerImpl;
import io.github.xinfra.lab.xkv.kv.server.ChangeDataServiceImpl;
import io.github.xinfra.lab.xkv.kv.server.RawKvService;
import io.github.xinfra.lab.xkv.kv.server.TikvServiceImpl;
import io.github.xinfra.lab.xkv.kv.server.TransactionService;
import io.github.xinfra.lab.xkv.proto.Cdcpb;
import io.github.xinfra.lab.xkv.proto.ChangeDataGrpc;
import io.github.xinfra.lab.xkv.proto.Kvrpcpb;
import io.github.xinfra.lab.xkv.proto.Metapb;
import io.github.xinfra.lab.xkv.proto.TikvGrpc;
import io.grpc.ManagedChannel;
import io.grpc.Server;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.stub.StreamObserver;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static org.assertj.core.api.Assertions.assertThat;

final class CdcKeyFilterTest {

    @TempDir Path dataDir;

    private RocksStorageEngine engine;
    private RegionPeerImpl peer;
    private Server grpcServer;
    private ManagedChannel channel;
    private TikvGrpc.TikvBlockingStub tikv;
    private ChangeDataGrpc.ChangeDataStub cdcStub;
    private ChangeDataServiceImpl cdcService;

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

        var cdcEventBus = new CdcEventBus();
        var cm = new ConcurrencyManager(new MaxTsTracker(raftEngine.persistedMaxTs()));

        peer = new RegionPeerImpl(
                engine, raftEngine, region, region.getPeers(0),
                List.of(new Peer(1)),
                new LoopbackTransport(),
                CompositeApplyHandler.defaultFor(engine, cm, 1, cdcEventBus),
                new RegionPeerImpl.Settings(10, 1, 30),
                cm);
        Awaitility.await().atMost(Duration.ofSeconds(10)).until(peer::isLeader);

        var rawKv = new RawKvService(engine, key -> peer, 5_000);
        var txn = new TransactionService(engine, key -> peer, 5_000, cm);
        cdcService = new ChangeDataServiceImpl(cdcEventBus, () -> cm.maxTs().current());

        var name = "cdc-filter-" + UUID.randomUUID();
        grpcServer = InProcessServerBuilder.forName(name)
                .addService(new TikvServiceImpl(rawKv, txn))
                .addService(cdcService)
                .build()
                .start();
        channel = InProcessChannelBuilder.forName(name).build();
        tikv = TikvGrpc.newBlockingStub(channel);
        cdcStub = ChangeDataGrpc.newStub(channel);
    }

    @AfterEach
    void teardown() {
        if (cdcService != null) cdcService.close();
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
    void keyRangeFilterDeliverOnlyMatchingKeys() throws Exception {
        var received = new CopyOnWriteArrayList<Cdcpb.ChangeDataEvent>();
        var latch = new CountDownLatch(2);

        var reqObserver = cdcStub.eventFeed(new StreamObserver<>() {
            @Override public void onNext(Cdcpb.ChangeDataEvent event) {
                received.add(event);
                for (var e : event.getEventsList()) {
                    latch.countDown();
                }
            }
            @Override public void onError(Throwable t) {}
            @Override public void onCompleted() {}
        });

        // Register with key range [b, d).
        reqObserver.onNext(Cdcpb.ChangeDataRequest.newBuilder()
                .setRegionId(1)
                .setStartKey(ByteString.copyFromUtf8("b"))
                .setEndKey(ByteString.copyFromUtf8("d"))
                .setRegister(Cdcpb.Register.newBuilder())
                .build());
        Thread.sleep(100);

        // Write keys a, b, c, d, e — only b and c should be delivered.
        for (String key : new String[]{"a", "b", "c", "d", "e"}) {
            long startTs = nextTs();
            long commitTs = nextTs();
            prewrite(key, "val-" + key, startTs);
            commit(key, startTs, commitTs);
        }

        assertThat(latch.await(5, TimeUnit.SECONDS))
                .as("should receive exactly 2 events for keys b and c")
                .isTrue();

        // Wait a bit more to make sure no extra events arrive.
        Thread.sleep(300);

        var allKeys = received.stream()
                .flatMap(e -> e.getEventsList().stream())
                .flatMap(e -> e.getEntries().getEntriesList().stream())
                .map(r -> r.getKey().toStringUtf8())
                .sorted()
                .toList();
        assertThat(allKeys).containsExactly("b", "c");

        reqObserver.onCompleted();
    }

    @Test
    void noFilterDeliversAllKeys() throws Exception {
        var received = new CopyOnWriteArrayList<Cdcpb.ChangeDataEvent>();
        var latch = new CountDownLatch(3);

        var reqObserver = cdcStub.eventFeed(new StreamObserver<>() {
            @Override public void onNext(Cdcpb.ChangeDataEvent event) {
                received.add(event);
                for (var e : event.getEventsList()) {
                    latch.countDown();
                }
            }
            @Override public void onError(Throwable t) {}
            @Override public void onCompleted() {}
        });

        // Register without start_key/end_key — no filter.
        reqObserver.onNext(Cdcpb.ChangeDataRequest.newBuilder()
                .setRegionId(1)
                .setRegister(Cdcpb.Register.newBuilder())
                .build());
        Thread.sleep(100);

        for (String key : new String[]{"x", "y", "z"}) {
            long startTs = nextTs();
            long commitTs = nextTs();
            prewrite(key, "val-" + key, startTs);
            commit(key, startTs, commitTs);
        }

        assertThat(latch.await(5, TimeUnit.SECONDS)).isTrue();

        var allKeys = received.stream()
                .flatMap(e -> e.getEventsList().stream())
                .flatMap(e -> e.getEntries().getEntriesList().stream())
                .map(r -> r.getKey().toStringUtf8())
                .sorted()
                .toList();
        assertThat(allKeys).containsExactly("x", "y", "z");

        reqObserver.onCompleted();
    }

    @Test
    void openEndedStartKeyFilter() throws Exception {
        var received = new CopyOnWriteArrayList<Cdcpb.ChangeDataEvent>();
        var latch = new CountDownLatch(2);

        var reqObserver = cdcStub.eventFeed(new StreamObserver<>() {
            @Override public void onNext(Cdcpb.ChangeDataEvent event) {
                received.add(event);
                for (var e : event.getEventsList()) {
                    latch.countDown();
                }
            }
            @Override public void onError(Throwable t) {}
            @Override public void onCompleted() {}
        });

        // start_key="c", no end_key — [c, +inf).
        reqObserver.onNext(Cdcpb.ChangeDataRequest.newBuilder()
                .setRegionId(1)
                .setStartKey(ByteString.copyFromUtf8("c"))
                .setRegister(Cdcpb.Register.newBuilder())
                .build());
        Thread.sleep(100);

        for (String key : new String[]{"a", "b", "c", "d"}) {
            long startTs = nextTs();
            long commitTs = nextTs();
            prewrite(key, "v", startTs);
            commit(key, startTs, commitTs);
        }

        assertThat(latch.await(5, TimeUnit.SECONDS)).isTrue();
        Thread.sleep(300);

        var allKeys = received.stream()
                .flatMap(e -> e.getEventsList().stream())
                .flatMap(e -> e.getEntries().getEntriesList().stream())
                .map(r -> r.getKey().toStringUtf8())
                .sorted()
                .toList();
        assertThat(allKeys).containsExactly("c", "d");

        reqObserver.onCompleted();
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
