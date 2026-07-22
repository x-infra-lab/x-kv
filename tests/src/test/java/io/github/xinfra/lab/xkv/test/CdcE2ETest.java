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

final class CdcE2ETest {

    @TempDir Path dataDir;

    private RocksStorageEngine engine;
    private BatchRegionPeer peer;
    private Server grpcServer;
    private ManagedChannel channel;
    private TikvGrpc.TikvBlockingStub tikv;
    private ChangeDataGrpc.ChangeDataStub cdcStub;
    private ChangeDataServiceImpl cdcService;
    private CdcEventBus cdcEventBus;
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
        cdcEventBus = new CdcEventBus();

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
        cdcService = new ChangeDataServiceImpl(cdcEventBus, () -> cm.maxTs().current());

        var name = "cdc-test-" + UUID.randomUUID();
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
    void commitEventDeliveredToCdcSubscriber() throws Exception {
        var received = new CopyOnWriteArrayList<Cdcpb.ChangeDataEvent>();
        var latch = new CountDownLatch(1);

        var reqObserver = cdcStub.eventFeed(new StreamObserver<>() {
            @Override public void onNext(Cdcpb.ChangeDataEvent event) {
                received.add(event);
                latch.countDown();
            }
            @Override public void onError(Throwable t) {}
            @Override public void onCompleted() {}
        });

        // Register for region 1.
        reqObserver.onNext(Cdcpb.ChangeDataRequest.newBuilder()
                .setRegionId(1)
                .setRegister(Cdcpb.Register.newBuilder())
                .build());

        // Give subscription time to register.
        Thread.sleep(100);

        // Prewrite + Commit a key.
        long startTs = nextTs();
        long commitTs = nextTs();
        prewrite("cdc-key", "cdc-value", startTs);
        commit("cdc-key", startTs, commitTs);

        // Wait for the CDC event.
        assertThat(latch.await(5, TimeUnit.SECONDS))
                .as("CDC event should be received within 5 seconds")
                .isTrue();

        assertThat(received).hasSize(1);
        var event = received.get(0);
        assertThat(event.getEventsCount()).isEqualTo(1);
        var inner = event.getEvents(0);
        assertThat(inner.getRegionId()).isEqualTo(1);
        var entries = inner.getEntries();
        assertThat(entries.getEntriesCount()).isEqualTo(1);
        var row = entries.getEntries(0);
        assertThat(row.getType()).isEqualTo(Cdcpb.Row.OpType.PUT);
        assertThat(row.getKey().toStringUtf8()).isEqualTo("cdc-key");
        assertThat(row.getValue().toStringUtf8()).isEqualTo("cdc-value");
        assertThat(row.getStartTs()).isEqualTo(startTs);
        assertThat(row.getCommitTs()).isEqualTo(commitTs);

        reqObserver.onCompleted();
    }

    @Test
    void deleteEventDelivered() throws Exception {
        // Pre-seed a key.
        long startTs1 = nextTs();
        long commitTs1 = nextTs();
        prewrite("del-key", "val", startTs1);
        commit("del-key", startTs1, commitTs1);

        var received = new CopyOnWriteArrayList<Cdcpb.ChangeDataEvent>();
        var latch = new CountDownLatch(1);

        var reqObserver = cdcStub.eventFeed(new StreamObserver<>() {
            @Override public void onNext(Cdcpb.ChangeDataEvent event) {
                received.add(event);
                latch.countDown();
            }
            @Override public void onError(Throwable t) {}
            @Override public void onCompleted() {}
        });

        reqObserver.onNext(Cdcpb.ChangeDataRequest.newBuilder()
                .setRegionId(1)
                .setRegister(Cdcpb.Register.newBuilder())
                .build());
        Thread.sleep(100);

        // Delete the key via MVCC.
        long startTs2 = nextTs();
        long commitTs2 = nextTs();
        prewriteDelete("del-key", startTs2);
        commit("del-key", startTs2, commitTs2);

        assertThat(latch.await(5, TimeUnit.SECONDS)).isTrue();

        var row = received.get(0).getEvents(0).getEntries().getEntries(0);
        assertThat(row.getType()).isEqualTo(Cdcpb.Row.OpType.DELETE);
        assertThat(row.getKey().toStringUtf8()).isEqualTo("del-key");

        reqObserver.onCompleted();
    }

    @Test
    void deregisterStopsEvents() throws Exception {
        var received = new CopyOnWriteArrayList<Cdcpb.ChangeDataEvent>();

        var reqObserver = cdcStub.eventFeed(new StreamObserver<>() {
            @Override public void onNext(Cdcpb.ChangeDataEvent event) {
                received.add(event);
            }
            @Override public void onError(Throwable t) {}
            @Override public void onCompleted() {}
        });

        reqObserver.onNext(Cdcpb.ChangeDataRequest.newBuilder()
                .setRegionId(1)
                .setRegister(Cdcpb.Register.newBuilder())
                .build());
        Thread.sleep(100);

        // Deregister immediately.
        reqObserver.onNext(Cdcpb.ChangeDataRequest.newBuilder()
                .setRegionId(1)
                .setDeregister(Cdcpb.Deregister.newBuilder())
                .build());
        Thread.sleep(100);

        // Commit a key — should NOT produce a CDC event.
        long startTs = nextTs();
        long commitTs = nextTs();
        prewrite("no-event-key", "val", startTs);
        commit("no-event-key", startTs, commitTs);

        Thread.sleep(300);
        assertThat(received).isEmpty();

        reqObserver.onCompleted();
    }

    @Test
    void rollbackEventDelivered() throws Exception {
        var received = new CopyOnWriteArrayList<Cdcpb.ChangeDataEvent>();
        var latch = new CountDownLatch(1);

        var reqObserver = cdcStub.eventFeed(new StreamObserver<>() {
            @Override public void onNext(Cdcpb.ChangeDataEvent event) {
                received.add(event);
                latch.countDown();
            }
            @Override public void onError(Throwable t) {}
            @Override public void onCompleted() {}
        });

        reqObserver.onNext(Cdcpb.ChangeDataRequest.newBuilder()
                .setRegionId(1)
                .setRegister(Cdcpb.Register.newBuilder())
                .build());
        Thread.sleep(100);

        long startTs = nextTs();
        prewrite("rb-key", "rb-val", startTs);
        rollback("rb-key", startTs);

        assertThat(latch.await(5, TimeUnit.SECONDS)).isTrue();

        var row = received.get(0).getEvents(0).getEntries().getEntries(0);
        assertThat(row.getType()).isEqualTo(Cdcpb.Row.OpType.ROLLBACK);
        assertThat(row.getKey().toStringUtf8()).isEqualTo("rb-key");
        assertThat(row.getStartTs()).isEqualTo(startTs);
        assertThat(row.getCommitTs()).isEqualTo(0);

        reqObserver.onCompleted();
    }

    @Test
    void multiKeyCommitProducesMultipleEvents() throws Exception {
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

        reqObserver.onNext(Cdcpb.ChangeDataRequest.newBuilder()
                .setRegionId(1)
                .setRegister(Cdcpb.Register.newBuilder())
                .build());
        Thread.sleep(100);

        long startTs = nextTs();
        long commitTs = nextTs();

        // Prewrite two keys in one batch.
        tikv.kvPrewrite(Kvrpcpb.PrewriteRequest.newBuilder()
                .setStartVersion(startTs)
                .setPrimaryLock(ByteString.copyFromUtf8("mk1"))
                .setLockTtl(5_000)
                .addMutations(Kvrpcpb.Mutation.newBuilder()
                        .setOp(Kvrpcpb.Op.Put)
                        .setKey(ByteString.copyFromUtf8("mk1"))
                        .setValue(ByteString.copyFromUtf8("v1")))
                .addMutations(Kvrpcpb.Mutation.newBuilder()
                        .setOp(Kvrpcpb.Op.Put)
                        .setKey(ByteString.copyFromUtf8("mk2"))
                        .setValue(ByteString.copyFromUtf8("v2")))
                .build());

        tikv.kvCommit(Kvrpcpb.CommitRequest.newBuilder()
                .setStartVersion(startTs)
                .setCommitVersion(commitTs)
                .addKeys(ByteString.copyFromUtf8("mk1"))
                .addKeys(ByteString.copyFromUtf8("mk2"))
                .build());

        assertThat(latch.await(5, TimeUnit.SECONDS)).isTrue();

        // Collect all rows across all events.
        var allRows = received.stream()
                .flatMap(e -> e.getEventsList().stream())
                .flatMap(e -> e.getEntries().getEntriesList().stream())
                .toList();
        assertThat(allRows).hasSize(2);
        assertThat(allRows.stream().map(r -> r.getKey().toStringUtf8()).sorted().toList())
                .containsExactly("mk1", "mk2");

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

    private void prewriteDelete(String key, long startTs) {
        var resp = tikv.kvPrewrite(Kvrpcpb.PrewriteRequest.newBuilder()
                .setStartVersion(startTs)
                .setPrimaryLock(ByteString.copyFromUtf8(key))
                .setLockTtl(5_000)
                .addMutations(Kvrpcpb.Mutation.newBuilder()
                        .setOp(Kvrpcpb.Op.Del)
                        .setKey(ByteString.copyFromUtf8(key)))
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

    private void rollback(String key, long startTs) {
        var resp = tikv.kvBatchRollback(Kvrpcpb.BatchRollbackRequest.newBuilder()
                .setStartVersion(startTs)
                .addKeys(ByteString.copyFromUtf8(key))
                .build());
        assertThat(resp.getError().getSerializedSize()).isZero();
    }
}
