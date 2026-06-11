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
import io.github.xinfra.lab.xkv.kv.raft.RegionPeerImpl;
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

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Verifies that max_ts survives a process restart.
 *
 * <p>The v1 bug: leader restart reset max_ts to 0; the new leader would
 * stamp a prewrite with min_commit_ts ≤ a still-in-flight reader's
 * read_ts, breaking SI for that reader.
 *
 * <p>The fix: the apply loop opportunistically persists the in-memory
 * max_ts (under the writer lock, so no reader is concurrently raising it).
 * On restart, the {@link MaxTsTracker} bootstraps from
 * {@link PerRegionRaftEngine#persistedMaxTs}.
 */
final class MaxTsPersistenceTest {

    @TempDir Path dataDir;
    private RocksStorageEngine engine;
    private PerRegionRaftEngine raftEngine;
    private RegionPeerImpl peer;
    private ConcurrencyManager cm;
    private Server grpcServer;
    private ManagedChannel channel;
    private TikvGrpc.TikvBlockingStub tikv;

    @BeforeEach
    void start() throws Exception {
        bootstrap();
    }

    @AfterEach
    void stop() {
        teardown();
    }

    @Test
    void maxTsSurvivesRestart() throws Exception {
        long highReadTs = 12_345_678_900L;

        // Drive max_ts via a read at a known timestamp.
        tikv.kvGet(Kvrpcpb.GetRequest.newBuilder()
                .setKey(ByteString.copyFromUtf8("k"))
                .setVersion(highReadTs)
                .build());
        assertThat(cm.maxTs().current()).isGreaterThanOrEqualTo(highReadTs);

        // Trigger an apply round so the max_ts is persisted (writes a value
        // so applyReady runs through Phase B).
        tikv.kvPrewrite(Kvrpcpb.PrewriteRequest.newBuilder()
                .setStartVersion(100)
                .setLockTtl(3000)
                .setPrimaryLock(ByteString.copyFromUtf8("k"))
                .addMutations(Kvrpcpb.Mutation.newBuilder()
                        .setOp(Kvrpcpb.Op.Put)
                        .setKey(ByteString.copyFromUtf8("k"))
                        .setValue(ByteString.copyFromUtf8("v")))
                .build());
        // Wait for the apply loop to have run and flushed.
        Awaitility.await().atMost(Duration.ofSeconds(2))
                .pollInterval(Duration.ofMillis(20))
                .until(() -> raftEngine.persistedMaxTs() >= highReadTs);

        // Now restart the whole peer + engine.
        teardown();
        bootstrap();

        // The reloaded tracker MUST contain the persisted floor — otherwise
        // a future prewrite could stamp min_commit_ts ≤ highReadTs.
        assertThat(cm.maxTs().current())
                .as("max_ts must reload from disk")
                .isGreaterThanOrEqualTo(highReadTs);
        assertThat(cm.maxTs().minCommitTsFloor())
                .isGreaterThan(highReadTs);
    }

    // ===== bootstrap helpers =====

    private void bootstrap() throws Exception {
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
                CompositeApplyHandler.defaultFor(engine, cm).withAdmin(raftEngine),
                new RegionPeerImpl.Settings(10, 1, 30),
                cm);
        Awaitility.await().atMost(Duration.ofSeconds(10)).until(peer::isLeader);

        var rawKv = new RawKvService(engine, key -> peer, 5_000);
        var txn = new TransactionService(engine, key -> peer, 5_000, cm);

        var name = "max-ts-" + UUID.randomUUID();
        grpcServer = InProcessServerBuilder.forName(name).directExecutor()
                .addService(new TikvServiceImpl(rawKv, txn))
                .build().start();
        channel = InProcessChannelBuilder.forName(name).directExecutor().build();
        tikv = TikvGrpc.newBlockingStub(channel);
    }

    private void teardown() {
        if (channel != null) try { channel.shutdownNow().awaitTermination(2, TimeUnit.SECONDS); } catch (Exception ignored) {}
        if (grpcServer != null) {
            grpcServer.shutdownNow();
            try { grpcServer.awaitTermination(2, TimeUnit.SECONDS); } catch (InterruptedException ignored) {}
        }
        if (peer != null) try { peer.shutdown(); } catch (Exception ignored) {}
        if (engine != null) try { engine.close(); } catch (Exception ignored) {}
    }
}
