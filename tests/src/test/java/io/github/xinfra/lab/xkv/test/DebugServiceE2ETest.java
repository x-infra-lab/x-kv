package io.github.xinfra.lab.xkv.test;

import io.github.xinfra.lab.raft.Peer;
import io.github.xinfra.lab.xkv.kv.config.KvConfig;
import io.github.xinfra.lab.xkv.kv.engine.PerRegionRaftEngine;
import io.github.xinfra.lab.xkv.kv.engine.RocksStorageEngine;
import io.github.xinfra.lab.xkv.kv.engine.StorageEngine;
import io.github.xinfra.lab.xkv.kv.raft.CompositeApplyHandler;
import io.github.xinfra.lab.xkv.kv.raft.RegionPeerImpl;
import io.github.xinfra.lab.xkv.kv.server.DebugServiceImpl;
import io.github.xinfra.lab.xkv.kv.store.StoreImpl;
import io.github.xinfra.lab.xkv.kv.transport.GrpcRaftTransport;
import io.github.xinfra.lab.xkv.proto.DebugGrpc;
import io.github.xinfra.lab.xkv.proto.Debugpb;
import io.github.xinfra.lab.xkv.proto.Metapb;
import io.grpc.ManagedChannel;
import io.grpc.Server;
import io.grpc.StatusRuntimeException;
import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder;
import io.grpc.netty.shaded.io.grpc.netty.NettyServerBuilder;
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
import static org.assertj.core.api.Assertions.assertThatThrownBy;

final class DebugServiceE2ETest {

    @TempDir Path dataDir;

    private RocksStorageEngine engine;
    private RegionPeerImpl peer;
    private StoreImpl store;
    private Server server;
    private ManagedChannel channel;
    private DebugGrpc.DebugBlockingStub stub;
    private GrpcRaftTransport transport;

    @BeforeEach
    void setUp() throws Exception {
        engine = RocksStorageEngine.open(dataDir, KvConfig.EngineConfig.defaults());
        var raftEngine = new PerRegionRaftEngine(engine, 1);

        var region = Metapb.Region.newBuilder()
                .setId(1)
                .setRegionEpoch(Metapb.RegionEpoch.newBuilder().setConfVer(1).setVersion(1))
                .addPeers(Metapb.Peer.newBuilder().setId(1).setStoreId(1).setRole(Metapb.PeerRole.Voter))
                .build();
        var self = region.getPeers(0);

        transport = new GrpcRaftTransport(1, 1);
        var cm = new io.github.xinfra.lab.xkv.kv.mvcc.ConcurrencyManager(
                new io.github.xinfra.lab.xkv.kv.mvcc.MaxTsTracker(0));
        peer = new RegionPeerImpl(
                engine, raftEngine, region, self,
                List.of(new Peer(1)),
                transport,
                CompositeApplyHandler.defaultFor(engine, cm)
                        .withAdmin(raftEngine, engine, (p, ch) -> {}, (t, s) -> {}),
                new RegionPeerImpl.Settings(10, 1, 30),
                cm);

        var storeMeta = Metapb.Store.newBuilder()
                .setId(1).setAddress("127.0.0.1:20160")
                .setPeerAddress("127.0.0.1:20170")
                .setState(Metapb.StoreState.Up)
                .build();
        store = new StoreImpl(1, storeMeta);
        store.registerPeer(peer);

        Awaitility.await().atMost(Duration.ofSeconds(10))
                .until(peer::isLeader);

        int port = ClusterHarness.freePort();
        ClusterHarness.releasePort(port);
        server = NettyServerBuilder.forPort(port)
                .addService(new DebugServiceImpl(null, store, engine, storeMeta, dataDir))
                .build()
                .start();
        channel = NettyChannelBuilder.forAddress("127.0.0.1", port)
                .usePlaintext().build();
        stub = DebugGrpc.newBlockingStub(channel);
    }

    @AfterEach
    void tearDown() throws Exception {
        if (channel != null) channel.shutdownNow().awaitTermination(2, TimeUnit.SECONDS);
        if (server != null) server.shutdownNow().awaitTermination(2, TimeUnit.SECONDS);
        if (peer != null) try { peer.shutdown(); } catch (Throwable e) { e.printStackTrace(); }
        if (transport != null) try { transport.close(); } catch (Throwable e) { e.printStackTrace(); }
        if (engine != null) try { engine.close(); } catch (Throwable e) { e.printStackTrace(); }
        ClusterHarness.releaseAllPorts();
    }

    @Test
    void getRegionInfo_returnsRegionState() {
        var resp = stub.getRegionInfo(
                Debugpb.GetRegionInfoRequest.newBuilder().setRegionId(1).build());
        assertThat(resp.getRegion().getId()).isEqualTo(1);
        assertThat(resp.getTerm()).isGreaterThan(0);
        assertThat(resp.hasLeader()).isTrue();
        assertThat(resp.getLeader().getId()).isEqualTo(1);
    }

    @Test
    void getRegionInfo_notFound() {
        assertThatThrownBy(() -> stub.getRegionInfo(
                Debugpb.GetRegionInfoRequest.newBuilder().setRegionId(999).build()))
                .isInstanceOf(StatusRuntimeException.class)
                .hasMessageContaining("NOT_FOUND");
    }

    @Test
    void getRaftState_returnsRaftMeta() {
        var resp = stub.getRaftState(
                Debugpb.GetRaftStateRequest.newBuilder().setRegionId(1).build());
        assertThat(resp.getRegionId()).isEqualTo(1);
        assertThat(resp.getTerm()).isGreaterThan(0);
        assertThat(resp.getFirstIndex()).isGreaterThanOrEqualTo(0);
    }

    @Test
    void getSnapshotMeta_returnsSnapshotInfo() {
        var resp = stub.getSnapshotMeta(
                Debugpb.GetSnapshotMetaRequest.newBuilder().setRegionId(1).build());
        assertThat(resp.getExists()).isTrue();
        assertThat(resp.getCfNamesList()).containsExactly("default", "lock", "write");
    }

    @Test
    void getClusterInfo_returnsLocalView() {
        var resp = stub.getClusterInfo(
                Debugpb.GetClusterInfoRequest.newBuilder().build());
        assertThat(resp.getStoreCount()).isEqualTo(1);
        assertThat(resp.getRegionCount()).isEqualTo(1);
    }

    @Test
    void getStoreInfo_returnsDiskStats() {
        var resp = stub.getStoreInfo(
                Debugpb.GetStoreInfoRequest.newBuilder().build());
        assertThat(resp.getStore().getId()).isEqualTo(1);
        assertThat(resp.getCapacity()).isGreaterThan(0);
        assertThat(resp.getRegionCount()).isEqualTo(1);
    }

    @Test
    void compactionEvent_succeeds() {
        var resp = stub.compactionEvent(
                Debugpb.CompactionEventRequest.newBuilder()
                        .setCf("DEFAULT")
                        .build());
        assertThat(resp).isNotNull();
    }

    @Test
    void compactionEvent_unknownCf() {
        assertThatThrownBy(() -> stub.compactionEvent(
                Debugpb.CompactionEventRequest.newBuilder()
                        .setCf("nonexistent")
                        .build()))
                .isInstanceOf(StatusRuntimeException.class)
                .hasMessageContaining("INVALID_ARGUMENT");
    }

    @Test
    void getAllRegions_returnsAllRegions() {
        var resp = stub.getAllRegions(
                Debugpb.GetAllRegionsRequest.newBuilder().build());
        assertThat(resp.getRegionsList()).hasSize(1);
        assertThat(resp.getRegions(0).getId()).isEqualTo(1);
    }

    @Test
    void unsafeForceLeader_returnsUnimplemented() {
        assertThatThrownBy(() -> stub.unsafeForceLeader(
                Debugpb.UnsafeForceLeaderRequest.newBuilder()
                        .setRegionId(1).build()))
                .isInstanceOf(StatusRuntimeException.class)
                .hasMessageContaining("UNIMPLEMENTED");
    }

    @Test
    void getMetrics_stillWorks() {
        var resp = stub.getMetrics(
                Debugpb.GetMetricsRequest.newBuilder().build());
        assertThat(resp.getContentType()).contains("text/plain");
    }
}
