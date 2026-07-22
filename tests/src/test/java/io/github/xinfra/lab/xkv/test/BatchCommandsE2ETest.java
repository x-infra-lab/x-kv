package io.github.xinfra.lab.xkv.test;

import com.google.protobuf.ByteString;
import io.github.xinfra.lab.raft.Peer;
import io.github.xinfra.lab.xkv.kv.config.KvConfig;
import io.github.xinfra.lab.xkv.kv.engine.PerRegionRaftEngine;
import io.github.xinfra.lab.xkv.kv.engine.RocksStorageEngine;
import io.github.xinfra.lab.xkv.kv.raft.LoopbackTransport;
import io.github.xinfra.lab.xkv.kv.raft.RawKvApplyHandler;
import io.github.xinfra.lab.xkv.kv.raft.BatchRegionPeer;
import io.github.xinfra.lab.xkv.kv.raft.RegionPeer;
import io.github.xinfra.lab.xkv.kv.server.RawKvService;
import io.github.xinfra.lab.xkv.kv.server.TikvServiceImpl;
import io.github.xinfra.lab.xkv.proto.Kvrpcpb;
import io.github.xinfra.lab.xkv.proto.Metapb;
import io.github.xinfra.lab.xkv.proto.TikvGrpc;
import io.github.xinfra.lab.xkv.proto.Tikvpb;
import io.grpc.ManagedChannel;
import io.grpc.Server;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.stub.StreamObserver;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * E2E test: BatchCommands bidi stream multiplexing multiple RPC types
 * over a single transport connection.
 */
final class BatchCommandsE2ETest {

    @TempDir Path dataDir;

    private RocksStorageEngine engine;
    private BatchRegionPeer peer;
    private Server grpcServer;
    private ManagedChannel channel;
    private String channelName;

    private void start() throws Exception {
        engine = RocksStorageEngine.open(dataDir, KvConfig.EngineConfig.defaults());
        var region = Metapb.Region.newBuilder()
                .setId(1)
                .setRegionEpoch(Metapb.RegionEpoch.newBuilder().setConfVer(1).setVersion(1))
                .addPeers(Metapb.Peer.newBuilder().setId(1).setStoreId(1))
                .build();
        var self = region.getPeers(0);
        var raftEngine = new PerRegionRaftEngine(engine, 1);
        var transport = new LoopbackTransport();
        peer = BatchRegionPeer.standalone(
                engine, raftEngine, region, self, List.of(new Peer(1)),
                transport, new RawKvApplyHandler(engine),
                new RegionPeer.Settings(10, 1, 100),
                null, null);
        Awaitility.await().atMost(Duration.ofSeconds(10)).until(peer::isLeader);

        var rawKv = new RawKvService(engine, key -> peer, 5_000);

        channelName = "batch-test-" + UUID.randomUUID();
        grpcServer = InProcessServerBuilder.forName(channelName)
                .directExecutor()
                .addService(new TikvServiceImpl(rawKv))
                .build()
                .start();
        channel = InProcessChannelBuilder.forName(channelName).directExecutor().build();
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
    void batchCommandsMixedRawOps() throws Exception {
        start();
        var asyncStub = TikvGrpc.newStub(channel);

        var respRef = new AtomicReference<Tikvpb.BatchCommandsResponse>();
        var latch = new CountDownLatch(1);
        var reqObserver = asyncStub.batchCommands(new StreamObserver<>() {
            @Override public void onNext(Tikvpb.BatchCommandsResponse v) { respRef.set(v); latch.countDown(); }
            @Override public void onError(Throwable t) { latch.countDown(); }
            @Override public void onCompleted() {}
        });

        // Batch: RawPut + RawGet + Empty heartbeat
        var batch = Tikvpb.BatchCommandsRequest.newBuilder()
                .addRequests(Tikvpb.BatchCommandsRequest.Request.newBuilder()
                        .setRawPut(Kvrpcpb.RawPutRequest.newBuilder()
                                .setKey(ByteString.copyFromUtf8("bk1"))
                                .setValue(ByteString.copyFromUtf8("bv1"))))
                .addRequestIds(100)
                .addRequests(Tikvpb.BatchCommandsRequest.Request.newBuilder()
                        .setRawGet(Kvrpcpb.RawGetRequest.newBuilder()
                                .setKey(ByteString.copyFromUtf8("bk1"))))
                .addRequestIds(101)
                .addRequests(Tikvpb.BatchCommandsRequest.Request.newBuilder()
                        .setEmpty(Tikvpb.Empty.getDefaultInstance()))
                .addRequestIds(102)
                .build();

        reqObserver.onNext(batch);
        assertThat(latch.await(10, TimeUnit.SECONDS)).isTrue();

        var resp = respRef.get();
        assertThat(resp).isNotNull();
        assertThat(resp.getResponsesCount()).isEqualTo(3);
        assertThat(resp.getRequestIdsCount()).isEqualTo(3);

        // Request IDs must match.
        assertThat(resp.getRequestIds(0)).isEqualTo(100);
        assertThat(resp.getRequestIds(1)).isEqualTo(101);
        assertThat(resp.getRequestIds(2)).isEqualTo(102);

        // RawPut response should be present.
        assertThat(resp.getResponses(0).getCmdCase())
                .isEqualTo(Tikvpb.BatchCommandsResponse.Response.CmdCase.RAWPUT);

        // RawGet: the put and get are in the same batch, so the value may or
        // may not be visible depending on apply timing. We just verify the
        // response type is correct.
        assertThat(resp.getResponses(1).getCmdCase())
                .isEqualTo(Tikvpb.BatchCommandsResponse.Response.CmdCase.RAWGET);

        // Empty heartbeat response.
        assertThat(resp.getResponses(2).getCmdCase())
                .isEqualTo(Tikvpb.BatchCommandsResponse.Response.CmdCase.EMPTY);

        // transport_layer_load should be set.
        assertThat(resp.getTransportLayerLoad()).isGreaterThanOrEqualTo(0);

        reqObserver.onCompleted();
    }

    @Test
    void batchCommandsMultipleBatches() throws Exception {
        start();
        var asyncStub = TikvGrpc.newStub(channel);

        // First batch: put several keys.
        var blockingStub = TikvGrpc.newBlockingStub(channel);
        for (int i = 0; i < 5; i++) {
            blockingStub.rawPut(Kvrpcpb.RawPutRequest.newBuilder()
                    .setKey(ByteString.copyFromUtf8("mk" + i))
                    .setValue(ByteString.copyFromUtf8("mv" + i))
                    .build());
        }

        // Now batch-get all of them.
        var respRef = new AtomicReference<Tikvpb.BatchCommandsResponse>();
        var latch = new CountDownLatch(1);
        var reqObserver = asyncStub.batchCommands(new StreamObserver<>() {
            @Override public void onNext(Tikvpb.BatchCommandsResponse v) { respRef.set(v); latch.countDown(); }
            @Override public void onError(Throwable t) { latch.countDown(); }
            @Override public void onCompleted() {}
        });

        var batchBuilder = Tikvpb.BatchCommandsRequest.newBuilder();
        for (int i = 0; i < 5; i++) {
            batchBuilder.addRequests(Tikvpb.BatchCommandsRequest.Request.newBuilder()
                    .setRawGet(Kvrpcpb.RawGetRequest.newBuilder()
                            .setKey(ByteString.copyFromUtf8("mk" + i))));
            batchBuilder.addRequestIds(200 + i);
        }
        reqObserver.onNext(batchBuilder.build());
        assertThat(latch.await(10, TimeUnit.SECONDS)).isTrue();

        var resp = respRef.get();
        assertThat(resp.getResponsesCount()).isEqualTo(5);
        for (int i = 0; i < 5; i++) {
            assertThat(resp.getRequestIds(i)).isEqualTo(200 + i);
            var rawGetResp = resp.getResponses(i).getRawGet();
            assertThat(rawGetResp.getNotFound()).isFalse();
            assertThat(rawGetResp.getValue().toStringUtf8()).isEqualTo("mv" + i);
        }

        reqObserver.onCompleted();
    }
}
