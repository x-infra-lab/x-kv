package io.github.xinfra.lab.xkv.test;

import io.github.xinfra.lab.xkv.kv.config.KvConfig;
import io.github.xinfra.lab.xkv.kv.server.KvServer;
import io.github.xinfra.lab.xkv.pd.config.PdConfig;
import io.github.xinfra.lab.xkv.pd.server.PdServer;
import io.github.xinfra.lab.xkv.proto.PDGrpc;
import io.github.xinfra.lab.xkv.proto.Pdpb;
import io.github.xinfra.lab.xkv.proto.TikvGrpc;
import io.github.xinfra.lab.xkv.proto.Kvrpcpb;
import io.grpc.ManagedChannel;
import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.Test;

import java.nio.file.Files;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Verify that both servers start, accept gRPC connections, and serve RPCs.
 *
 * <p>The KV test boots a real PD + KV pair and runs raw KV RPCs end-to-end
 * through the standalone {@link KvServer} (not the test harness). This
 * exercises the production bootstrap + wiring path.
 */
final class ServerStartupTest {

    @Test
    void pdStartsAndAnswersGetMembers() throws Exception {
        int port = ClusterHarness.freePort();
        int raftPort = ClusterHarness.freePort();
        var dir = Files.createTempDirectory("x-pd-startup");

        var cfg = PdConfig.builder()
                .nodeId(1)
                .clusterId(42)
                .clientAddress("127.0.0.1:" + port)
                .raftAddress("127.0.0.1:" + raftPort)
                .dataDir(dir)
                .build();

        ClusterHarness.releasePort(port);
        ClusterHarness.releasePort(raftPort);
        var pd = new PdServer(cfg);
        pd.start();
        try {
            ManagedChannel ch = NettyChannelBuilder.forAddress("127.0.0.1", port)
                    .usePlaintext()
                    .build();
            try {
                var stub = PDGrpc.newBlockingStub(ch);
                var resp = stub.getMembers(Pdpb.GetMembersRequest.getDefaultInstance());
                assertThat(resp.getMembersCount()).isEqualTo(1);
                assertThat(resp.getMembers(0).getMemberId()).isEqualTo(1L);
                assertThat(resp.getLeader().getMemberId()).isEqualTo(1L);
            } finally {
                ch.shutdownNow().awaitTermination(2, TimeUnit.SECONDS);
            }
        } finally {
            pd.stop();
            ClusterHarness.releaseAllPorts();
        }
    }

    @Test
    void kvServerBootstrapsWithPdAndServesRawKv() throws Exception {
        int pdPort = ClusterHarness.freePort();
        int pdRaftPort = ClusterHarness.freePort();
        var pdDir = Files.createTempDirectory("x-pd-kvboot");

        var pdCfg = PdConfig.builder()
                .nodeId(1).clusterId(1)
                .clientAddress("127.0.0.1:" + pdPort)
                .raftAddress("127.0.0.1:" + pdRaftPort)
                .dataDir(pdDir)
                .build();
        ClusterHarness.releasePort(pdPort);
        ClusterHarness.releasePort(pdRaftPort);
        var pd = new PdServer(pdCfg);
        pd.start();

        int kvClientPort = ClusterHarness.freePort();
        int kvRaftPort = ClusterHarness.freePort();
        var kvDir = Files.createTempDirectory("x-kv-kvboot");

        var kvCfg = KvConfig.builder()
                .storeId(1)
                .pdEndpoints(List.of("127.0.0.1:" + pdPort))
                .clientAddress("127.0.0.1:" + kvClientPort)
                .raftAddress("127.0.0.1:" + kvRaftPort)
                .dataDir(kvDir)
                .build();
        ClusterHarness.releasePort(kvClientPort);
        ClusterHarness.releasePort(kvRaftPort);
        var kv = new KvServer(kvCfg);
        kv.start();

        try {
            // Wait for the single-node raft to elect itself leader.
            Awaitility.await().atMost(Duration.ofSeconds(10))
                    .pollInterval(Duration.ofMillis(100))
                    .until(() -> kv.store().peers().stream()
                            .anyMatch(p -> p.isLeader()));

            ManagedChannel ch = NettyChannelBuilder.forAddress("127.0.0.1", kvClientPort)
                    .usePlaintext().build();
            try {
                var stub = TikvGrpc.newBlockingStub(ch);

                // RawPut
                var putResp = stub.rawPut(Kvrpcpb.RawPutRequest.newBuilder()
                        .setKey(com.google.protobuf.ByteString.copyFromUtf8("hello"))
                        .setValue(com.google.protobuf.ByteString.copyFromUtf8("world"))
                        .build());
                assertThat(putResp.getError()).isEmpty();

                // RawGet
                var getResp = stub.rawGet(Kvrpcpb.RawGetRequest.newBuilder()
                        .setKey(com.google.protobuf.ByteString.copyFromUtf8("hello"))
                        .build());
                assertThat(getResp.getNotFound()).isFalse();
                assertThat(getResp.getValue().toStringUtf8()).isEqualTo("world");

                // RawDelete
                var delResp = stub.rawDelete(Kvrpcpb.RawDeleteRequest.newBuilder()
                        .setKey(com.google.protobuf.ByteString.copyFromUtf8("hello"))
                        .build());
                assertThat(delResp.getError()).isEmpty();

                // Verify deleted
                var getResp2 = stub.rawGet(Kvrpcpb.RawGetRequest.newBuilder()
                        .setKey(com.google.protobuf.ByteString.copyFromUtf8("hello"))
                        .build());
                assertThat(getResp2.getNotFound()).isTrue();
            } finally {
                ch.shutdownNow().awaitTermination(2, TimeUnit.SECONDS);
            }
        } finally {
            kv.stop();
            pd.stop();
            ClusterHarness.releaseAllPorts();
        }
    }
}
