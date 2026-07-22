package io.github.xinfra.lab.xkv.test;

import com.google.protobuf.ByteString;
import io.github.xinfra.lab.xkv.common.metrics.GrpcServerMetricsInterceptor;
import io.github.xinfra.lab.xkv.common.metrics.MetricsHttpServer;
import io.github.xinfra.lab.xkv.kv.server.DebugServiceImpl;
import io.github.xinfra.lab.xkv.kv.server.TikvServiceImpl;
import io.github.xinfra.lab.xkv.proto.DebugGrpc;
import io.github.xinfra.lab.xkv.proto.Debugpb;
import io.github.xinfra.lab.xkv.proto.Kvrpcpb;
import io.github.xinfra.lab.xkv.proto.TikvGrpc;
import io.micrometer.prometheusmetrics.PrometheusConfig;
import io.micrometer.prometheusmetrics.PrometheusMeterRegistry;
import io.grpc.ManagedChannel;
import io.grpc.Server;
import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder;
import io.grpc.netty.shaded.io.grpc.netty.NettyServerBuilder;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Path;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

final class MetricsE2ETest {

    @TempDir Path dataDir;

    private Server server;
    private ManagedChannel channel;
    private PrometheusMeterRegistry registry;
    private MetricsHttpServer metricsHttp;

    @AfterEach
    void tearDown() throws Exception {
        if (channel != null) channel.shutdownNow().awaitTermination(2, TimeUnit.SECONDS);
        if (server != null) server.shutdownNow().awaitTermination(2, TimeUnit.SECONDS);
        if (metricsHttp != null) metricsHttp.close();
        if (registry != null) registry.close();
        TestCluster.releaseAllPorts();
    }

    @Test
    void metricsInterceptorAndHttpEndpoint() throws Exception {
        registry = new PrometheusMeterRegistry(PrometheusConfig.DEFAULT);
        int grpcPort = TestCluster.freePort();
        TestCluster.releasePort(grpcPort);

        server = NettyServerBuilder.forPort(grpcPort)
                .addService(new TikvServiceImpl())
                .addService(new DebugServiceImpl(registry))
                .intercept(new GrpcServerMetricsInterceptor(registry))
                .build()
                .start();

        metricsHttp = new MetricsHttpServer(0, registry);
        int httpPort = metricsHttp.port();

        channel = NettyChannelBuilder.forAddress("127.0.0.1", grpcPort)
                .usePlaintext().build();
        var tikv = TikvGrpc.newBlockingStub(channel);

        // Make some RPCs (service has no backing engine, so they'll fail
        // internally, but the interceptor still records them).
        for (int i = 0; i < 3; i++) {
            try {
                tikv.rawGet(Kvrpcpb.RawGetRequest.newBuilder()
                        .setKey(ByteString.copyFromUtf8("k" + i)).build());
            } catch (Exception e) { e.printStackTrace(); }
        }

        // Verify HTTP /metrics endpoint
        var httpClient = HttpClient.newHttpClient();
        var resp = httpClient.send(
                HttpRequest.newBuilder()
                        .uri(URI.create("http://127.0.0.1:" + httpPort + "/metrics"))
                        .build(),
                HttpResponse.BodyHandlers.ofString());
        assertThat(resp.statusCode()).isEqualTo(200);
        assertThat(resp.body()).contains("grpc_server_requests_total");
        assertThat(resp.body()).contains("grpc_server_request_duration_seconds");

        // Verify gRPC GetMetrics RPC
        var debugStub = DebugGrpc.newBlockingStub(channel);
        var metricsResp = debugStub.getMetrics(
                Debugpb.GetMetricsRequest.newBuilder().build());
        String payload = metricsResp.getPayload().toStringUtf8();
        assertThat(payload).contains("grpc_server_requests_total");
    }
}
