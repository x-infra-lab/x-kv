package io.github.xinfra.lab.xkv.test;

import com.google.protobuf.ByteString;
import io.github.xinfra.lab.xkv.common.auth.AuthClientInterceptor;
import io.github.xinfra.lab.xkv.common.auth.AuthServerInterceptor;
import io.github.xinfra.lab.xkv.proto.Kvrpcpb;
import io.github.xinfra.lab.xkv.proto.TikvGrpc;
import io.grpc.ManagedChannel;
import io.grpc.Server;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder;
import io.grpc.netty.shaded.io.grpc.netty.NettyServerBuilder;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

final class AuthE2ETest {

    @TempDir Path dataDir;

    private ClusterHarness harness;

    @AfterEach
    void tearDown() {
        if (harness != null) harness.close();
        ClusterHarness.releaseAllPorts();
    }

    @Test
    void authInterceptorRejectsInvalidToken() throws Exception {
        harness = new ClusterHarness(dataDir, 1).start();
        var leader = harness.leader();

        int port = leader.clientPort;
        int authPort = ClusterHarness.freePort();
        ClusterHarness.releasePort(authPort);
        var server = NettyServerBuilder.forPort(authPort)
                .addService(new io.github.xinfra.lab.xkv.kv.server.TikvServiceImpl())
                .intercept(new AuthServerInterceptor("secret-token"))
                .build()
                .start();

        try {
            var noAuthCh = NettyChannelBuilder.forAddress("127.0.0.1", server.getPort())
                    .usePlaintext().build();
            var noAuthStub = TikvGrpc.newBlockingStub(noAuthCh);

            assertThatThrownBy(() -> noAuthStub.rawGet(
                    Kvrpcpb.RawGetRequest.newBuilder()
                            .setKey(ByteString.copyFromUtf8("k1")).build()))
                    .isInstanceOf(StatusRuntimeException.class)
                    .satisfies(e -> assertThat(((StatusRuntimeException) e).getStatus().getCode())
                            .isEqualTo(Status.Code.UNAUTHENTICATED));

            var wrongCh = NettyChannelBuilder.forAddress("127.0.0.1", server.getPort())
                    .usePlaintext()
                    .intercept(new AuthClientInterceptor("wrong-token"))
                    .build();
            var wrongStub = TikvGrpc.newBlockingStub(wrongCh);

            assertThatThrownBy(() -> wrongStub.rawGet(
                    Kvrpcpb.RawGetRequest.newBuilder()
                            .setKey(ByteString.copyFromUtf8("k1")).build()))
                    .isInstanceOf(StatusRuntimeException.class)
                    .satisfies(e -> assertThat(((StatusRuntimeException) e).getStatus().getCode())
                            .isEqualTo(Status.Code.UNAUTHENTICATED));

            var goodCh = NettyChannelBuilder.forAddress("127.0.0.1", server.getPort())
                    .usePlaintext()
                    .intercept(new AuthClientInterceptor("secret-token"))
                    .build();
            var goodStub = TikvGrpc.newBlockingStub(goodCh);

            // Valid token — should not throw UNAUTHENTICATED (the service has no
            // backing engine, so it will NPE, but the auth interceptor passes).
            try {
                goodStub.rawGet(Kvrpcpb.RawGetRequest.newBuilder()
                        .setKey(ByteString.copyFromUtf8("k1")).build());
            } catch (StatusRuntimeException e) {
                assertThat(e.getStatus().getCode()).isNotEqualTo(Status.Code.UNAUTHENTICATED);
            }

            noAuthCh.shutdownNow();
            wrongCh.shutdownNow();
            goodCh.shutdownNow();
        } finally {
            server.shutdownNow().awaitTermination(2, java.util.concurrent.TimeUnit.SECONDS);
        }
    }
}
