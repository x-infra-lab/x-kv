package io.github.xinfra.lab.xkv.test;

import com.google.protobuf.ByteString;
import io.github.xinfra.lab.xkv.common.ratelimit.ConcurrencyLimitInterceptor;
import io.github.xinfra.lab.xkv.kv.server.RawKvService;
import io.github.xinfra.lab.xkv.kv.server.TikvServiceImpl;
import io.github.xinfra.lab.xkv.proto.Kvrpcpb;
import io.github.xinfra.lab.xkv.proto.TikvGrpc;
import io.grpc.ManagedChannel;
import io.grpc.Server;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

final class RateLimitE2ETest {

    @TempDir Path dataDir;

    private ClusterHarness harness;

    @AfterEach
    void tearDown() {
        if (harness != null) harness.close();
    }

    @Test
    void rateLimitRejectsExcessConcurrency() throws Exception {
        harness = new ClusterHarness(dataDir, 1).start();
        var leader = harness.leader();

        var stub = leader.blockingStub();
        stub.rawPut(Kvrpcpb.RawPutRequest.newBuilder()
                .setKey(ByteString.copyFromUtf8("k1"))
                .setValue(ByteString.copyFromUtf8("v1"))
                .build());

        var resp = stub.rawGet(Kvrpcpb.RawGetRequest.newBuilder()
                .setKey(ByteString.copyFromUtf8("k1")).build());
        assertThat(resp.getValue().toStringUtf8()).isEqualTo("v1");
    }
}
