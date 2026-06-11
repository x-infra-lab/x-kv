package io.github.xinfra.lab.xkv.test;

import com.google.protobuf.ByteString;
import io.github.xinfra.lab.xkv.proto.Kvrpcpb;
import io.github.xinfra.lab.xkv.proto.TikvGrpc;
import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;

final class FollowerReadE2ETest {

    @TempDir Path dataDir;

    private ClusterHarness harness;

    @AfterEach
    void tearDown() {
        if (harness != null) harness.close();
    }

    @Test
    void followerReadReturnsValueWrittenByLeader() throws Exception {
        harness = new ClusterHarness(dataDir, 3).start();

        var leaderNode = harness.leader();
        var leaderStub = leaderNode.blockingStub();

        leaderStub.rawPut(Kvrpcpb.RawPutRequest.newBuilder()
                .setKey(ByteString.copyFromUtf8("fr-k1"))
                .setValue(ByteString.copyFromUtf8("fr-v1"))
                .build());

        var followerNode = harness.kvNodes().stream()
                .filter(n -> !n.peer.isLeader())
                .findFirst()
                .orElseThrow();

        var followerCh = NettyChannelBuilder.forAddress("127.0.0.1", followerNode.clientPort)
                .usePlaintext().build();
        try {
            var followerStub = TikvGrpc.newBlockingStub(followerCh);

            // follower read with LEADER mode should return NotLeader
            var leaderModeResp = followerStub.rawGet(Kvrpcpb.RawGetRequest.newBuilder()
                    .setKey(ByteString.copyFromUtf8("fr-k1"))
                    .setContext(Kvrpcpb.Context.newBuilder()
                            .setReplicaRead(Kvrpcpb.ReplicaReadType.LEADER))
                    .build());
            assertThat(leaderModeResp.hasRegionError()).isTrue();
            assertThat(leaderModeResp.getRegionError().hasNotLeader()).isTrue();

            // follower read with FOLLOWER mode should succeed
            Awaitility.await().atMost(Duration.ofSeconds(10))
                    .pollInterval(Duration.ofMillis(200))
                    .untilAsserted(() -> {
                        var resp = followerStub.rawGet(Kvrpcpb.RawGetRequest.newBuilder()
                                .setKey(ByteString.copyFromUtf8("fr-k1"))
                                .setContext(Kvrpcpb.Context.newBuilder()
                                        .setReplicaRead(Kvrpcpb.ReplicaReadType.FOLLOWER))
                                .build());
                        assertThat(resp.hasRegionError()).isFalse();
                        assertThat(resp.getValue().toStringUtf8()).isEqualTo("fr-v1");
                    });

            // Write a new value, then follower read should see it (linearizable)
            leaderStub.rawPut(Kvrpcpb.RawPutRequest.newBuilder()
                    .setKey(ByteString.copyFromUtf8("fr-k1"))
                    .setValue(ByteString.copyFromUtf8("fr-v2"))
                    .build());

            Awaitility.await().atMost(Duration.ofSeconds(10))
                    .pollInterval(Duration.ofMillis(200))
                    .untilAsserted(() -> {
                        var resp = followerStub.rawGet(Kvrpcpb.RawGetRequest.newBuilder()
                                .setKey(ByteString.copyFromUtf8("fr-k1"))
                                .setContext(Kvrpcpb.Context.newBuilder()
                                        .setReplicaRead(Kvrpcpb.ReplicaReadType.FOLLOWER))
                                .build());
                        assertThat(resp.hasRegionError()).isFalse();
                        assertThat(resp.getValue().toStringUtf8()).isEqualTo("fr-v2");
                    });
        } finally {
            followerCh.shutdownNow();
        }
    }
}
