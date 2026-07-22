package io.github.xinfra.lab.xkv.test;

import com.google.protobuf.ByteString;
import io.github.xinfra.lab.xkv.proto.Kvrpcpb;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;

final class FollowerReadE2ETest {

    @TempDir Path dataDir;

    private TestCluster cluster;

    @AfterEach
    void tearDown() {
        if (cluster != null) cluster.close();
    }

    @Test
    void followerReadReturnsValueWrittenByLeader() throws Exception {
        cluster = new TestCluster(dataDir).startReplicated(1, 3);

        var leaderStore = cluster.leaderStoreFor(TestCluster.BOOTSTRAP_REGION_ID);
        var leaderStub = cluster.clientStub(leaderStore.storeId);

        leaderStub.rawPut(Kvrpcpb.RawPutRequest.newBuilder()
                .setKey(ByteString.copyFromUtf8("fr-k1"))
                .setValue(ByteString.copyFromUtf8("fr-v1"))
                .build());

        var followerStore = cluster.followerStoresFor(TestCluster.BOOTSTRAP_REGION_ID).get(0);
        var followerStub = cluster.clientStub(followerStore.storeId);

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
    }
}
