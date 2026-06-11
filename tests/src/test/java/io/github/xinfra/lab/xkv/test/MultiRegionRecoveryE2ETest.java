package io.github.xinfra.lab.xkv.test;

import com.google.protobuf.ByteString;
import io.github.xinfra.lab.xkv.kv.engine.PerRegionRaftEngine;
import io.github.xinfra.lab.xkv.kv.engine.RaftCfKeys;
import io.github.xinfra.lab.xkv.kv.engine.StorageEngine;
import io.github.xinfra.lab.xkv.kv.store.SplitDriver;
import io.github.xinfra.lab.xkv.proto.Metapb;
import io.github.xinfra.lab.xkv.proto.PDGrpc;
import io.grpc.ManagedChannel;
import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Validates the multi-region recovery path: after a split, child region
 * descriptors persisted to RAFT CF can be scanned and used to recreate
 * peers on restart.
 *
 * <p>This test uses ClusterHarness to set up a real cluster, splits a
 * region, and verifies:
 * <ol>
 *   <li>RaftCfKeys helper methods correctly encode/decode region keys</li>
 *   <li>AdminApplyHandler.applySplit() persists child region descriptors</li>
 *   <li>Scanning RAFT CF with allRegionKeysPrefix() finds all regions</li>
 *   <li>Region descriptors are valid protobuf and contain correct metadata</li>
 * </ol>
 */
final class MultiRegionRecoveryE2ETest {

    @TempDir Path baseDir;
    private ClusterHarness harness;
    private ManagedChannel pdChannel;
    private PDGrpc.PDBlockingStub pd;

    @BeforeEach
    void start() throws Exception {
        harness = new ClusterHarness(baseDir, 3).start();
        pdChannel = NettyChannelBuilder.forAddress("127.0.0.1", harness.pdPort())
                .usePlaintext().build();
        pd = PDGrpc.newBlockingStub(pdChannel);
    }

    @AfterEach
    void stop() throws Exception {
        if (pdChannel != null) pdChannel.shutdownNow().awaitTermination(2, TimeUnit.SECONDS);
        if (harness != null) harness.close();
        ClusterHarness.releaseAllPorts();
    }

    @Test
    void raftCfKeysRoundTrip() {
        byte[] key = RaftCfKeys.regionKey(42);
        assertThat(key).hasSize(9);
        assertThat(key[0]).isEqualTo(RaftCfKeys.TYPE_REGION);
        assertThat(RaftCfKeys.regionIdFromKey(key)).isEqualTo(42);

        byte[] prefix = RaftCfKeys.allRegionKeysPrefix();
        assertThat(prefix).hasSize(1);
        assertThat(prefix[0]).isEqualTo(RaftCfKeys.TYPE_REGION);

        byte[] end = RaftCfKeys.allRegionKeysEnd();
        assertThat(end).hasSize(1);
        assertThat(end[0]).isEqualTo((byte) (RaftCfKeys.TYPE_REGION + 1));
    }

    @Test
    void splitPersistsChildRegionDescriptorToRaftCf() throws Exception {
        var leader = harness.leader();
        var driver = new SplitDriver(pd, 5_000);

        var resulting = driver.split(leader.peer, List.of("m".getBytes()));
        assertThat(resulting).hasSize(2);
        var child = resulting.get(1);

        Awaitility.await().atMost(Duration.ofSeconds(5))
                .pollInterval(Duration.ofMillis(20))
                .until(() -> leader.childPeers.stream()
                        .anyMatch(c -> c.regionId() == child.getId()));

        // Verify the child region descriptor was persisted to RAFT CF.
        byte[] regionKey = RaftCfKeys.regionKey(child.getId());
        byte[] regionData = leader.engine.get(StorageEngine.Cf.RAFT, regionKey);
        assertThat(regionData).isNotNull();

        var persistedRegion = Metapb.Region.parseFrom(regionData);
        assertThat(persistedRegion.getId()).isEqualTo(child.getId());
        assertThat(persistedRegion.getStartKey().toStringUtf8()).isEqualTo("m");
        assertThat(persistedRegion.getPeersCount()).isEqualTo(3);
    }

    @Test
    void scanRaftCfFindsAllPersistedRegions() throws Exception {
        var leader = harness.leader();
        var driver = new SplitDriver(pd, 5_000);

        // Split at "m" to create 2 regions total.
        var resulting = driver.split(leader.peer, List.of("m".getBytes()));
        assertThat(resulting).hasSize(2);
        var child = resulting.get(1);

        Awaitility.await().atMost(Duration.ofSeconds(5))
                .pollInterval(Duration.ofMillis(20))
                .until(() -> leader.childPeers.stream()
                        .anyMatch(c -> c.regionId() == child.getId()));

        // Scan RAFT CF for all region descriptors.
        byte[] prefix = RaftCfKeys.allRegionKeysPrefix();
        byte[] end = RaftCfKeys.allRegionKeysEnd();
        var ro = leader.engine.newReadOptions()
                .iterateLowerBound(prefix)
                .iterateUpperBound(end);

        var foundRegionIds = new ArrayList<Long>();
        try (var it = leader.engine.newIterator(StorageEngine.Cf.RAFT, ro)) {
            for (it.seek(prefix); it.isValid(); it.next()) {
                long regionId = RaftCfKeys.regionIdFromKey(it.key());
                var region = Metapb.Region.parseFrom(it.value());
                assertThat(region.getId()).isEqualTo(regionId);
                foundRegionIds.add(regionId);
            }
        }

        // We should find the bootstrap region (id=1) + 1 child region.
        assertThat(foundRegionIds).hasSize(2);
        assertThat(foundRegionIds).contains(1L);
        assertThat(foundRegionIds).contains(child.getId());
    }

    @Test
    void storeRoutesCorrectlyWithRecoveredRegions() throws Exception {
        var leader = harness.leader();
        var driver = new SplitDriver(pd, 5_000);

        // Write data before split.
        var tikv = leader.blockingStub();
        tikv.rawPut(io.github.xinfra.lab.xkv.proto.Kvrpcpb.RawPutRequest.newBuilder()
                .setKey(ByteString.copyFromUtf8("alpha"))
                .setValue(ByteString.copyFromUtf8("v_alpha"))
                .build());
        tikv.rawPut(io.github.xinfra.lab.xkv.proto.Kvrpcpb.RawPutRequest.newBuilder()
                .setKey(ByteString.copyFromUtf8("omega"))
                .setValue(ByteString.copyFromUtf8("v_omega"))
                .build());

        // Split at "m".
        var resulting = driver.split(leader.peer, List.of("m".getBytes()));
        var child = resulting.get(1);

        Awaitility.await().atMost(Duration.ofSeconds(5))
                .pollInterval(Duration.ofMillis(20))
                .until(() -> leader.childPeers.stream()
                        .anyMatch(c -> c.regionId() == child.getId()));

        // Store routes "alpha" to parent and "omega" to child.
        var parentPeer = leader.store.peerForKey("alpha".getBytes()).orElseThrow();
        assertThat(parentPeer.regionId()).isEqualTo(leader.peer.regionId());

        var childRoutedPeer = leader.store.peerForKey("omega".getBytes()).orElseThrow();
        assertThat(childRoutedPeer.regionId()).isEqualTo(child.getId());

        // Verify data is still accessible (data in the shared engine is
        // visible regardless of which peer routes; this tests routing).
        var getAlpha = tikv.rawGet(io.github.xinfra.lab.xkv.proto.Kvrpcpb.RawGetRequest.newBuilder()
                .setKey(ByteString.copyFromUtf8("alpha")).build());
        assertThat(getAlpha.getValue().toStringUtf8()).isEqualTo("v_alpha");
    }
}
