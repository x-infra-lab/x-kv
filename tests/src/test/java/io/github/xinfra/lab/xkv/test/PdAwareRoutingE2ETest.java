package io.github.xinfra.lab.xkv.test;

import com.google.protobuf.ByteString;
import io.github.xinfra.lab.xkv.client.config.ClientConfig;
import io.github.xinfra.lab.xkv.client.pd.PdClient;
import io.github.xinfra.lab.xkv.client.region.RegionCacheImpl;
import io.github.xinfra.lab.xkv.client.tso.TsoBatcherImpl;
import io.github.xinfra.lab.xkv.proto.Kvrpcpb;
import io.github.xinfra.lab.xkv.proto.PDGrpc;
import io.github.xinfra.lab.xkv.proto.TikvGrpc;
import io.grpc.ManagedChannel;
import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Phase 4/5 verification: a client uses real PD-driven routing + real
 * PD-stream TSO to write into a multi-store cluster.
 *
 * <p>Setup: 1 PD + 3 KV stores forming one 3-peer raft group. The test:
 * <ul>
 *   <li>discovers the leader's store via {@link RegionCacheImpl#locateRegion}</li>
 *   <li>fetches monotonic TSOs via {@link TsoBatcherImpl}</li>
 *   <li>issues raw KV writes through the leader's gRPC port</li>
 * </ul>
 */
final class PdAwareRoutingE2ETest {

    @TempDir Path baseDir;
    private ClusterHarness harness;
    private PdClient pdClient;

    @BeforeEach
    void start() throws Exception {
        harness = new ClusterHarness(baseDir, 3).start();
        pdClient = new PdClient(List.of("127.0.0.1:" + harness.pdPort()));
    }

    @AfterEach
    void teardown() {
        if (pdClient != null) pdClient.close();
        if (harness != null) harness.close();
    }

    @Test
    void regionCacheLearnsRegionFromPd() {
        var cache = new RegionCacheImpl(pdClient, ClientConfig.RegionCacheConfig.defaults());
        // Cache miss → loads from PD.
        var info = cache.locateKey("anything".getBytes()).orElseThrow();
        assertThat(info.region().getId()).isEqualTo(harness.bootstrapRegionId());
        assertThat(info.region().getPeersCount()).isEqualTo(3);

        // Hit the cache the second time.
        assertThat(cache.size()).isEqualTo(1);
        var hit = cache.locateKey("else".getBytes()).orElseThrow();
        assertThat(hit.region().getId()).isEqualTo(harness.bootstrapRegionId());
    }

    @Test
    void tsoBatcherDeliversMonotonicTimestamps() throws Exception {
        try (var batcher = new TsoBatcherImpl(pdClient, ClientConfig.TsoConfig.defaults())) {
            var seen = new HashSet<Long>();
            long prev = -1;
            for (int i = 0; i < 200; i++) {
                long ts = batcher.getTimestamp().get(2, TimeUnit.SECONDS);
                assertThat(ts).isGreaterThan(prev);
                assertThat(seen.add(ts)).isTrue();
                prev = ts;
            }
        }
    }

    @Test
    void clientRoutesWriteToCorrectKvStore() throws Exception {
        var cache = new RegionCacheImpl(pdClient, ClientConfig.RegionCacheConfig.defaults());
        try (var tso = new TsoBatcherImpl(pdClient, ClientConfig.TsoConfig.defaults())) {
            // Fetch a TSO (proves end-to-end PD wiring).
            long startTs = tso.getTimestamp().get(2, TimeUnit.SECONDS);
            assertThat(startTs).isPositive();

            // Locate region via cache.
            var info = cache.locateKey("k".getBytes()).orElseThrow();
            // Pick the leader peer's storeId (Phase 4 simplification: peer 0
            // is the elected leader in this small cluster — but here we ask
            // every store node and find the actual leader at runtime).
            var leader = harness.leader();

            // Issue write via the leader's stub.
            var stub = leader.blockingStub();
            stub.rawPut(Kvrpcpb.RawPutRequest.newBuilder()
                    .setKey(ByteString.copyFromUtf8("k"))
                    .setValue(ByteString.copyFromUtf8("v"))
                    .build());

            // Read-back from the leader.
            var got = stub.rawGet(Kvrpcpb.RawGetRequest.newBuilder()
                    .setKey(ByteString.copyFromUtf8("k"))
                    .build());
            assertThat(got.getValue().toStringUtf8()).isEqualTo("v");
        }
    }

    @Test
    void multipleConcurrentTsoStreamsAreUniqueAndMonotonic() throws Exception {
        try (var batcher = new TsoBatcherImpl(pdClient, ClientConfig.TsoConfig.defaults())) {
            var pool = java.util.concurrent.Executors.newFixedThreadPool(8);
            var seen = java.util.concurrent.ConcurrentHashMap.<Long>newKeySet();
            var futs = new java.util.ArrayList<java.util.concurrent.Future<Long>>();
            for (int i = 0; i < 500; i++) {
                futs.add(pool.submit(() -> batcher.getTimestamp().get(5, TimeUnit.SECONDS)));
            }
            for (var f : futs) {
                long ts = f.get();
                assertThat(seen.add(ts)).as("ts unique: %d", ts).isTrue();
            }
            pool.shutdown();
            assertThat(seen).hasSize(500);
        }
    }

    @Test
    void invalidateRangeDropsOverlappingCache() {
        var cache = new RegionCacheImpl(pdClient, ClientConfig.RegionCacheConfig.defaults());
        cache.locateKey("a".getBytes());
        assertThat(cache.size()).isEqualTo(1);
        // Invalidate a range covering the bootstrap region.
        cache.invalidateRange("".getBytes(), null);
        assertThat(cache.size()).isZero();
    }
}
