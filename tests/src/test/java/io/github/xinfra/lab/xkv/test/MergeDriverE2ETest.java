package io.github.xinfra.lab.xkv.test;

import io.github.xinfra.lab.xkv.kv.store.MergeDriver;
import io.github.xinfra.lab.xkv.kv.store.SplitDriver;
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
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Verifies region merge: split a region into two, then merge them back.
 *
 * <p>Workflow:
 * <ol>
 *   <li>Start with one region covering [-, -).</li>
 *   <li>Split at "m" → parent[-, m) + child[m, -).</li>
 *   <li>Merge child INTO parent. The merge proposal lands on the parent
 *       (the TARGET); the apply path persists parent's absorbed range and
 *       removes the child's descriptor. The observer destroys the child
 *       peer locally.</li>
 *   <li>After merge: parent covers [-, -) again; the child peer is gone
 *       from the local store.</li>
 * </ol>
 */
final class MergeDriverE2ETest {

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
    }

    @Test
    void splitThenMergeRestoresOriginalRange() throws Exception {
        var leader = harness.leader();

        // 1) Split at "m".
        var split = new SplitDriver(pd, /* proposeTimeoutMs= */ 5_000);
        var afterSplit = split.split(leader.peer, List.of("m".getBytes()));
        assertThat(afterSplit).hasSize(2);
        var parentAfterSplit = afterSplit.get(0);
        var child = afterSplit.get(1);

        // Wait for the child peer to materialize on this store.
        Awaitility.await().atMost(Duration.ofSeconds(5))
                .pollInterval(Duration.ofMillis(20))
                .until(() -> leader.childPeers.stream()
                        .anyMatch(c -> c.regionId() == child.getId()));
        var childPeer = leader.childPeers.stream()
                .filter(c -> c.regionId() == child.getId()).findFirst().orElseThrow();

        // 2) Wait for the child raft group to elect its own leader (the
        //    merge driver requires the target peer to be a leader; in our
        //    test the source peer doesn't need to be a leader since merge
        //    is target-proposed).
        // The child raft group may take a few election timeouts to settle.
        // Wait on ANY of the child peers to be leader (cross-store), then
        // use the right one as the merge source.
        Awaitility.await().atMost(Duration.ofSeconds(30))
                .pollInterval(Duration.ofMillis(100))
                .until(() -> harness.kvNodes().stream()
                        .flatMap(n -> n.childPeers.stream())
                        .anyMatch(c -> c.regionId() == child.getId() && c.isLeader()));
        var childLeader = harness.kvNodes().stream()
                .flatMap(n -> n.childPeers.stream())
                .filter(c -> c.regionId() == child.getId() && c.isLeader())
                .findFirst().orElseThrow();

        // 3) Merge child INTO parent. Parent is the merge target.
        var merge = new MergeDriver(/* proposeTimeoutMs= */ 5_000);
        var merged = merge.merge(/* source= */ childLeader, /* target= */ leader.peer);

        // Merged target's range covers parent's original [-, -).
        assertThat(merged.getStartKey().isEmpty()).isTrue();
        assertThat(merged.getEndKey().isEmpty()).isTrue();

        // Wait for the child peer to be destroyed on EVERY store (the
        // commit-merge entry replicates to all stores' parent raft groups).
        Awaitility.await().atMost(Duration.ofSeconds(10))
                .pollInterval(Duration.ofMillis(50))
                .until(() -> harness.kvNodes().stream()
                        .noneMatch(n -> n.store.peerForRegion(child.getId()).isPresent()));

        // Parent (the merged target) now routes the whole keyspace.
        assertThat(leader.store.peerForKey("alpha".getBytes()).orElseThrow().regionId())
                .isEqualTo(leader.peer.regionId());
        assertThat(leader.store.peerForKey("zulu".getBytes()).orElseThrow().regionId())
                .isEqualTo(leader.peer.regionId());
        // Child region descriptor gone from EVERY store.
        for (var n : harness.kvNodes()) {
            assertThat(n.store.peerForRegion(child.getId()))
                    .as("store %d still has child peer", n.peerId).isEmpty();
        }
    }
}
