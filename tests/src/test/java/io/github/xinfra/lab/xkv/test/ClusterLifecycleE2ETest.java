package io.github.xinfra.lab.xkv.test;

import com.google.protobuf.ByteString;
import io.github.xinfra.lab.xkv.client.TxnClient;
import io.github.xinfra.lab.xkv.client.config.ClientConfig;
import io.github.xinfra.lab.xkv.kv.store.MergeDriver;
import io.github.xinfra.lab.xkv.pd.config.PdConfig;
import io.github.xinfra.lab.xkv.pd.state.Operator;
import io.github.xinfra.lab.xkv.pd.state.OperatorSteps;
import io.github.xinfra.lab.xkv.pd.state.SimpleOperator;
import io.github.xinfra.lab.xkv.proto.Kvrpcpb;
import io.github.xinfra.lab.xkv.proto.Metapb;
import io.github.xinfra.lab.xkv.proto.Pdpb;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;
import java.time.Duration;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * Comprehensive cluster lifecycle E2E test covering:
 *
 * <ol>
 *   <li>Cluster initialization: start with 1 store, grow to 3</li>
 *   <li>Region replication via RegionBalanceScheduler</li>
 *   <li>Single-region CRUD (raw + transactional)</li>
 *   <li>Region split via PD operator → SplitDriver</li>
 *   <li>Post-split verification: both regions functional</li>
 *   <li>Region merge via MergeDriver</li>
 *   <li>Post-merge verification: merged region functional</li>
 * </ol>
 *
 * <p>All steps run in-process: 1 PD + 3 KV stores with real gRPC
 * and real raft transport. No mocks.
 */
@Timeout(value = 180, unit = TimeUnit.SECONDS)
final class ClusterLifecycleE2ETest {

    private static final Logger log = LoggerFactory.getLogger(ClusterLifecycleE2ETest.class);

    @TempDir Path baseDir;
    private ClusterHarness harness;
    private TxnClient txnClient;

    @AfterEach
    void teardown() {
        if (txnClient != null) {
            try { txnClient.close(); } catch (Throwable e) { e.printStackTrace(); }
        }
        if (harness != null) harness.close();
    }

    @Test
    void fullClusterLifecycle() throws Exception {
        // ====================================================================
        // Phase 1: Cluster Initialization — 1 store, then grow to 3
        // ====================================================================
        log.info("=== Phase 1: Cluster Initialization ===");

        harness = new ClusterHarness(baseDir, 1).start();
        long regionId = harness.bootstrapRegionId();

        // Wait for the single-node raft to elect itself leader.
        await().atMost(Duration.ofSeconds(15))
                .until(() -> harness.kvNodes().stream().anyMatch(n -> n.peer != null && n.peer.isLeader()));
        var leaderNode = harness.leader();
        log.info("single-store leader elected: store={}", leaderNode.peerId);

        // Verify single-store raw CRUD works immediately.
        var stub = leaderNode.blockingStub();
        stub.rawPut(Kvrpcpb.RawPutRequest.newBuilder()
                .setKey(bs("init-key")).setValue(bs("init-val")).build());
        var got = stub.rawGet(Kvrpcpb.RawGetRequest.newBuilder()
                .setKey(bs("init-key")).build());
        assertThat(got.getValue().toStringUtf8()).isEqualTo("init-val");
        log.info("single-store raw CRUD verified");

        // Add stores 2 and 3 dynamically.
        var store2 = harness.addStore(2);
        var store3 = harness.addStore(3);
        log.info("stores 2 and 3 added to PD");

        // Wait for RegionBalanceScheduler to dispatch AddPeer operators.
        // The scheduler runs on a timer; once it detects the imbalance
        // (1 store has 1 region, 2 stores have 0), it AddPeers to the
        // under-loaded stores.
        await().atMost(Duration.ofSeconds(90))
                .pollInterval(Duration.ofMillis(1000))
                .until(() -> {
                    var pdRegion = harness.pdServer().state().getRegion(regionId);
                    int peerCount = pdRegion.isPresent() ? pdRegion.get().getPeersCount() : 0;
                    boolean leaderAlive = leaderNode.peer != null && leaderNode.peer.isLeader();
                    var oc = harness.pdServer().operatorController();
                    var ops = oc.getOperators();
                    log.info("await replication: peers={} leader_alive={} ops_inflight={}{}",
                            peerCount, leaderAlive, ops.size(),
                            ops.isEmpty() ? "" : " [" + ops.iterator().next().desc() + "]");
                    return peerCount >= 3;
                });
        log.info("region {} replicated to 3 stores", regionId);

        // Verify all 3 stores host the region.
        var pdRegion = harness.pdServer().state().getRegion(regionId).orElseThrow();
        var storeIds = new HashSet<Long>();
        for (var p : pdRegion.getPeersList()) storeIds.add(p.getStoreId());
        assertThat(storeIds).containsExactlyInAnyOrder(1L, 2L, 3L);

        // Wait for a stable leader to emerge in the 3-peer group.
        await().atMost(Duration.ofSeconds(15))
                .pollInterval(Duration.ofMillis(200))
                .until(() -> {
                    for (var n : harness.kvNodes()) {
                        if (n.peer != null && n.peer.isLeader()) return true;
                        for (var cp : n.childPeers) {
                            if (cp.regionId() == regionId && cp.isLeader()) return true;
                        }
                    }
                    return false;
                });
        log.info("Phase 1 complete: 3-store cluster with replicated region");

        // ====================================================================
        // Phase 2: Single-Region Raft Cluster — Raw + Txn CRUD
        // ====================================================================
        log.info("=== Phase 2: Single-Region CRUD ===");

        // Find the current leader (may have changed after replication).
        var currentLeader = findLeaderStub(regionId);

        // Raw CRUD: write, read, delete
        int rawKeyCount = 20;
        for (int i = 0; i < rawKeyCount; i++) {
            currentLeader.rawPut(Kvrpcpb.RawPutRequest.newBuilder()
                    .setKey(bs("raw-" + String.format("%04d", i)))
                    .setValue(bs("value-" + i))
                    .build());
        }
        for (int i = 0; i < rawKeyCount; i++) {
            var resp = currentLeader.rawGet(Kvrpcpb.RawGetRequest.newBuilder()
                    .setKey(bs("raw-" + String.format("%04d", i)))
                    .build());
            assertThat(resp.getValue().toStringUtf8())
                    .as("raw key %d", i)
                    .isEqualTo("value-" + i);
        }
        // Delete odd keys
        for (int i = 1; i < rawKeyCount; i += 2) {
            currentLeader.rawDelete(Kvrpcpb.RawDeleteRequest.newBuilder()
                    .setKey(bs("raw-" + String.format("%04d", i)))
                    .build());
        }
        for (int i = 1; i < rawKeyCount; i += 2) {
            var resp = currentLeader.rawGet(Kvrpcpb.RawGetRequest.newBuilder()
                    .setKey(bs("raw-" + String.format("%04d", i)))
                    .build());
            assertThat(resp.getNotFound())
                    .as("deleted raw key %d should be not found", i)
                    .isTrue();
        }
        // Even keys still present
        for (int i = 0; i < rawKeyCount; i += 2) {
            var resp = currentLeader.rawGet(Kvrpcpb.RawGetRequest.newBuilder()
                    .setKey(bs("raw-" + String.format("%04d", i)))
                    .build());
            assertThat(resp.getValue().toStringUtf8())
                    .as("even raw key %d still present", i)
                    .isEqualTo("value-" + i);
        }
        log.info("raw CRUD verified: {} written, odds deleted, evens confirmed", rawKeyCount);

        // Txn CRUD via TxnClient
        txnClient = TxnClient.create(ClientConfig.builder()
                .pdEndpoints(List.of("127.0.0.1:" + harness.pdPort()))
                .build());

        // Write transaction
        try (var txn = txnClient.begin()) {
            for (int i = 0; i < 10; i++) {
                txn.put(("txn-key-" + i).getBytes(), ("txn-val-" + i).getBytes());
            }
            txn.commit();
        }
        log.info("txn write committed");

        // Read-only transaction: verify values
        try (var txn = txnClient.begin()) {
            for (int i = 0; i < 10; i++) {
                var val = txn.get(("txn-key-" + i).getBytes());
                assertThat(val).as("txn key %d present", i).isPresent();
                assertThat(new String(val.get()))
                        .as("txn key %d", i)
                        .isEqualTo("txn-val-" + i);
            }
            txn.rollback();
        }
        log.info("txn read-only verified");

        log.info("Phase 2 complete: raw + txn CRUD on single region");

        // ====================================================================
        // Phase 3: Region Split
        // ====================================================================
        log.info("=== Phase 3: Region Split ===");

        // Write enough data for a meaningful midpoint calculation.
        // Use a sorted key namespace so the midpoint splits meaningfully.
        currentLeader = findLeaderStub(regionId);
        byte[] largeVal = new byte[100];
        Arrays.fill(largeVal, (byte) 'x');
        int splitDataCount = 200;
        for (int i = 0; i < splitDataCount; i++) {
            currentLeader.rawPut(Kvrpcpb.RawPutRequest.newBuilder()
                    .setKey(bs("split-data-" + String.format("%04d", i)))
                    .setValue(ByteString.copyFrom(largeVal))
                    .build());
        }
        log.info("wrote {} keys for split", splitDataCount);

        // The bootstrap region has end_key="" so approximateSize=0 always.
        // Trigger split manually via PD's operator controller (same pattern
        // as SplitMergeClientE2ETest).
        scheduleApproximateSplit(regionId);

        // Wait for split to materialize.
        await().atMost(Duration.ofSeconds(30))
                .pollInterval(Duration.ofMillis(200))
                .until(() -> harness.kvNodes().stream()
                        .anyMatch(n -> !n.childPeers.isEmpty()));
        log.info("split detected — child peer(s) created");

        // Identify the child region.
        long childRegionId = harness.kvNodes().stream()
                .flatMap(n -> n.childPeers.stream())
                .mapToLong(p -> p.regionId())
                .findFirst().orElseThrow();

        // Wait for child region to elect a leader.
        await().atMost(Duration.ofSeconds(15))
                .pollInterval(Duration.ofMillis(200))
                .until(() -> harness.kvNodes().stream()
                        .flatMap(n -> n.childPeers.stream())
                        .anyMatch(c -> c.regionId() == childRegionId && c.isLeader()));
        log.info("child region {} has leader", childRegionId);

        // Wait for PD to know about the child region.
        await().atMost(Duration.ofSeconds(15))
                .pollInterval(Duration.ofMillis(200))
                .until(() -> harness.pdServer().state().getRegion(childRegionId).isPresent());

        // Verify region ranges are contiguous.
        var parentAfterSplit = harness.pdServer().state().getRegion(regionId).orElseThrow();
        var childRegion = harness.pdServer().state().getRegion(childRegionId).orElseThrow();
        assertThat(parentAfterSplit.getEndKey())
                .as("parent.end == child.start (contiguous)")
                .isEqualTo(childRegion.getStartKey());
        assertThat(parentAfterSplit.getStartKey().isEmpty())
                .as("parent starts at min key")
                .isTrue();
        byte[] splitPoint = parentAfterSplit.getEndKey().toByteArray();
        log.info("split verified: parent [{}, {}), child [{}, {})",
                parentAfterSplit.getStartKey().toStringUtf8(),
                parentAfterSplit.getEndKey().toStringUtf8(),
                childRegion.getStartKey().toStringUtf8(),
                childRegion.getEndKey().toStringUtf8());

        log.info("Phase 3 complete: region split at '{}'",
                new String(splitPoint));

        // ====================================================================
        // Phase 4: Post-Split Verification
        // ====================================================================
        log.info("=== Phase 4: Post-Split Verification ===");

        // Wait for parent leader + PD metadata convergence.
        await().atMost(Duration.ofSeconds(15))
                .pollInterval(Duration.ofMillis(200))
                .until(() -> harness.pdServer().state().getLeader(regionId).isPresent());
        await().atMost(Duration.ofSeconds(15))
                .pollInterval(Duration.ofMillis(200))
                .until(() -> harness.pdServer().state().getLeader(childRegionId).isPresent());

        // Determine which existing keys are in parent vs child.
        var parentLeaderStub = findLeaderStub(regionId);
        var childLeaderStub = findChildLeaderStub(childRegionId);

        // Verify raw reads: all split-data keys should be readable from
        // the appropriate region. Keys < splitPoint → parent, >= → child.
        int parentKeys = 0, childKeys = 0;
        for (int i = 0; i < splitDataCount; i++) {
            String keyStr = "split-data-" + String.format("%04d", i);
            byte[] keyBytes = keyStr.getBytes();
            if (compareKey(keyBytes, splitPoint) < 0) {
                var resp = parentLeaderStub.rawGet(Kvrpcpb.RawGetRequest.newBuilder()
                        .setKey(ByteString.copyFrom(keyBytes)).build());
                assertThat(resp.getNotFound())
                        .as("parent should have key %s", keyStr)
                        .isFalse();
                parentKeys++;
            } else {
                var resp = childLeaderStub.rawGet(Kvrpcpb.RawGetRequest.newBuilder()
                        .setKey(ByteString.copyFrom(keyBytes)).build());
                assertThat(resp.getNotFound())
                        .as("child should have key %s", keyStr)
                        .isFalse();
                childKeys++;
            }
        }
        assertThat(parentKeys + childKeys).isEqualTo(splitDataCount);
        assertThat(parentKeys).as("parent has some keys").isGreaterThan(0);
        assertThat(childKeys).as("child has some keys").isGreaterThan(0);
        log.info("post-split raw read: parent={} keys, child={} keys", parentKeys, childKeys);

        // Raw write + read in parent's range.
        byte[] parentTestKey = "aaa-parent-new".getBytes();
        assertThat(compareKey(parentTestKey, splitPoint)).isLessThan(0);
        parentLeaderStub.rawPut(Kvrpcpb.RawPutRequest.newBuilder()
                .setKey(ByteString.copyFrom(parentTestKey))
                .setValue(bs("parent-new-val")).build());
        var parentNewResp = parentLeaderStub.rawGet(Kvrpcpb.RawGetRequest.newBuilder()
                .setKey(ByteString.copyFrom(parentTestKey)).build());
        assertThat(parentNewResp.getValue().toStringUtf8()).isEqualTo("parent-new-val");

        // Raw write + read + delete in child's range.
        byte[] childTestKey = ("zzz-child-new").getBytes();
        childLeaderStub.rawPut(Kvrpcpb.RawPutRequest.newBuilder()
                .setKey(ByteString.copyFrom(childTestKey))
                .setValue(bs("child-new-val")).build());
        var childNewResp = childLeaderStub.rawGet(Kvrpcpb.RawGetRequest.newBuilder()
                .setKey(ByteString.copyFrom(childTestKey)).build());
        assertThat(childNewResp.getValue().toStringUtf8()).isEqualTo("child-new-val");
        childLeaderStub.rawDelete(Kvrpcpb.RawDeleteRequest.newBuilder()
                .setKey(ByteString.copyFrom(childTestKey)).build());
        var childDelResp = childLeaderStub.rawGet(Kvrpcpb.RawGetRequest.newBuilder()
                .setKey(ByteString.copyFrom(childTestKey)).build());
        assertThat(childDelResp.getNotFound()).isTrue();

        // Txn CRUD spanning both regions via TxnClient.
        try (var txn = txnClient.begin()) {
            txn.put("aaa-txn-parent".getBytes(), "txn-parent-val".getBytes());
            txn.put("zzz-txn-child".getBytes(), "txn-child-val".getBytes());
            txn.commit();
        }
        try (var txn = txnClient.begin()) {
            var v1 = txn.get("aaa-txn-parent".getBytes());
            assertThat(v1).isPresent();
            assertThat(new String(v1.get())).isEqualTo("txn-parent-val");
            var v2 = txn.get("zzz-txn-child".getBytes());
            assertThat(v2).isPresent();
            assertThat(new String(v2.get())).isEqualTo("txn-child-val");
            txn.rollback();
        }
        log.info("Phase 4 complete: post-split raw + txn CRUD verified");

        // ====================================================================
        // Phase 5: Region Merge
        // ====================================================================
        log.info("=== Phase 5: Region Merge ===");

        // Find region leaders for merge. MergeDriver needs RegionPeer refs.
        var childLeaderPeer = harness.kvNodes().stream()
                .flatMap(n -> n.childPeers.stream())
                .filter(c -> c.regionId() == childRegionId && c.isLeader())
                .findFirst().orElseThrow();

        // Parent leader peer.
        io.github.xinfra.lab.xkv.kv.raft.RegionPeerImpl parentLeaderPeer = null;
        for (var n : harness.kvNodes()) {
            if (n.peer != null && n.peer.regionId() == regionId && n.peer.isLeader()) {
                parentLeaderPeer = n.peer;
                break;
            }
        }
        assertThat(parentLeaderPeer).as("parent leader peer").isNotNull();

        // Execute merge: child INTO parent.
        var mergeDriver = new MergeDriver(10_000);
        var mergedRegion = mergeDriver.merge(childLeaderPeer, parentLeaderPeer);
        log.info("merge complete: child {} → parent {}", childRegionId, regionId);

        // Verify merged region's range covers the full keyspace.
        assertThat(mergedRegion.getStartKey().isEmpty())
                .as("merged region starts at min key")
                .isTrue();
        assertThat(mergedRegion.getEndKey().isEmpty())
                .as("merged region ends at max key (infinity)")
                .isTrue();

        // Wait for child peer to be destroyed on all stores.
        await().atMost(Duration.ofSeconds(15))
                .pollInterval(Duration.ofMillis(100))
                .until(() -> harness.kvNodes().stream()
                        .noneMatch(n -> n.store.peerForRegion(childRegionId).isPresent()));
        log.info("child peer destroyed on all stores");

        // Wait for PD to reflect the merged region.
        await().atMost(Duration.ofSeconds(15))
                .pollInterval(Duration.ofMillis(200))
                .until(() -> {
                    var pr = harness.pdServer().state().getRegion(regionId);
                    return pr.isPresent()
                            && pr.get().getStartKey().isEmpty()
                            && pr.get().getEndKey().isEmpty()
                            && harness.pdServer().state().getLeader(regionId).isPresent();
                });
        log.info("PD metadata reflects merged region");

        log.info("Phase 5 complete: region merge done");

        // ====================================================================
        // Phase 6: Post-Merge Verification
        // ====================================================================
        log.info("=== Phase 6: Post-Merge Verification ===");

        // Wait for the merged region leader to stabilize.
        await().atMost(Duration.ofSeconds(10))
                .pollInterval(Duration.ofMillis(200))
                .until(() -> {
                    for (var n : harness.kvNodes()) {
                        if (n.peer != null && n.peer.regionId() == regionId && n.peer.isLeader()) {
                            return true;
                        }
                    }
                    return false;
                });

        var mergedStub = findLeaderStub(regionId);

        // Read all split-data keys — they should ALL be readable from
        // the merged region regardless of which side of the split they
        // were on.
        for (int i = 0; i < splitDataCount; i++) {
            String keyStr = "split-data-" + String.format("%04d", i);
            var resp = mergedStub.rawGet(Kvrpcpb.RawGetRequest.newBuilder()
                    .setKey(bs(keyStr)).build());
            assertThat(resp.getNotFound())
                    .as("merged region should have key %s", keyStr)
                    .isFalse();
        }
        log.info("all {} split-data keys readable in merged region", splitDataCount);

        // Raw write + read + delete in merged region.
        mergedStub.rawPut(Kvrpcpb.RawPutRequest.newBuilder()
                .setKey(bs("merged-new-key"))
                .setValue(bs("merged-val")).build());
        var mergedGet = mergedStub.rawGet(Kvrpcpb.RawGetRequest.newBuilder()
                .setKey(bs("merged-new-key")).build());
        assertThat(mergedGet.getValue().toStringUtf8()).isEqualTo("merged-val");
        mergedStub.rawDelete(Kvrpcpb.RawDeleteRequest.newBuilder()
                .setKey(bs("merged-new-key")).build());
        var mergedDel = mergedStub.rawGet(Kvrpcpb.RawGetRequest.newBuilder()
                .setKey(bs("merged-new-key")).build());
        assertThat(mergedDel.getNotFound()).isTrue();

        // Txn CRUD on merged region — keys that span the old split boundary.
        try (var txn = txnClient.begin()) {
            txn.put("aaa-post-merge".getBytes(), "pm-val-a".getBytes());
            txn.put("zzz-post-merge".getBytes(), "pm-val-z".getBytes());
            txn.commit();
        }
        try (var txn = txnClient.begin()) {
            var va = txn.get("aaa-post-merge".getBytes());
            assertThat(va).isPresent();
            assertThat(new String(va.get())).isEqualTo("pm-val-a");
            var vz = txn.get("zzz-post-merge".getBytes());
            assertThat(vz).isPresent();
            assertThat(new String(vz.get())).isEqualTo("pm-val-z");
            txn.rollback();
        }

        // Verify the earlier txn keys (written pre-split) are still accessible.
        try (var txn = txnClient.begin()) {
            for (int i = 0; i < 10; i++) {
                var val = txn.get(("txn-key-" + i).getBytes());
                assertThat(val).as("pre-split txn key %d present", i).isPresent();
                assertThat(new String(val.get()))
                        .as("pre-split txn key %d still readable after merge", i)
                        .isEqualTo("txn-val-" + i);
            }
            txn.rollback();
        }

        log.info("Phase 6 complete: post-merge raw + txn CRUD verified");
        log.info("=== Full cluster lifecycle test PASSED ===");
    }

    // ---- Helpers ----

    private io.github.xinfra.lab.xkv.proto.TikvGrpc.TikvBlockingStub findLeaderStub(long regionId) {
        for (var n : harness.kvNodes()) {
            if (n.peer != null && n.peer.regionId() == regionId && n.peer.isLeader()) {
                return n.blockingStub();
            }
        }
        throw new IllegalStateException("no leader found for region " + regionId);
    }

    private io.github.xinfra.lab.xkv.proto.TikvGrpc.TikvBlockingStub findChildLeaderStub(long childRegionId) {
        for (var n : harness.kvNodes()) {
            for (var cp : n.childPeers) {
                if (cp.regionId() == childRegionId && cp.isLeader()) {
                    return n.blockingStub();
                }
            }
        }
        throw new IllegalStateException("no child leader found for region " + childRegionId);
    }

    private void scheduleApproximateSplit(long regionId) {
        var region = harness.pdServer().state().getRegion(regionId).orElseThrow();
        long currentVersion = region.getRegionEpoch().getVersion();
        var storeIdSet = new HashSet<Long>();
        for (var p : region.getPeersList()) storeIdSet.add(p.getStoreId());
        var sr = Pdpb.SplitRegion.newBuilder()
                .setPolicy(Pdpb.SplitRegion.Policy.APPROXIMATE).build();
        var resp = Pdpb.RegionHeartbeatResponse.newBuilder()
                .setRegionId(regionId).setSplitRegion(sr).build();
        var splitOp = new SimpleOperator(System.nanoTime(), regionId, Operator.Kind.SPLIT,
                "test: approximate split", resp, storeIdSet,
                List.of(new OperatorSteps.SplitRegionStep(currentVersion + 1)),
                Operator.PRIORITY_ADMIN);
        harness.pdServer().operatorController().addOperator(splitOp);
    }

    private static int compareKey(byte[] a, byte[] b) {
        return Arrays.compareUnsigned(a, b);
    }

    private static ByteString bs(String s) {
        return ByteString.copyFromUtf8(s);
    }
}
