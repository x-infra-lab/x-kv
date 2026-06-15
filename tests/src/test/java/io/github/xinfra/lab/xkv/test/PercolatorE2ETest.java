package io.github.xinfra.lab.xkv.test;

import com.google.protobuf.ByteString;
import io.github.xinfra.lab.raft.Peer;
import io.github.xinfra.lab.xkv.kv.config.KvConfig;
import io.github.xinfra.lab.xkv.kv.engine.PerRegionRaftEngine;
import io.github.xinfra.lab.xkv.kv.engine.RocksStorageEngine;
import io.github.xinfra.lab.xkv.kv.raft.CompositeApplyHandler;
import io.github.xinfra.lab.xkv.kv.raft.LoopbackTransport;
import io.github.xinfra.lab.xkv.kv.raft.RegionPeer;
import io.github.xinfra.lab.xkv.kv.raft.RegionPeerImpl;
import io.github.xinfra.lab.xkv.kv.server.RawKvService;
import io.github.xinfra.lab.xkv.kv.server.TikvServiceImpl;
import io.github.xinfra.lab.xkv.kv.server.TransactionService;
import io.github.xinfra.lab.xkv.proto.Kvrpcpb;
import io.github.xinfra.lab.xkv.proto.Metapb;
import io.github.xinfra.lab.xkv.proto.TikvGrpc;
import io.grpc.ManagedChannel;
import io.grpc.Server;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Phase 2 end-to-end verification: Percolator 2PC over gRPC.
 *
 * <p>Each test issues real {@code KvPrewrite / KvCommit / KvBatchRollback}
 * RPCs against an in-process KV server backed by a single-peer Raft group.
 * Validates that the v1 critical bugs cannot recur:
 * <ul>
 *   <li>Multi-mutation prewrite is all-or-nothing (no orphan locks)</li>
 *   <li>commit_ts &lt;= start_ts is rejected</li>
 *   <li>commit_ts &lt; min_commit_ts is rejected (async-commit invariant)</li>
 *   <li>Snapshot read sees pre-commit values, blocked-on-lock for in-flight</li>
 * </ul>
 */
final class PercolatorE2ETest {

    @TempDir Path dataDir;
    private RocksStorageEngine engine;
    private RegionPeerImpl peer;
    private Server grpcServer;
    private ManagedChannel channel;
    private TikvGrpc.TikvBlockingStub tikv;

    @BeforeEach
    void start() throws Exception {
        engine = RocksStorageEngine.open(dataDir, KvConfig.EngineConfig.defaults());
        var raftEngine = new PerRegionRaftEngine(engine, 1);
        var region = Metapb.Region.newBuilder()
                .setId(1)
                .setRegionEpoch(Metapb.RegionEpoch.newBuilder().setConfVer(1).setVersion(1))
                .addPeers(Metapb.Peer.newBuilder().setId(1).setStoreId(1).setRole(Metapb.PeerRole.Voter))
                .build();
        // Production-shape wiring: one ConcurrencyManager (and its
        // MaxTsTracker) is shared by the TransactionService reader path and
        // the apply-side prewrite path. Without sharing, apply doesn't see
        // reader-observed max_ts and the SI invariant breaks. The tracker is
        // bootstrapped from any persisted floor (cold start = 0).
        var cm = new io.github.xinfra.lab.xkv.kv.mvcc.ConcurrencyManager(
                new io.github.xinfra.lab.xkv.kv.mvcc.MaxTsTracker(raftEngine.persistedMaxTs()));
        peer = new RegionPeerImpl(
                engine, raftEngine, region, region.getPeers(0),
                List.of(new Peer(1)),
                new LoopbackTransport(),
                CompositeApplyHandler.defaultFor(engine, cm).withAdmin(raftEngine),
                new RegionPeerImpl.Settings(10, 1, 30),
                cm);
        Awaitility.await().atMost(Duration.ofSeconds(10)).until(peer::isLeader);

        var rawKv = new RawKvService(engine, key -> peer, 5_000);
        var txn = new TransactionService(engine, key -> peer, 5_000, cm);

        var name = "test-" + UUID.randomUUID();
        grpcServer = InProcessServerBuilder.forName(name)
                .directExecutor()
                .addService(new TikvServiceImpl(rawKv, txn))
                .build()
                .start();
        channel = InProcessChannelBuilder.forName(name).directExecutor().build();
        tikv = TikvGrpc.newBlockingStub(channel);
    }

    @AfterEach
    void teardown() {
        if (channel != null) channel.shutdownNow();
        if (grpcServer != null) {
            grpcServer.shutdownNow();
            try { grpcServer.awaitTermination(2, TimeUnit.SECONDS); } catch (InterruptedException ignored) { Thread.currentThread().interrupt(); }
        }
        if (peer != null) peer.shutdown();
        if (engine != null) engine.close();
    }

    // =====================================================================

    @Test
    void backofferExternalDeadlineShortensBudget() {
        var cfg = io.github.xinfra.lab.xkv.client.config.ClientConfig.BackoffConfig.defaults();
        // External deadline = 100ms (much shorter than cfg.maxOverallElapsedMs).
        var external = java.time.Instant.now().plusMillis(100);
        var bo = new io.github.xinfra.lab.xkv.client.backoff.BackofferImpl(cfg, external);
        long t0 = System.nanoTime();
        try {
            // Loop until budget is exhausted.
            while (true) {
                bo.backoff(io.github.xinfra.lab.xkv.client.backoff.Backoffer.Reason.NOT_LEADER, "drain");
            }
        } catch (io.github.xinfra.lab.xkv.client.backoff.Backoffer.BackoffExceededException expected) {
            long elapsedMs = (System.nanoTime() - t0) / 1_000_000;
            // Should give up promptly — within the 100ms deadline plus a sleep tail.
            assertThat(elapsedMs).isLessThan(500);
        }
    }

    @Test
    void proposeHonorsClientGrpcDeadline() throws Exception {
        // A peer that NEVER completes its propose — proves the propose path
        // would otherwise wait the full proposeTimeoutMs (5s here). With the
        // gRPC Context deadline plumbed, propose returns within the
        // deadline budget instead.
        var hangingPeer = new io.github.xinfra.lab.xkv.kv.raft.RegionPeer() {
            @Override public long regionId() { return 1; }
            @Override public io.github.xinfra.lab.xkv.proto.Metapb.Region region() {
                return io.github.xinfra.lab.xkv.proto.Metapb.Region.newBuilder().setId(1).build();
            }
            @Override public io.github.xinfra.lab.xkv.proto.Metapb.Peer self() {
                return io.github.xinfra.lab.xkv.proto.Metapb.Peer.newBuilder().setId(1).setStoreId(1).build();
            }
            @Override public boolean isLeader() { return true; }
            @Override public boolean isDestroyed() { return false; }
            @Override public long firstIndex() { return 0; }
            @Override public long appliedIndex() { return 0; }
            @Override public java.util.concurrent.CompletableFuture<ApplyResult> propose(Proposal p) {
                return new java.util.concurrent.CompletableFuture<>();  // never completes
            }
            @Override public java.util.concurrent.CompletableFuture<ApplyResult> proposeAdmin(AdminProposal p) {
                return new java.util.concurrent.CompletableFuture<>();
            }
            @Override public java.util.concurrent.CompletableFuture<Void> readIndex() {
                return java.util.concurrent.CompletableFuture.completedFuture(null);
            }
            @Override public void transferLeader(long t) {}
            @Override public void updateRegion(io.github.xinfra.lab.xkv.proto.Metapb.Region r) {}
            @Override public void maybeGenerateSnapshot() {}
            @Override public java.util.concurrent.CompletableFuture<ApplyResult> proposeConfChange(io.github.xinfra.lab.raft.proto.Eraftpb.ConfChangeV2 cc) { return java.util.concurrent.CompletableFuture.completedFuture(ApplyResult.err("unused")); }
            @Override public void shutdown() {}
        };
        var cm = new io.github.xinfra.lab.xkv.kv.mvcc.ConcurrencyManager(
                new io.github.xinfra.lab.xkv.kv.mvcc.MaxTsTracker());
        // Server default propose timeout = 60s — much longer than the
        // client deadline we'll plumb in below.
        var txn = new io.github.xinfra.lab.xkv.kv.server.TransactionService(
                engine, key -> hangingPeer, /* proposeTimeoutMs= */ 60_000, cm);

        // Synthesize a gRPC Context with a 200ms deadline and run the
        // propose inside it.
        var deadline = io.grpc.Deadline.after(200, TimeUnit.MILLISECONDS);
        var deadlineExecutor = java.util.concurrent.Executors.newSingleThreadScheduledExecutor();
        long t0 = System.nanoTime();
        var resp = io.grpc.Context.current().withDeadline(deadline, deadlineExecutor).call(() ->
                txn.kvPrewrite(Kvrpcpb.PrewriteRequest.newBuilder()
                        .setStartVersion(100)
                        .setLockTtl(3000)
                        .setPrimaryLock(ByteString.copyFromUtf8("dl"))
                        .addMutations(Kvrpcpb.Mutation.newBuilder()
                                .setOp(Kvrpcpb.Op.Put)
                                .setKey(ByteString.copyFromUtf8("dl"))
                                .setValue(ByteString.copyFromUtf8("v")))
                        .build()));
        long elapsedMs = (System.nanoTime() - t0) / 1_000_000;
        deadlineExecutor.shutdownNow();

        // Must have failed (timeout), not waited the full 60s.
        assertThat(elapsedMs)
                .as("propose respected the client gRPC deadline")
                .isLessThan(2_000);
        assertThat(resp.getErrorsCount()).isPositive();
    }

    @Test
    void kvGetOnNonLeaderReturnsTypedRegionError() {
        var notLeaderPeer = new io.github.xinfra.lab.xkv.kv.raft.RegionPeer() {
            @Override public long regionId() { return 1; }
            @Override public io.github.xinfra.lab.xkv.proto.Metapb.Region region() {
                return io.github.xinfra.lab.xkv.proto.Metapb.Region.newBuilder().setId(1).build();
            }
            @Override public io.github.xinfra.lab.xkv.proto.Metapb.Peer self() {
                return io.github.xinfra.lab.xkv.proto.Metapb.Peer.newBuilder().setId(7).setStoreId(7).build();
            }
            @Override public boolean isLeader() { return false; }
            @Override public boolean isDestroyed() { return false; }
            @Override public long firstIndex() { return 0; }
            @Override public long appliedIndex() { return 0; }
            @Override public java.util.concurrent.CompletableFuture<ApplyResult> propose(Proposal p) {
                return java.util.concurrent.CompletableFuture.completedFuture(ApplyResult.err("unused"));
            }
            @Override public java.util.concurrent.CompletableFuture<ApplyResult> proposeAdmin(AdminProposal p) {
                return java.util.concurrent.CompletableFuture.completedFuture(ApplyResult.err("unused"));
            }
            @Override public java.util.concurrent.CompletableFuture<Void> readIndex() {
                return java.util.concurrent.CompletableFuture.completedFuture(null);
            }
            @Override public void shutdown() {}
            @Override public void transferLeader(long t) {}
            @Override public void updateRegion(io.github.xinfra.lab.xkv.proto.Metapb.Region r) {}
            @Override public void maybeGenerateSnapshot() {}
            @Override public java.util.concurrent.CompletableFuture<ApplyResult> proposeConfChange(io.github.xinfra.lab.raft.proto.Eraftpb.ConfChangeV2 cc) { return java.util.concurrent.CompletableFuture.completedFuture(ApplyResult.err("unused")); }
        };
        var cm = new io.github.xinfra.lab.xkv.kv.mvcc.ConcurrencyManager(
                new io.github.xinfra.lab.xkv.kv.mvcc.MaxTsTracker());
        var txn = new io.github.xinfra.lab.xkv.kv.server.TransactionService(
                engine, key -> notLeaderPeer, 5_000, cm);
        var resp = txn.kvGet(Kvrpcpb.GetRequest.newBuilder()
                .setKey(ByteString.copyFromUtf8("anything"))
                .setVersion(100)
                .build());
        assertThat(resp.hasRegionError()).isTrue();
        assertThat(resp.getRegionError().hasNotLeader()).isTrue();
        // Importantly: did NOT mistakenly return data (NotFound or otherwise).
        assertThat(resp.getNotFound()).isFalse();
    }

    @Test
    void prewriteOnNonLeaderReturnsTypedRegionError() {
        // Shut down the only peer's leadership by faking peerLocator that
        // returns a non-leader peer. Simulated here via a TxnService bound
        // to a peer that is deliberately not the elected leader. In the
        // single-peer setup we use a lambda locator that fakes
        // isLeader=false to exercise the path.
        var notLeaderPeer = new io.github.xinfra.lab.xkv.kv.raft.RegionPeer() {
            @Override public long regionId() { return 1; }
            @Override public io.github.xinfra.lab.xkv.proto.Metapb.Region region() {
                return io.github.xinfra.lab.xkv.proto.Metapb.Region.newBuilder().setId(1).build();
            }
            @Override public io.github.xinfra.lab.xkv.proto.Metapb.Peer self() {
                return io.github.xinfra.lab.xkv.proto.Metapb.Peer.newBuilder().setId(7).setStoreId(7).build();
            }
            @Override public boolean isLeader() { return false; }
            @Override public boolean isDestroyed() { return false; }
            @Override public long firstIndex() { return 0; }
            @Override public long appliedIndex() { return 0; }
            @Override public java.util.concurrent.CompletableFuture<ApplyResult> propose(Proposal p) {
                return java.util.concurrent.CompletableFuture.completedFuture(ApplyResult.err("unused"));
            }
            @Override public java.util.concurrent.CompletableFuture<ApplyResult> proposeAdmin(AdminProposal p) {
                return java.util.concurrent.CompletableFuture.completedFuture(ApplyResult.err("unused"));
            }
            @Override public java.util.concurrent.CompletableFuture<Void> readIndex() {
                return java.util.concurrent.CompletableFuture.completedFuture(null);
            }
            @Override public void shutdown() {}
            @Override public void transferLeader(long t) {}
            @Override public void updateRegion(io.github.xinfra.lab.xkv.proto.Metapb.Region r) {}
            @Override public void maybeGenerateSnapshot() {}
            @Override public java.util.concurrent.CompletableFuture<ApplyResult> proposeConfChange(io.github.xinfra.lab.raft.proto.Eraftpb.ConfChangeV2 cc) { return java.util.concurrent.CompletableFuture.completedFuture(ApplyResult.err("unused")); }
        };
        var cm = new io.github.xinfra.lab.xkv.kv.mvcc.ConcurrencyManager(
                new io.github.xinfra.lab.xkv.kv.mvcc.MaxTsTracker());
        var txn = new io.github.xinfra.lab.xkv.kv.server.TransactionService(
                engine, key -> notLeaderPeer, 5_000, cm);
        var resp = txn.kvPrewrite(Kvrpcpb.PrewriteRequest.newBuilder()
                .setStartVersion(100)
                .setLockTtl(3000)
                .setPrimaryLock(ByteString.copyFromUtf8("nl-pri"))
                .addMutations(Kvrpcpb.Mutation.newBuilder()
                        .setOp(Kvrpcpb.Op.Put)
                        .setKey(ByteString.copyFromUtf8("nl-pri"))
                        .setValue(ByteString.copyFromUtf8("v")))
                .build());
        // Typed RegionError, NOT a KeyError.abort. The client's
        // RegionRequestSender pivots on this and refreshes leader/cache.
        assertThat(resp.hasRegionError()).isTrue();
        assertThat(resp.getRegionError().hasNotLeader()).isTrue();
        assertThat(resp.getErrorsCount()).isZero();
    }

    @Test
    void simpleTwoPhaseCommit() {
        long startTs = 100, commitTs = 150;
        var prewrite = tikv.kvPrewrite(Kvrpcpb.PrewriteRequest.newBuilder()
                .setStartVersion(startTs)
                .setLockTtl(3000)
                .setPrimaryLock(ByteString.copyFromUtf8("alice"))
                .addMutations(Kvrpcpb.Mutation.newBuilder()
                        .setOp(Kvrpcpb.Op.Put)
                        .setKey(ByteString.copyFromUtf8("alice"))
                        .setValue(ByteString.copyFromUtf8("100")))
                .build());
        assertThat(prewrite.getErrorsCount()).isZero();

        var commit = tikv.kvCommit(Kvrpcpb.CommitRequest.newBuilder()
                .setStartVersion(startTs)
                .setCommitVersion(commitTs)
                .addKeys(ByteString.copyFromUtf8("alice"))
                .build());
        assertThat(commit.hasError()).isFalse();

        var get = tikv.kvGet(Kvrpcpb.GetRequest.newBuilder()
                .setKey(ByteString.copyFromUtf8("alice"))
                .setVersion(200)
                .build());
        assertThat(get.getNotFound()).isFalse();
        assertThat(get.getValue().toStringUtf8()).isEqualTo("100");
    }

    @Test
    void readBlockedByLockReturnsKeyError() {
        var prewrite = tikv.kvPrewrite(Kvrpcpb.PrewriteRequest.newBuilder()
                .setStartVersion(100)
                .setLockTtl(3000)
                .setPrimaryLock(ByteString.copyFromUtf8("k"))
                .addMutations(Kvrpcpb.Mutation.newBuilder()
                        .setOp(Kvrpcpb.Op.Put)
                        .setKey(ByteString.copyFromUtf8("k"))
                        .setValue(ByteString.copyFromUtf8("v")))
                .build());
        assertThat(prewrite.getErrorsCount()).isZero();

        // Lock present at startTs=100; reading at readTs=200 must surface
        // a Locked KeyError (caller's lock resolver handles it).
        var get = tikv.kvGet(Kvrpcpb.GetRequest.newBuilder()
                .setKey(ByteString.copyFromUtf8("k"))
                .setVersion(200)
                .build());
        assertThat(get.hasError()).isTrue();
        assertThat(get.getError().hasLocked()).isTrue();
        assertThat(get.getError().getLocked().getLockVersion()).isEqualTo(100);
    }

    @Test
    void rollbackCleansLockAndPreventsCommit() {
        tikv.kvPrewrite(Kvrpcpb.PrewriteRequest.newBuilder()
                .setStartVersion(100)
                .setLockTtl(3000)
                .setPrimaryLock(ByteString.copyFromUtf8("k"))
                .addMutations(Kvrpcpb.Mutation.newBuilder()
                        .setOp(Kvrpcpb.Op.Put)
                        .setKey(ByteString.copyFromUtf8("k"))
                        .setValue(ByteString.copyFromUtf8("v")))
                .build());

        var rb = tikv.kvBatchRollback(Kvrpcpb.BatchRollbackRequest.newBuilder()
                .setStartVersion(100)
                .addKeys(ByteString.copyFromUtf8("k"))
                .build());
        assertThat(rb.hasError()).isFalse();

        // Subsequent commit on a rolled-back txn must return an error.
        var commit = tikv.kvCommit(Kvrpcpb.CommitRequest.newBuilder()
                .setStartVersion(100)
                .setCommitVersion(150)
                .addKeys(ByteString.copyFromUtf8("k"))
                .build());
        assertThat(commit.hasError()).isTrue();
    }

    @Test
    void multiMutationAtomicityOnConflict() {
        // First txn commits at ts=200 on key "b".
        commitOnce("b", "first", 100, 200);

        // Second txn tries to prewrite both "a" (free) and "b" (conflict).
        // Expectation: errors are reported; NEITHER lock survives
        // (all-or-nothing prewrite, the v1 fix).
        var prewrite = tikv.kvPrewrite(Kvrpcpb.PrewriteRequest.newBuilder()
                .setStartVersion(150)
                .setLockTtl(3000)
                .setPrimaryLock(ByteString.copyFromUtf8("a"))
                .addMutations(Kvrpcpb.Mutation.newBuilder()
                        .setOp(Kvrpcpb.Op.Put)
                        .setKey(ByteString.copyFromUtf8("a"))
                        .setValue(ByteString.copyFromUtf8("a-val")))
                .addMutations(Kvrpcpb.Mutation.newBuilder()
                        .setOp(Kvrpcpb.Op.Put)
                        .setKey(ByteString.copyFromUtf8("b"))
                        .setValue(ByteString.copyFromUtf8("b-val")))
                .build());
        assertThat(prewrite.getErrorsCount()).isPositive();

        // Reading "a" at ts=250 must NOT see a lock (it would block the
        // read needlessly). The all-or-nothing rule means the apply layer
        // wrote no locks at all.
        var getA = tikv.kvGet(Kvrpcpb.GetRequest.newBuilder()
                .setKey(ByteString.copyFromUtf8("a"))
                .setVersion(250)
                .build());
        assertThat(getA.hasError()).as("no orphan lock on 'a'").isFalse();
        assertThat(getA.getNotFound()).isTrue();
    }

    @Test
    void commitWithCommitTsLessOrEqualStartTsRejected() {
        tikv.kvPrewrite(Kvrpcpb.PrewriteRequest.newBuilder()
                .setStartVersion(100)
                .setLockTtl(3000)
                .setPrimaryLock(ByteString.copyFromUtf8("k"))
                .addMutations(Kvrpcpb.Mutation.newBuilder()
                        .setOp(Kvrpcpb.Op.Put)
                        .setKey(ByteString.copyFromUtf8("k"))
                        .setValue(ByteString.copyFromUtf8("v")))
                .build());
        var commit = tikv.kvCommit(Kvrpcpb.CommitRequest.newBuilder()
                .setStartVersion(100)
                .setCommitVersion(100)
                .addKeys(ByteString.copyFromUtf8("k"))
                .build());
        assertThat(commit.hasError()).isTrue();
        assertThat(commit.getError().hasCommitTsExpired()).isTrue();
    }

    @Test
    void scanReturnsCommittedPairs() {
        // 4 keys committed at unique startTs to avoid write conflicts.
        commitOnce("a", "va", 10, 20);
        commitOnce("b", "vb", 30, 40);
        commitOnce("c", "vc", 50, 60);
        commitOnce("d", "vd", 70, 80);

        var resp = tikv.kvScan(Kvrpcpb.ScanRequest.newBuilder()
                .setStartKey(ByteString.copyFromUtf8("b"))
                .setEndKey(ByteString.copyFromUtf8("d"))
                .setLimit(100)
                .setVersion(100)
                .build());
        assertThat(resp.getPairsCount()).isEqualTo(2);
        assertThat(resp.getPairs(0).getKey().toStringUtf8()).isEqualTo("b");
        assertThat(resp.getPairs(1).getKey().toStringUtf8()).isEqualTo("c");
    }

    @Test
    void scanLockReturnsActiveLocks() {
        tikv.kvPrewrite(Kvrpcpb.PrewriteRequest.newBuilder()
                .setStartVersion(100)
                .setLockTtl(3000)
                .setPrimaryLock(ByteString.copyFromUtf8("k1"))
                .addMutations(Kvrpcpb.Mutation.newBuilder()
                        .setOp(Kvrpcpb.Op.Put)
                        .setKey(ByteString.copyFromUtf8("k1"))
                        .setValue(ByteString.copyFromUtf8("v")))
                .addMutations(Kvrpcpb.Mutation.newBuilder()
                        .setOp(Kvrpcpb.Op.Put)
                        .setKey(ByteString.copyFromUtf8("k2"))
                        .setValue(ByteString.copyFromUtf8("v")))
                .build());

        var resp = tikv.kvScanLock(Kvrpcpb.ScanLockRequest.newBuilder()
                .setMaxVersion(200)
                .setLimit(100)
                .build());
        assertThat(resp.getLocksCount()).isEqualTo(2);
    }

    @Test
    void crashRecoveryPreservesCommittedTxns() throws Exception {
        commitOnce("k1", "v1", 10, 20);
        commitOnce("k2", "v2", 30, 40);

        // Hard restart.
        teardown();
        start();

        var get1 = tikv.kvGet(Kvrpcpb.GetRequest.newBuilder()
                .setKey(ByteString.copyFromUtf8("k1")).setVersion(100).build());
        var get2 = tikv.kvGet(Kvrpcpb.GetRequest.newBuilder()
                .setKey(ByteString.copyFromUtf8("k2")).setVersion(100).build());
        assertThat(get1.getValue().toStringUtf8()).isEqualTo("v1");
        assertThat(get2.getValue().toStringUtf8()).isEqualTo("v2");
    }

    @Test
    void commitIsIdempotent() {
        long startTs = 100, commitTs = 150;
        tikv.kvPrewrite(Kvrpcpb.PrewriteRequest.newBuilder()
                .setStartVersion(startTs)
                .setLockTtl(3000)
                .setPrimaryLock(ByteString.copyFromUtf8("k"))
                .addMutations(Kvrpcpb.Mutation.newBuilder()
                        .setOp(Kvrpcpb.Op.Put)
                        .setKey(ByteString.copyFromUtf8("k"))
                        .setValue(ByteString.copyFromUtf8("v")))
                .build());
        var c1 = tikv.kvCommit(Kvrpcpb.CommitRequest.newBuilder()
                .setStartVersion(startTs).setCommitVersion(commitTs)
                .addKeys(ByteString.copyFromUtf8("k")).build());
        var c2 = tikv.kvCommit(Kvrpcpb.CommitRequest.newBuilder()
                .setStartVersion(startTs).setCommitVersion(commitTs)
                .addKeys(ByteString.copyFromUtf8("k")).build());
        assertThat(c1.hasError()).isFalse();
        assertThat(c2.hasError()).isFalse();
    }

    @Test
    void txnHeartBeatExtendsLockTtl() {
        // Prewrite with TTL=3000ms, then heartbeat with advise=10000.
        long startTs = 100;
        var prewrite = tikv.kvPrewrite(Kvrpcpb.PrewriteRequest.newBuilder()
                .setStartVersion(startTs)
                .setLockTtl(3000)
                .setPrimaryLock(ByteString.copyFromUtf8("hb-primary"))
                .addMutations(Kvrpcpb.Mutation.newBuilder()
                        .setOp(Kvrpcpb.Op.Put)
                        .setKey(ByteString.copyFromUtf8("hb-primary"))
                        .setValue(ByteString.copyFromUtf8("v")))
                .build());
        assertThat(prewrite.getErrorsCount()).isZero();

        // Heartbeat with bigger TTL — must take the max.
        var hb1 = tikv.kvTxnHeartBeat(Kvrpcpb.TxnHeartBeatRequest.newBuilder()
                .setPrimaryLock(ByteString.copyFromUtf8("hb-primary"))
                .setStartVersion(startTs)
                .setAdviseLockTtl(10_000)
                .build());
        assertThat(hb1.hasError()).isFalse();
        assertThat(hb1.getLockTtl()).isEqualTo(10_000);

        // Heartbeat with smaller TTL — must NOT decrease.
        var hb2 = tikv.kvTxnHeartBeat(Kvrpcpb.TxnHeartBeatRequest.newBuilder()
                .setPrimaryLock(ByteString.copyFromUtf8("hb-primary"))
                .setStartVersion(startTs)
                .setAdviseLockTtl(500)
                .build());
        assertThat(hb2.hasError()).isFalse();
        assertThat(hb2.getLockTtl()).isEqualTo(10_000);

        // ScanLock confirms the new TTL is durable on disk.
        var locks = tikv.kvScanLock(Kvrpcpb.ScanLockRequest.newBuilder()
                .setMaxVersion(1_000_000)
                .setLimit(10)
                .build());
        assertThat(locks.getLocksCount()).isEqualTo(1);
        assertThat(locks.getLocks(0).getLockTtl()).isEqualTo(10_000);
    }

    @Test
    void resolveLockScansAndCommitsAllLocksByStartTs() {
        // Prewrite a 3-key txn at startTs=100, then resolve at commit_ts=150
        // WITHOUT supplying keys. The apply path must scan the LOCK CF and
        // commit every lock with start_ts==100.
        var prewrite = tikv.kvPrewrite(Kvrpcpb.PrewriteRequest.newBuilder()
                .setStartVersion(100)
                .setLockTtl(3000)
                .setPrimaryLock(ByteString.copyFromUtf8("rl-a"))
                .addMutations(Kvrpcpb.Mutation.newBuilder()
                        .setOp(Kvrpcpb.Op.Put)
                        .setKey(ByteString.copyFromUtf8("rl-a"))
                        .setValue(ByteString.copyFromUtf8("va")))
                .addMutations(Kvrpcpb.Mutation.newBuilder()
                        .setOp(Kvrpcpb.Op.Put)
                        .setKey(ByteString.copyFromUtf8("rl-b"))
                        .setValue(ByteString.copyFromUtf8("vb")))
                .addMutations(Kvrpcpb.Mutation.newBuilder()
                        .setOp(Kvrpcpb.Op.Put)
                        .setKey(ByteString.copyFromUtf8("rl-c"))
                        .setValue(ByteString.copyFromUtf8("vc")))
                .build());
        assertThat(prewrite.getErrorsCount()).isZero();

        var resolve = tikv.kvResolveLock(Kvrpcpb.ResolveLockRequest.newBuilder()
                .setStartVersion(100)
                .setCommitVersion(150)
                .build());
        assertThat(resolve.hasError()).isFalse();

        // No locks remain.
        var locks = tikv.kvScanLock(Kvrpcpb.ScanLockRequest.newBuilder()
                .setMaxVersion(1_000_000)
                .setLimit(100)
                .build());
        assertThat(locks.getLocksCount()).isZero();

        // All three keys readable at ts=200.
        for (String k : List.of("rl-a", "rl-b", "rl-c")) {
            var get = tikv.kvGet(Kvrpcpb.GetRequest.newBuilder()
                    .setKey(ByteString.copyFromUtf8(k))
                    .setVersion(200)
                    .build());
            assertThat(get.getNotFound()).as("key %s found", k).isFalse();
        }
    }

    @Test
    void resolveLockBatchedTxnInfosCommitsAndRollsBack() {
        // Two concurrent txns: T1 (startTs=100) commits at 150, T2 (startTs=200) rolls back.
        // Both leave locks on different keys; one ResolveLock RPC must handle both.
        tikv.kvPrewrite(Kvrpcpb.PrewriteRequest.newBuilder()
                .setStartVersion(100)
                .setLockTtl(3000)
                .setPrimaryLock(ByteString.copyFromUtf8("t1-k"))
                .addMutations(Kvrpcpb.Mutation.newBuilder()
                        .setOp(Kvrpcpb.Op.Put)
                        .setKey(ByteString.copyFromUtf8("t1-k"))
                        .setValue(ByteString.copyFromUtf8("t1-v")))
                .build());
        tikv.kvPrewrite(Kvrpcpb.PrewriteRequest.newBuilder()
                .setStartVersion(200)
                .setLockTtl(3000)
                .setPrimaryLock(ByteString.copyFromUtf8("t2-k"))
                .addMutations(Kvrpcpb.Mutation.newBuilder()
                        .setOp(Kvrpcpb.Op.Put)
                        .setKey(ByteString.copyFromUtf8("t2-k"))
                        .setValue(ByteString.copyFromUtf8("t2-v")))
                .build());

        var resolve = tikv.kvResolveLock(Kvrpcpb.ResolveLockRequest.newBuilder()
                .addTxnInfos(Kvrpcpb.TxnInfo.newBuilder().setTxn(100).setStatus(150))
                .addTxnInfos(Kvrpcpb.TxnInfo.newBuilder().setTxn(200).setStatus(0))
                .build());
        assertThat(resolve.hasError()).isFalse();

        // T1 committed: t1-k visible at ts=300.
        var g1 = tikv.kvGet(Kvrpcpb.GetRequest.newBuilder()
                .setKey(ByteString.copyFromUtf8("t1-k")).setVersion(300).build());
        assertThat(g1.getValue().toStringUtf8()).isEqualTo("t1-v");

        // T2 rolled back: t2-k absent at ts=300.
        var g2 = tikv.kvGet(Kvrpcpb.GetRequest.newBuilder()
                .setKey(ByteString.copyFromUtf8("t2-k")).setVersion(300).build());
        assertThat(g2.getNotFound()).isTrue();
    }

    @Test
    void txnHeartBeatTxnNotFoundOnMissingOrMismatchedLock() {
        // No lock yet → TxnNotFound.
        var hb1 = tikv.kvTxnHeartBeat(Kvrpcpb.TxnHeartBeatRequest.newBuilder()
                .setPrimaryLock(ByteString.copyFromUtf8("ghost"))
                .setStartVersion(123)
                .setAdviseLockTtl(5_000)
                .build());
        assertThat(hb1.hasError()).isTrue();
        assertThat(hb1.getError().hasTxnNotFound()).isTrue();

        // Prewrite at startTs=200; heartbeat at startTs=999 → mismatched start_ts.
        tikv.kvPrewrite(Kvrpcpb.PrewriteRequest.newBuilder()
                .setStartVersion(200)
                .setLockTtl(3000)
                .setPrimaryLock(ByteString.copyFromUtf8("k"))
                .addMutations(Kvrpcpb.Mutation.newBuilder()
                        .setOp(Kvrpcpb.Op.Put)
                        .setKey(ByteString.copyFromUtf8("k"))
                        .setValue(ByteString.copyFromUtf8("v")))
                .build());
        var hb2 = tikv.kvTxnHeartBeat(Kvrpcpb.TxnHeartBeatRequest.newBuilder()
                .setPrimaryLock(ByteString.copyFromUtf8("k"))
                .setStartVersion(999)
                .setAdviseLockTtl(5_000)
                .build());
        assertThat(hb2.hasError()).isTrue();
        assertThat(hb2.getError().hasTxnNotFound()).isTrue();
    }

    @Test
    void checkSecondaryLocksReturnsLockWhenStillPrewritten() {
        // Async-commit-style prewrite: primary "p", secondaries ["s1","s2"].
        long startTs = 100;
        var prewrite = tikv.kvPrewrite(Kvrpcpb.PrewriteRequest.newBuilder()
                .setStartVersion(startTs)
                .setLockTtl(3000)
                .setPrimaryLock(ByteString.copyFromUtf8("p"))
                .setUseAsyncCommit(true)
                .addSecondaries(ByteString.copyFromUtf8("s1"))
                .addSecondaries(ByteString.copyFromUtf8("s2"))
                .addMutations(Kvrpcpb.Mutation.newBuilder()
                        .setOp(Kvrpcpb.Op.Put).setKey(ByteString.copyFromUtf8("p")).setValue(ByteString.copyFromUtf8("vp")))
                .addMutations(Kvrpcpb.Mutation.newBuilder()
                        .setOp(Kvrpcpb.Op.Put).setKey(ByteString.copyFromUtf8("s1")).setValue(ByteString.copyFromUtf8("v1")))
                .addMutations(Kvrpcpb.Mutation.newBuilder()
                        .setOp(Kvrpcpb.Op.Put).setKey(ByteString.copyFromUtf8("s2")).setValue(ByteString.copyFromUtf8("v2")))
                .build());
        assertThat(prewrite.getErrorsCount()).isZero();

        var resp = tikv.kvCheckSecondaryLocks(Kvrpcpb.CheckSecondaryLocksRequest.newBuilder()
                .setStartVersion(startTs)
                .addKeys(ByteString.copyFromUtf8("s1"))
                .addKeys(ByteString.copyFromUtf8("s2"))
                .build());
        assertThat(resp.hasError()).isFalse();
        assertThat(resp.getLocksCount()).isEqualTo(2);
        assertThat(resp.getCommitTs()).isZero();
        // Each lock carries the async-commit flag forward — that's how the
        // resolver discovers it can derive commit_ts from min_commit_ts.
        assertThat(resp.getLocks(0).getUseAsyncCommit()).isTrue();
    }

    @Test
    void checkSecondaryLocksReturnsCommitTsWhenSecondariesCommitted() {
        // Secondary "s1" already committed at commit_ts=150 for start_ts=100.
        commitOnce("s1", "v1", 100, 150);

        var resp = tikv.kvCheckSecondaryLocks(Kvrpcpb.CheckSecondaryLocksRequest.newBuilder()
                .setStartVersion(100)
                .addKeys(ByteString.copyFromUtf8("s1"))
                .build());
        assertThat(resp.hasError()).isFalse();
        assertThat(resp.getCommitTs()).isEqualTo(150);
    }

    @Test
    void checkSecondaryLocksRollsBackProtectivelyForUntouchedKey() {
        // No prewrite ever happened on "ghost". CheckSecondaryLocks should
        // stamp a ROLLBACK to neutralise any future late-arriving prewrite,
        // and report commit_ts=0.
        var resp = tikv.kvCheckSecondaryLocks(Kvrpcpb.CheckSecondaryLocksRequest.newBuilder()
                .setStartVersion(100)
                .addKeys(ByteString.copyFromUtf8("ghost"))
                .build());
        assertThat(resp.hasError()).isFalse();
        assertThat(resp.getCommitTs()).isZero();
        assertThat(resp.getLocksCount()).isZero();

        // A subsequent prewrite at start_ts=100 must now fail on the
        // already-rolled-back key. (This is the v2 invariant the protective
        // ROLLBACK record buys us.)
        var prewrite = tikv.kvPrewrite(Kvrpcpb.PrewriteRequest.newBuilder()
                .setStartVersion(100)
                .setLockTtl(3000)
                .setPrimaryLock(ByteString.copyFromUtf8("ghost"))
                .addMutations(Kvrpcpb.Mutation.newBuilder()
                        .setOp(Kvrpcpb.Op.Put)
                        .setKey(ByteString.copyFromUtf8("ghost"))
                        .setValue(ByteString.copyFromUtf8("v")))
                .build());
        assertThat(prewrite.getErrorsCount()).isPositive();
    }

    @Test
    void pessimisticTxnAcquiresLocksThenCommits() {
        // 1) Acquire pessimistic locks for two keys.
        long startTs = 100, forUpdateTs = 100;
        var pl = tikv.kvPessimisticLock(Kvrpcpb.PessimisticLockRequest.newBuilder()
                .setStartVersion(startTs)
                .setForUpdateTs(forUpdateTs)
                .setLockTtl(3000)
                .setPrimaryLock(ByteString.copyFromUtf8("pl-pri"))
                .addMutations(Kvrpcpb.Mutation.newBuilder()
                        .setOp(Kvrpcpb.Op.PessimisticLock)
                        .setKey(ByteString.copyFromUtf8("pl-pri")))
                .addMutations(Kvrpcpb.Mutation.newBuilder()
                        .setOp(Kvrpcpb.Op.PessimisticLock)
                        .setKey(ByteString.copyFromUtf8("pl-sec")))
                .build());
        assertThat(pl.getErrorsCount()).isZero();

        // 2) Prewrite the same keys with is_pessimistic_lock = [1,1]. The
        //    apply path must call checkPessimisticPrewrite (skip write-conflict
        //    scan) and OVERWRITE the PESSIMISTIC locks with PUT locks.
        var prewrite = tikv.kvPrewrite(Kvrpcpb.PrewriteRequest.newBuilder()
                .setStartVersion(startTs)
                .setForUpdateTs(forUpdateTs)
                .setLockTtl(3000)
                .setPrimaryLock(ByteString.copyFromUtf8("pl-pri"))
                .addIsPessimisticLock(1)
                .addIsPessimisticLock(1)
                .addMutations(Kvrpcpb.Mutation.newBuilder()
                        .setOp(Kvrpcpb.Op.Put)
                        .setKey(ByteString.copyFromUtf8("pl-pri"))
                        .setValue(ByteString.copyFromUtf8("vp")))
                .addMutations(Kvrpcpb.Mutation.newBuilder()
                        .setOp(Kvrpcpb.Op.Put)
                        .setKey(ByteString.copyFromUtf8("pl-sec"))
                        .setValue(ByteString.copyFromUtf8("vs")))
                .build());
        assertThat(prewrite.getErrorsCount()).isZero();

        // 3) Commit must succeed — locks were upgraded from PESSIMISTIC to PUT.
        var commit = tikv.kvCommit(Kvrpcpb.CommitRequest.newBuilder()
                .setStartVersion(startTs)
                .setCommitVersion(150)
                .addKeys(ByteString.copyFromUtf8("pl-pri"))
                .addKeys(ByteString.copyFromUtf8("pl-sec"))
                .build());
        assertThat(commit.hasError()).isFalse();

        var get = tikv.kvGet(Kvrpcpb.GetRequest.newBuilder()
                .setKey(ByteString.copyFromUtf8("pl-pri")).setVersion(200).build());
        assertThat(get.getValue().toStringUtf8()).isEqualTo("vp");
    }

    @Test
    void pessimisticLockBumpsMaxTsToForUpdateTs() {
        // Acquire pessimistic lock at start_ts=100, for_update_ts=500.
        var pl = tikv.kvPessimisticLock(Kvrpcpb.PessimisticLockRequest.newBuilder()
                .setStartVersion(100)
                .setForUpdateTs(500)
                .setLockTtl(3000)
                .setPrimaryLock(ByteString.copyFromUtf8("plft-pri"))
                .addMutations(Kvrpcpb.Mutation.newBuilder()
                        .setOp(Kvrpcpb.Op.PessimisticLock)
                        .setKey(ByteString.copyFromUtf8("plft-pri")))
                .build());
        assertThat(pl.getErrorsCount()).isZero();

        // Prewrite must observe min_commit_ts floor ≥ for_update_ts + 1.
        // We can't read min_commit_ts off the lock directly through public
        // RPC except via ScanLock — verify there.
        var prewrite = tikv.kvPrewrite(Kvrpcpb.PrewriteRequest.newBuilder()
                .setStartVersion(100)
                .setForUpdateTs(500)
                .setLockTtl(3000)
                .setPrimaryLock(ByteString.copyFromUtf8("plft-pri"))
                .addIsPessimisticLock(1)
                .addMutations(Kvrpcpb.Mutation.newBuilder()
                        .setOp(Kvrpcpb.Op.Put)
                        .setKey(ByteString.copyFromUtf8("plft-pri"))
                        .setValue(ByteString.copyFromUtf8("v")))
                .build());
        assertThat(prewrite.getErrorsCount()).isZero();

        var locks = tikv.kvScanLock(Kvrpcpb.ScanLockRequest.newBuilder()
                .setMaxVersion(Long.MAX_VALUE).setLimit(10).build());
        assertThat(locks.getLocksCount()).isEqualTo(1);
        assertThat(locks.getLocks(0).getMinCommitTs())
                .as("min_commit_ts must be > for_update_ts to keep blocked readers consistent")
                .isGreaterThan(500);

        // Commit at for_update_ts (500) must fail because the floor is
        // for_update_ts + 1 = 501 — guarantees commit_ts strictly greater
        // than any reader-blocked-on-this-lock's read_ts.
        var bad = tikv.kvCommit(Kvrpcpb.CommitRequest.newBuilder()
                .setStartVersion(100)
                .setCommitVersion(500)
                .addKeys(ByteString.copyFromUtf8("plft-pri"))
                .build());
        assertThat(bad.hasError()).isTrue();

        // Commit at 501 succeeds.
        var good = tikv.kvCommit(Kvrpcpb.CommitRequest.newBuilder()
                .setStartVersion(100)
                .setCommitVersion(501)
                .addKeys(ByteString.copyFromUtf8("plft-pri"))
                .build());
        assertThat(good.hasError()).isFalse();
    }

    @Test
    void pessimisticPrewriteFailsWhenLockMissing() {
        // No prior PessimisticLock — but the client claims is_pessimistic_lock=[1].
        // checkPessimisticPrewrite must reject with PessimisticLockNotFound.
        var prewrite = tikv.kvPrewrite(Kvrpcpb.PrewriteRequest.newBuilder()
                .setStartVersion(100)
                .setForUpdateTs(100)
                .setLockTtl(3000)
                .setPrimaryLock(ByteString.copyFromUtf8("plnf"))
                .addIsPessimisticLock(1)
                .addMutations(Kvrpcpb.Mutation.newBuilder()
                        .setOp(Kvrpcpb.Op.Put)
                        .setKey(ByteString.copyFromUtf8("plnf"))
                        .setValue(ByteString.copyFromUtf8("v")))
                .build());
        assertThat(prewrite.getErrorsCount()).isPositive();
        assertThat(prewrite.getErrors(0).getAbort())
                .contains("pessimistic-lock-not-found");
    }

    @Test
    void asyncCommitFallsBackToTwoPcWhenMaxCommitTsTooLow() {
        // Drive max_ts above start_ts via a large-version read first.
        tikv.kvGet(Kvrpcpb.GetRequest.newBuilder()
                .setKey(ByteString.copyFromUtf8("anything"))
                .setVersion(1_000_000)
                .build());

        // Client requests async-commit with max_commit_ts=startTs+1 — but
        // max_ts is already 1_000_000, so the round floor is 1_000_001 ≫
        // max_commit_ts. Server must FALL BACK to non-async commit:
        // response.min_commit_ts == 0 AND lock.useAsyncCommit == false.
        long startTs = 100;
        var prewrite = tikv.kvPrewrite(Kvrpcpb.PrewriteRequest.newBuilder()
                .setStartVersion(startTs)
                .setLockTtl(3000)
                .setPrimaryLock(ByteString.copyFromUtf8("ac-fallback"))
                .setUseAsyncCommit(true)
                .setMaxCommitTs(startTs + 1)
                .addMutations(Kvrpcpb.Mutation.newBuilder()
                        .setOp(Kvrpcpb.Op.Put)
                        .setKey(ByteString.copyFromUtf8("ac-fallback"))
                        .setValue(ByteString.copyFromUtf8("v")))
                .build());
        assertThat(prewrite.getErrorsCount()).isZero();
        assertThat(prewrite.getMinCommitTs())
                .as("server clears min_commit_ts when async-commit cannot be honored")
                .isZero();

        // The persisted lock must NOT be flagged async-commit.
        var locks = tikv.kvScanLock(Kvrpcpb.ScanLockRequest.newBuilder()
                .setMaxVersion(1_000_000_000L).setLimit(10).build());
        assertThat(locks.getLocksCount()).isEqualTo(1);
        assertThat(locks.getLocks(0).getUseAsyncCommit()).isFalse();
    }

    @Test
    void checkTxnStatusDoesNotRollbackAsyncCommitPrimaryEvenWhenTtlExpired() {
        // Async-commit prewrite at startTs=t0 (encoded HLC physical_ms in
        // top 46 bits + logical in bottom 18 bits). Pick a startTs whose
        // physical_ms is far in the past so that "currentTs" used in
        // CheckTxnStatus is well beyond ttl_ms — would normally rollback.
        long startTs = 1_000L << 18;             // physical_ms = 1000
        long currentTs = (1_000_000L) << 18;     // physical_ms = 1_000_000 (≈1000s later)

        var prewrite = tikv.kvPrewrite(Kvrpcpb.PrewriteRequest.newBuilder()
                .setStartVersion(startTs)
                .setLockTtl(3000)
                .setPrimaryLock(ByteString.copyFromUtf8("ac-pri"))
                .setUseAsyncCommit(true)
                .addSecondaries(ByteString.copyFromUtf8("ac-sec"))
                .addMutations(Kvrpcpb.Mutation.newBuilder()
                        .setOp(Kvrpcpb.Op.Put)
                        .setKey(ByteString.copyFromUtf8("ac-pri"))
                        .setValue(ByteString.copyFromUtf8("vp")))
                .addMutations(Kvrpcpb.Mutation.newBuilder()
                        .setOp(Kvrpcpb.Op.Put)
                        .setKey(ByteString.copyFromUtf8("ac-sec"))
                        .setValue(ByteString.copyFromUtf8("vs")))
                .build());
        assertThat(prewrite.getErrorsCount()).isZero();

        var status = tikv.kvCheckTxnStatus(Kvrpcpb.CheckTxnStatusRequest.newBuilder()
                .setPrimaryKey(ByteString.copyFromUtf8("ac-pri"))
                .setLockTs(startTs)
                .setCurrentTs(currentTs)
                .setRollbackIfNotExist(true)
                .build());
        assertThat(status.hasError()).isFalse();
        // Critical assertion: action is NOT TtlExpireRollback.
        assertThat(status.getAction()).isEqualTo(Kvrpcpb.Action.NoAction);
        // Lock info propagated, marked async-commit, with secondaries listed.
        assertThat(status.hasLockInfo()).isTrue();
        assertThat(status.getLockInfo().getUseAsyncCommit()).isTrue();
        assertThat(status.getLockInfo().getSecondariesCount()).isEqualTo(1);

        // Lock is still on disk — the txn was NOT rolled back.
        var locks = tikv.kvScanLock(Kvrpcpb.ScanLockRequest.newBuilder()
                .setMaxVersion(Long.MAX_VALUE).setLimit(10).build());
        assertThat(locks.getLocksCount()).isEqualTo(2);
    }

    @Test
    void gcRetainsLatestVisibleAndDropsOlderVersions() {
        // Three sequential commits to "k": values v1@(s=10,c=20), v2@(s=30,c=40),
        // v3@(s=50,c=60). Safe point at 45 means v2 is the latest visible at
        // safePoint, v1 must be dropped, v3 is above safePoint and preserved.
        commitOnce("k", "v1", 10, 20);
        commitOnce("k", "v2", 30, 40);
        commitOnce("k", "v3", 50, 60);

        var gc = tikv.kvGC(Kvrpcpb.GCRequest.newBuilder().setSafePoint(45).build());
        assertThat(gc.hasError()).isFalse();

        // Read at ts=20 — v1 was the visible version at that read_ts, but
        // GC dropped it. After GC, a read at ts=20 will see no version (v2's
        // commitTs=40 > 20). This is acceptable: the GC contract is that
        // read_ts > safePoint is the only supported read range post-GC.
        var atSp = tikv.kvGet(Kvrpcpb.GetRequest.newBuilder()
                .setKey(ByteString.copyFromUtf8("k")).setVersion(45).build());
        assertThat(atSp.getValue().toStringUtf8()).isEqualTo("v2");

        var above = tikv.kvGet(Kvrpcpb.GetRequest.newBuilder()
                .setKey(ByteString.copyFromUtf8("k")).setVersion(70).build());
        assertThat(above.getValue().toStringUtf8()).isEqualTo("v3");
    }

    @Test
    void gcDropsRollbackRecordsBelowSafePoint() {
        // Prewrite + Rollback at startTs=10 leaves a ROLLBACK record at
        // commitTs=10. Then a real PUT at (s=30,c=40). After GC at sp=20,
        // the ROLLBACK record should be gone.
        tikv.kvPrewrite(Kvrpcpb.PrewriteRequest.newBuilder()
                .setStartVersion(10)
                .setLockTtl(3000)
                .setPrimaryLock(ByteString.copyFromUtf8("k"))
                .addMutations(Kvrpcpb.Mutation.newBuilder()
                        .setOp(Kvrpcpb.Op.Put)
                        .setKey(ByteString.copyFromUtf8("k"))
                        .setValue(ByteString.copyFromUtf8("v")))
                .build());
        tikv.kvBatchRollback(Kvrpcpb.BatchRollbackRequest.newBuilder()
                .setStartVersion(10)
                .addKeys(ByteString.copyFromUtf8("k"))
                .build());
        commitOnce("k", "vfinal", 30, 40);

        var gc = tikv.kvGC(Kvrpcpb.GCRequest.newBuilder().setSafePoint(20).build());
        assertThat(gc.hasError()).isFalse();

        var read = tikv.kvGet(Kvrpcpb.GetRequest.newBuilder()
                .setKey(ByteString.copyFromUtf8("k")).setVersion(50).build());
        assertThat(read.getValue().toStringUtf8()).isEqualTo("vfinal");
    }

    @Test
    void kvDeleteRangeWipesEverythingInRangeAcrossAllCfs() {
        // Commit several versions inside the range, plus one outside.
        commitOnce("dr-a", "va", 10, 20);
        commitOnce("dr-b", "vb", 30, 40);
        commitOnce("dr-c", "vc", 50, 60);
        commitOnce("dr-z-outside", "vz", 70, 80);
        // A pending lock inside the range — should be removed too.
        tikv.kvPrewrite(Kvrpcpb.PrewriteRequest.newBuilder()
                .setStartVersion(90)
                .setLockTtl(3000)
                .setPrimaryLock(ByteString.copyFromUtf8("dr-d"))
                .addMutations(Kvrpcpb.Mutation.newBuilder()
                        .setOp(Kvrpcpb.Op.Put)
                        .setKey(ByteString.copyFromUtf8("dr-d"))
                        .setValue(ByteString.copyFromUtf8("vd")))
                .build());

        var resp = tikv.kvDeleteRange(Kvrpcpb.DeleteRangeRequest.newBuilder()
                .setStartKey(ByteString.copyFromUtf8("dr-"))
                .setEndKey(ByteString.copyFromUtf8("dr-z"))   // exclusive — leaves dr-z-outside
                .build());
        assertThat(resp.getError()).isEmpty();

        // Verify in-range keys are gone (no commit record visible).
        for (String k : List.of("dr-a", "dr-b", "dr-c", "dr-d")) {
            var got = tikv.kvGet(Kvrpcpb.GetRequest.newBuilder()
                    .setKey(ByteString.copyFromUtf8(k))
                    .setVersion(1_000)
                    .build());
            assertThat(got.getNotFound()).as("'%s' should be wiped", k).isTrue();
        }
        // Lock CF cleared too.
        var locks = tikv.kvScanLock(Kvrpcpb.ScanLockRequest.newBuilder()
                .setMaxVersion(Long.MAX_VALUE).setLimit(10).build());
        assertThat(locks.getLocksCount()).isZero();

        // Outside-of-range key untouched.
        var outside = tikv.kvGet(Kvrpcpb.GetRequest.newBuilder()
                .setKey(ByteString.copyFromUtf8("dr-z-outside"))
                .setVersion(1_000)
                .build());
        assertThat(outside.getValue().toStringUtf8()).isEqualTo("vz");
    }

    @Test
    void kvCleanupTranslatesCheckTxnStatusToLegacyResponse() {
        // Already committed: Cleanup returns commit_version > 0.
        commitOnce("clk1", "v", 100, 150);
        var c1 = tikv.kvCleanup(Kvrpcpb.CleanupRequest.newBuilder()
                .setKey(ByteString.copyFromUtf8("clk1"))
                .setStartVersion(100)
                .setCurrentTs(((1L << 18) * 1_000_000L))
                .build());
        assertThat(c1.hasError()).isFalse();
        assertThat(c1.getCommitVersion()).isEqualTo(150);

        // Live lock at start_ts=200 with adequate TTL: Cleanup surfaces KeyError.locked.
        long startTs = (1_000L << 18);   // physical_ms = 1000
        long currentTs = (1_500L << 18); // 500ms later — TTL=3000 still alive
        tikv.kvPrewrite(Kvrpcpb.PrewriteRequest.newBuilder()
                .setStartVersion(startTs)
                .setLockTtl(3_000)
                .setPrimaryLock(ByteString.copyFromUtf8("clk2"))
                .addMutations(Kvrpcpb.Mutation.newBuilder()
                        .setOp(Kvrpcpb.Op.Put)
                        .setKey(ByteString.copyFromUtf8("clk2"))
                        .setValue(ByteString.copyFromUtf8("v")))
                .build());
        var c2 = tikv.kvCleanup(Kvrpcpb.CleanupRequest.newBuilder()
                .setKey(ByteString.copyFromUtf8("clk2"))
                .setStartVersion(startTs)
                .setCurrentTs(currentTs)
                .build());
        assertThat(c2.hasError()).isTrue();
        assertThat(c2.getError().hasLocked()).isTrue();
    }

    @Test
    void mvccGetByKeyDumpsAllVersions() {
        // Two committed versions + one pending lock.
        commitOnce("dbgk", "v1", 10, 20);
        commitOnce("dbgk", "v2", 30, 40);
        tikv.kvPrewrite(Kvrpcpb.PrewriteRequest.newBuilder()
                .setStartVersion(50)
                .setLockTtl(3000)
                .setPrimaryLock(ByteString.copyFromUtf8("dbgk"))
                .addMutations(Kvrpcpb.Mutation.newBuilder()
                        .setOp(Kvrpcpb.Op.Put)
                        .setKey(ByteString.copyFromUtf8("dbgk"))
                        .setValue(ByteString.copyFromUtf8("v3-pending")))
                .build());

        var resp = tikv.mvccGetByKey(Kvrpcpb.MvccGetByKeyRequest.newBuilder()
                .setKey(ByteString.copyFromUtf8("dbgk"))
                .build());
        assertThat(resp.getError()).isEmpty();
        var info = resp.getInfo();
        assertThat(info.hasLock()).isTrue();
        assertThat(info.getLock().getLockVersion()).isEqualTo(50);
        // Two committed write records — v1 (cts=20) and v2 (cts=40).
        assertThat(info.getWritesCount()).isEqualTo(2);
        var commitTimes = info.getWritesList().stream()
                .map(Kvrpcpb.MvccWrite::getCommitTs).toList();
        assertThat(commitTimes).containsExactlyInAnyOrder(20L, 40L);
    }

    @Test
    void mvccGetByStartTsFindsLockedKey() {
        // Lock 'sslk' at start_ts=777.
        tikv.kvPrewrite(Kvrpcpb.PrewriteRequest.newBuilder()
                .setStartVersion(777)
                .setLockTtl(3000)
                .setPrimaryLock(ByteString.copyFromUtf8("sslk"))
                .addMutations(Kvrpcpb.Mutation.newBuilder()
                        .setOp(Kvrpcpb.Op.Put)
                        .setKey(ByteString.copyFromUtf8("sslk"))
                        .setValue(ByteString.copyFromUtf8("v")))
                .build());

        var resp = tikv.mvccGetByStartTs(Kvrpcpb.MvccGetByStartTsRequest.newBuilder()
                .setStartTs(777).build());
        assertThat(resp.getError()).isEmpty();
        assertThat(resp.getKey().toStringUtf8()).isEqualTo("sslk");
        assertThat(resp.getInfo().hasLock()).isTrue();
        assertThat(resp.getInfo().getLock().getLockVersion()).isEqualTo(777);
    }

    // ---- helpers ----

    private void commitOnce(String key, String value, long startTs, long commitTs) {
        tikv.kvPrewrite(Kvrpcpb.PrewriteRequest.newBuilder()
                .setStartVersion(startTs)
                .setLockTtl(3000)
                .setPrimaryLock(ByteString.copyFromUtf8(key))
                .addMutations(Kvrpcpb.Mutation.newBuilder()
                        .setOp(Kvrpcpb.Op.Put)
                        .setKey(ByteString.copyFromUtf8(key))
                        .setValue(ByteString.copyFromUtf8(value)))
                .build());
        tikv.kvCommit(Kvrpcpb.CommitRequest.newBuilder()
                .setStartVersion(startTs)
                .setCommitVersion(commitTs)
                .addKeys(ByteString.copyFromUtf8(key))
                .build());
    }
}
