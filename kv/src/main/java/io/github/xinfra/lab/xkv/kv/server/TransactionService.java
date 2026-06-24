package io.github.xinfra.lab.xkv.kv.server;

import com.google.protobuf.ByteString;
import io.github.xinfra.lab.xkv.common.logging.MdcContextUtil;
import io.github.xinfra.lab.xkv.kv.engine.StorageEngine;
import io.github.xinfra.lab.xkv.kv.mvcc.KeyLockedException;
import io.github.xinfra.lab.xkv.kv.mvcc.Lock;
import io.github.xinfra.lab.xkv.kv.mvcc.ConcurrencyManager;
import io.github.xinfra.lab.xkv.kv.mvcc.MaxTsTracker;
import io.github.xinfra.lab.xkv.kv.mvcc.MvccReader;
import io.github.xinfra.lab.xkv.kv.mvcc.TxnSnapshotCache;
import io.github.xinfra.lab.xkv.kv.raft.ProposalCodec;
import io.github.xinfra.lab.xkv.kv.raft.RegionPeer;
import io.github.xinfra.lab.xkv.kv.server.RawKvService.PeerLocator;
import io.github.xinfra.lab.xkv.kv.transport.DeadlockClient;
import io.github.xinfra.lab.xkv.proto.Kvrpcpb;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.concurrent.TimeUnit;

/**
 * Server-side handlers for the Tikv transactional RPCs (Phase 2).
 *
 * <h3>Read path</h3>
 *
 * <p>{@code KvGet} / {@code KvBatchGet} / {@code KvScan} go straight to
 * {@link MvccReader} — no Raft involvement on a leader doing its own read.
 * (Phase 4 will route follower reads through readIndex.)
 *
 * <h3>Write path</h3>
 *
 * <p>{@code KvPrewrite} / {@code KvCommit} / {@code KvBatchRollback} /
 * {@code KvCheckTxnStatus} / {@code KvResolveLock} go through
 * {@link RegionPeer#propose}. The future resolves once the apply loop has
 * fsync'd the result; we then deserialize the response that
 * {@link io.github.xinfra.lab.xkv.kv.raft.MvccApplyHandler} produced.
 */
public final class TransactionService {
    private static final Logger log = LoggerFactory.getLogger(TransactionService.class);

    private final StorageEngine engine;
    private final PeerLocator locator;
    private final long proposeTimeoutMs;
    private final ConcurrencyManager cm;
    private final TxnSnapshotCache snapshotCache;
    private final io.github.xinfra.lab.xkv.kv.mvcc.InMemoryLockTable inMemoryLockTable;
    /** Optional distributed deadlock-detector hook; null when running without PD. */
    private volatile DeadlockClient deadlockClient;

    public TransactionService(StorageEngine engine, PeerLocator locator, long proposeTimeoutMs) {
        this(engine, locator, proposeTimeoutMs,
                new ConcurrencyManager(new MaxTsTracker()),
                new TxnSnapshotCache(engine),
                new io.github.xinfra.lab.xkv.kv.mvcc.InMemoryLockTable());
    }

    public TransactionService(StorageEngine engine, PeerLocator locator,
                              long proposeTimeoutMs, ConcurrencyManager cm) {
        this(engine, locator, proposeTimeoutMs, cm, new TxnSnapshotCache(engine),
                new io.github.xinfra.lab.xkv.kv.mvcc.InMemoryLockTable());
    }

    public TransactionService(StorageEngine engine, PeerLocator locator,
                              long proposeTimeoutMs, ConcurrencyManager cm,
                              io.github.xinfra.lab.xkv.kv.mvcc.InMemoryLockTable inMemoryLockTable) {
        this(engine, locator, proposeTimeoutMs, cm, new TxnSnapshotCache(engine), inMemoryLockTable);
    }

    public TransactionService(StorageEngine engine, PeerLocator locator,
                              long proposeTimeoutMs, ConcurrencyManager cm,
                              TxnSnapshotCache snapshotCache,
                              io.github.xinfra.lab.xkv.kv.mvcc.InMemoryLockTable inMemoryLockTable) {
        this.engine = engine;
        this.locator = locator;
        this.proposeTimeoutMs = proposeTimeoutMs;
        this.cm = cm;
        this.snapshotCache = snapshotCache;
        this.inMemoryLockTable = inMemoryLockTable;
    }

    public MaxTsTracker maxTs() { return cm.maxTs(); }
    public ConcurrencyManager concurrencyManager() { return cm; }
    public TxnSnapshotCache snapshotCache() { return snapshotCache; }
    public io.github.xinfra.lab.xkv.kv.mvcc.InMemoryLockTable inMemoryLockTable() { return inMemoryLockTable; }
    /** Inject the deadlock-detector client. Pass {@code null} to disable. */
    public void setDeadlockClient(DeadlockClient client) { this.deadlockClient = client; }
    public DeadlockClient deadlockClient() { return deadlockClient; }

    // =====================================================================
    // Reads (no Raft)
    // =====================================================================

    public Kvrpcpb.GetResponse kvGet(Kvrpcpb.GetRequest req) {
        byte[] key = req.getKey().toByteArray();
        var leaderError = ensureReadable(key, req.getContext());
        if (leaderError != null) {
            return Kvrpcpb.GetResponse.newBuilder().setRegionError(leaderError).build();
        }
        return cm.withReader(key, req.getVersion(), () -> {
            var b = Kvrpcpb.GetResponse.newBuilder();
            try (var snap = engine.newSnapshot();
                 var reader = new MvccReader(engine, snap, false)) {
                try {
                    var v = reader.get(key, req.getVersion());
                    if (v.isEmpty()) {
                        b.setNotFound(true);
                    } else {
                        b.setValue(ByteString.copyFrom(v.get()));
                    }
                } catch (KeyLockedException e) {
                    b.setError(Kvrpcpb.KeyError.newBuilder()
                            .setLocked(toLockInfo(e.key(), e.lock())));
                }
            }
            return b.build();
        });
    }

    /**
     * Linearizable-read fence. When {@code replica_read} is LEADER (default)
     * or PREFER_LEADER, this peer must be the leader. When FOLLOWER or MIXED,
     * the leader check is skipped — a follower may serve the read via
     * readIndex (the raft protocol forwards MsgReadIndex to the leader,
     * which quorum-confirms and replies with the commit index; the follower
     * waits until its appliedIndex catches up).
     */
    private io.github.xinfra.lab.xkv.proto.Errorpb.Error ensureReadable(byte[] key,
                                                                          Kvrpcpb.Context ctx) {
        var peer = locator.peerForKey(key);
        if (peer == null) return regionNotFound(key);
        MdcContextUtil.setRegion(peer.regionId());

        boolean followerRead = ctx != null && (
                ctx.getReplicaRead() == Kvrpcpb.ReplicaReadType.FOLLOWER
                        || ctx.getReplicaRead() == Kvrpcpb.ReplicaReadType.MIXED);
        if (!followerRead && !peer.isLeader()) return notLeader(peer);

        var regionErr = validateRegion(peer, key, ctx);
        if (regionErr != null) return regionErr;

        try {
            peer.readIndex().get(effectiveProposeTimeoutMs(), TimeUnit.MILLISECONDS);
        } catch (java.util.concurrent.TimeoutException e) {
            log.warn("readIndex timeout for region={}", peer.regionId());
            return notLeader(peer);
        } catch (Exception e) {
            log.warn("readIndex failed for region={}: {}", peer.regionId(), e.getMessage());
            return notLeader(peer);
        }
        return null;
    }

    public Kvrpcpb.BatchGetResponse kvBatchGet(Kvrpcpb.BatchGetRequest req) {
        if (req.getKeysCount() > 0) {
            var le = ensureReadable(req.getKeys(0).toByteArray(), req.getContext());
            if (le != null) return Kvrpcpb.BatchGetResponse.newBuilder().setRegionError(le).build();
        }
        return cm.withReader(req.getVersion(), () -> {
            var b = Kvrpcpb.BatchGetResponse.newBuilder();
            try (var snap = engine.newSnapshot();
                 var reader = new MvccReader(engine, snap, false)) {
                for (var k : req.getKeysList()) {
                    try {
                        var v = reader.get(k.toByteArray(), req.getVersion());
                        if (v.isPresent()) {
                            b.addPairs(Kvrpcpb.KvPair.newBuilder()
                                    .setKey(k)
                                    .setValue(ByteString.copyFrom(v.get()))
                                    .build());
                        }
                    } catch (KeyLockedException e) {
                        b.addPairs(Kvrpcpb.KvPair.newBuilder()
                                .setKey(k)
                                .setError(Kvrpcpb.KeyError.newBuilder()
                                        .setLocked(toLockInfo(e.key(), e.lock())))
                                .build());
                    }
                }
            }
            return b.build();
        });
    }

    public Kvrpcpb.ScanResponse kvScan(Kvrpcpb.ScanRequest req) {
        byte[] routeKey = req.getStartKey().isEmpty() ? new byte[]{0} : req.getStartKey().toByteArray();
        var le = ensureReadable(routeKey, req.getContext());
        if (le != null) return Kvrpcpb.ScanResponse.newBuilder().setRegionError(le).build();
        return cm.withReader(req.getVersion(), () -> {
            var b = Kvrpcpb.ScanResponse.newBuilder();
            try (var snap = engine.newSnapshot();
                 var reader = new MvccReader(engine, snap, false)) {
                byte[] start = req.getStartKey().toByteArray();
                byte[] end = req.getEndKey().isEmpty() ? null : req.getEndKey().toByteArray();
                int limit = req.getLimit() <= 0 ? Integer.MAX_VALUE : req.getLimit();
                MvccReader.ScanResult result;
                if (req.getReverse()) {
                    result = reader.reverseScan(start, end, limit, req.getVersion());
                } else {
                    result = reader.scan(start, end, limit, req.getVersion());
                }
                for (var p : result.pairs()) {
                    var pb = Kvrpcpb.KvPair.newBuilder()
                            .setKey(ByteString.copyFrom(p.key()));
                    if (!req.getKeyOnly()) {
                        pb.setValue(ByteString.copyFrom(p.value()));
                    }
                    b.addPairs(pb.build());
                }
                if (result.lockError() != null) {
                    var e = result.lockError();
                    b.addPairs(Kvrpcpb.KvPair.newBuilder()
                            .setKey(ByteString.copyFrom(e.key()))
                            .setError(Kvrpcpb.KeyError.newBuilder()
                                    .setLocked(toLockInfo(e.key(), e.lock()))
                                    .build())
                            .build());
                }
            }
            return b.build();
        });
    }

    /**
     * Debug RPC: dump every MVCC artefact for one user-key — the current
     * lock (if any) plus every WRITE CF record and every DEFAULT CF value
     * keyed by this user-key. Reads are bound to a single snapshot so the
     * dump is point-in-time consistent across CFs.
     */
    public Kvrpcpb.MvccGetByKeyResponse kvMvccGetByKey(Kvrpcpb.MvccGetByKeyRequest req) {
        byte[] userKey = req.getKey().toByteArray();
        var info = Kvrpcpb.MvccInfo.newBuilder();
        try (var snap = engine.newSnapshot();
             var ro = engine.newReadOptions().snapshot(snap)) {

            // Lock CF (keys stored as KeyCodec.encodeBytes(userKey))
            byte[] lockBytes = engine.get(StorageEngine.Cf.LOCK,
                    io.github.xinfra.lab.xkv.kv.mvcc.MvccKey.lockKey(userKey), ro);
            if (lockBytes != null) {
                info.setLock(toLockInfo(userKey, Lock.decode(lockBytes)));
            }

            // Write CF: all versions for this user-key.
            try (var it = engine.newIterator(StorageEngine.Cf.WRITE, ro)) {
                byte[] seek = io.github.xinfra.lab.xkv.kv.mvcc.MvccKey.firstVersionFor(userKey);
                for (it.seek(seek); it.isValid(); it.next()) {
                    if (!io.github.xinfra.lab.xkv.kv.mvcc.MvccKey.userKeyEquals(it.key(), userKey)) break;
                    var w = io.github.xinfra.lab.xkv.kv.mvcc.Write.decode(it.value());
                    long ct = io.github.xinfra.lab.xkv.kv.mvcc.MvccKey.ts(it.key());
                    var mw = Kvrpcpb.MvccWrite.newBuilder()
                            .setType(switch (w.type()) {
                                case PUT -> Kvrpcpb.Op.Put;
                                case DELETE -> Kvrpcpb.Op.Del;
                                case ROLLBACK -> Kvrpcpb.Op.Rollback;
                                case LOCK -> Kvrpcpb.Op.Lock;
                            })
                            .setStartTs(w.startTs())
                            .setCommitTs(ct)
                            .setHasOverlappedRollback(w.hasOverlappedRollback());
                    if (w.hasShortValue()) {
                        mw.setShortValue(ByteString.copyFrom(w.shortValue()));
                    }
                    info.addWrites(mw.build());
                }
            }

            // Default CF: all values for this user-key.
            try (var it = engine.newIterator(StorageEngine.Cf.DEFAULT, ro)) {
                byte[] seek = io.github.xinfra.lab.xkv.kv.mvcc.MvccKey.firstVersionFor(userKey);
                for (it.seek(seek); it.isValid(); it.next()) {
                    if (!io.github.xinfra.lab.xkv.kv.mvcc.MvccKey.userKeyEquals(it.key(), userKey)) break;
                    long startTs = io.github.xinfra.lab.xkv.kv.mvcc.MvccKey.ts(it.key());
                    info.addValues(Kvrpcpb.MvccValue.newBuilder()
                            .setStartTs(startTs)
                            .setValue(ByteString.copyFrom(it.value()))
                            .build());
                }
            }
        }
        return Kvrpcpb.MvccGetByKeyResponse.newBuilder().setInfo(info.build()).build();
    }

    /**
     * Debug RPC: find any user-key that has activity at {@code start_ts} —
     * either a lock at this start_ts or a WRITE record whose {@code
     * w.start_ts == start_ts}. Used by tooling to inspect a specific
     * transaction's footprint. Returns the FIRST match; the caller
     * iterates by issuing separate calls per region.
     */
    public Kvrpcpb.MvccGetByStartTsResponse kvMvccGetByStartTs(Kvrpcpb.MvccGetByStartTsRequest req) {
        long startTs = req.getStartTs();
        var resp = Kvrpcpb.MvccGetByStartTsResponse.newBuilder();
        try (var snap = engine.newSnapshot();
             var ro = engine.newReadOptions().snapshot(snap)) {

            // Look in lock CF first (faster, smaller).
            try (var it = engine.newIterator(StorageEngine.Cf.LOCK, ro)) {
                for (it.seek(new byte[]{0}); it.isValid(); it.next()) {
                    var lock = Lock.decode(it.value());
                    if (lock.startTs() == startTs) {
                        byte[] rawKey = io.github.xinfra.lab.xkv.kv.mvcc.MvccKey.userKeyFromLockKey(it.key());
                        return resp.setKey(ByteString.copyFrom(rawKey))
                                .setInfo(buildMvccInfoForKey(rawKey))
                                .build();
                    }
                }
            }

            // Fall through: scan WRITE CF for any record at start_ts.
            try (var it = engine.newIterator(StorageEngine.Cf.WRITE, ro)) {
                for (it.seek(new byte[]{0}); it.isValid(); it.next()) {
                    var w = io.github.xinfra.lab.xkv.kv.mvcc.Write.decode(it.value());
                    if (w.startTs() == startTs) {
                        byte[] userKey = io.github.xinfra.lab.xkv.kv.mvcc.MvccKey.userKey(it.key());
                        return resp.setKey(ByteString.copyFrom(userKey))
                                .setInfo(buildMvccInfoForKey(userKey))
                                .build();
                    }
                }
            }
        }
        // No match — empty response.
        return resp.build();
    }

    private Kvrpcpb.MvccInfo buildMvccInfoForKey(byte[] userKey) {
        var byKey = kvMvccGetByKey(Kvrpcpb.MvccGetByKeyRequest.newBuilder()
                .setKey(ByteString.copyFrom(userKey)).build());
        return byKey.getInfo();
    }

    public Kvrpcpb.ScanLockResponse kvScanLock(Kvrpcpb.ScanLockRequest req) {
        cm.maxTs().observe(req.getMaxVersion());
        byte[] routeKey = req.getStartKey().isEmpty() ? new byte[]{0} : req.getStartKey().toByteArray();
        var le = ensureReadable(routeKey, req.getContext());
        if (le != null) {
            return Kvrpcpb.ScanLockResponse.newBuilder().setRegionError(le).build();
        }
        var b = Kvrpcpb.ScanLockResponse.newBuilder();
        try (var snap = engine.newSnapshot();
             var ro = engine.newReadOptions().snapshot(snap);
             var it = engine.newIterator(StorageEngine.Cf.LOCK, ro)) {
            byte[] start = req.getStartKey().isEmpty() ? new byte[]{0} : req.getStartKey().toByteArray();
            byte[] end = req.getEndKey().isEmpty() ? null : req.getEndKey().toByteArray();
            int limit = req.getLimit() <= 0 ? Integer.MAX_VALUE : req.getLimit();
            int n = 0;
            for (it.seek(start); it.isValid() && n < limit; it.next()) {
                if (end != null && java.util.Arrays.compareUnsigned(it.key(), end) >= 0) break;
                var lock = Lock.decode(it.value());
                if (lock.startTs() <= req.getMaxVersion()) {
                    b.addLocks(toLockInfo(it.key(), lock));
                    n++;
                }
            }
        }
        return b.build();
    }

    // =====================================================================
    // Writes (through Raft)
    // =====================================================================

    public Kvrpcpb.PrewriteResponse kvPrewrite(Kvrpcpb.PrewriteRequest req) {
        if (req.getMutationsCount() == 0) return Kvrpcpb.PrewriteResponse.newBuilder().build();
        byte[] sample = req.getMutations(0).getKey().toByteArray();
        return propose(ProposalCodec.Kind.MVCC_PREWRITE, req.toByteArray(), sample, req.getContext(),
                bytes -> {
                    try { return Kvrpcpb.PrewriteResponse.parseFrom(bytes); }
                    catch (Exception e) { throw new RuntimeException(e); }
                },
                err -> Kvrpcpb.PrewriteResponse.newBuilder()
                        .addErrors(Kvrpcpb.KeyError.newBuilder().setAbort(err))
                        .build(),
                re -> Kvrpcpb.PrewriteResponse.newBuilder().setRegionError(re).build());
    }

    public Kvrpcpb.CommitResponse kvCommit(Kvrpcpb.CommitRequest req) {
        if (req.getKeysCount() == 0) return Kvrpcpb.CommitResponse.newBuilder().build();
        byte[] sample = req.getKeys(0).toByteArray();
        var resp = propose(ProposalCodec.Kind.MVCC_COMMIT, req.toByteArray(), sample, req.getContext(),
                bytes -> {
                    try { return Kvrpcpb.CommitResponse.parseFrom(bytes); }
                    catch (Exception e) { throw new RuntimeException(e); }
                },
                err -> Kvrpcpb.CommitResponse.newBuilder()
                        .setError(Kvrpcpb.KeyError.newBuilder().setAbort(err))
                        .build(),
                re -> Kvrpcpb.CommitResponse.newBuilder().setRegionError(re).build());
        // The txn released its locks — drop every wait-for edge that named it
        // as a holder so subsequent waiters proceed without false-positive
        // cycles. (Safe to call regardless of success: on commit failure the
        // locks are still held; cleanup of a never-inserted edge is a no-op.)
        if (!resp.hasRegionError() && !resp.hasError()) {
            var dc = deadlockClient;
            if (dc != null) dc.cleanupHolder(req.getStartVersion());
        }
        return resp;
    }

    public Kvrpcpb.BatchRollbackResponse kvBatchRollback(Kvrpcpb.BatchRollbackRequest req) {
        if (req.getKeysCount() == 0) return Kvrpcpb.BatchRollbackResponse.newBuilder().build();
        byte[] sample = req.getKeys(0).toByteArray();
        var resp = propose(ProposalCodec.Kind.MVCC_ROLLBACK, req.toByteArray(), sample, req.getContext(),
                bytes -> {
                    try { return Kvrpcpb.BatchRollbackResponse.parseFrom(bytes); }
                    catch (Exception e) { throw new RuntimeException(e); }
                },
                err -> Kvrpcpb.BatchRollbackResponse.newBuilder()
                        .setError(Kvrpcpb.KeyError.newBuilder().setAbort(err))
                        .build(),
                re -> Kvrpcpb.BatchRollbackResponse.newBuilder().setRegionError(re).build());
        if (!resp.hasRegionError() && !resp.hasError()) {
            var dc = deadlockClient;
            if (dc != null) dc.cleanupHolder(req.getStartVersion());
        }
        return resp;
    }

    /**
     * Pipelined pessimistic lock: do the write-conflict check locally using
     * a snapshot, insert into InMemoryLockTable, return success immediately,
     * then fire-and-forget the Raft proposal for durability.
     *
     * <p>On crash before Raft persists, the in-memory lock is lost; prewrite
     * detects {@code PessimisticLockNotFound} and the client retries. This
     * matches TiKV's pipelined pessimistic lock design.
     */
    public Kvrpcpb.PessimisticLockResponse kvPessimisticLock(Kvrpcpb.PessimisticLockRequest req) {
        if (req.getMutationsCount() == 0) return Kvrpcpb.PessimisticLockResponse.newBuilder().build();
        byte[] sample = req.getMutations(0).getKey().toByteArray();

        var peer = locator.peerForKey(sample);
        if (peer == null) return Kvrpcpb.PessimisticLockResponse.newBuilder()
                .setRegionError(regionNotFound(sample)).build();
        if (!peer.isLeader()) return Kvrpcpb.PessimisticLockResponse.newBuilder()
                .setRegionError(notLeader(peer)).build();

        var regionErr = validateRegion(peer, sample, req.getContext());
        if (regionErr != null) return Kvrpcpb.PessimisticLockResponse.newBuilder()
                .setRegionError(regionErr).build();

        // Bump max_ts to at least for_update_ts for SI correctness.
        if (req.getForUpdateTs() != 0) {
            cm.maxTs().observe(req.getForUpdateTs());
        }

        // Fast path: local write-conflict check + in-memory lock insertion.
        var resp = Kvrpcpb.PessimisticLockResponse.newBuilder();
        boolean allAcquired = true;
        try (var snap = engine.newSnapshot();
             var reader = new MvccReader(engine, snap, true)) {
            var txn = new io.github.xinfra.lab.xkv.kv.mvcc.MvccTxn(
                    engine.newWriteBatch(), reader);

            for (var m : req.getMutationsList()) {
                byte[] key = m.getKey().toByteArray();

                // Check existing in-memory lock from other txn.
                var memLock = inMemoryLockTable.get(key);
                if (memLock.isPresent() && memLock.get().startTs() != req.getStartVersion()) {
                    resp.addErrors(Kvrpcpb.KeyError.newBuilder()
                            .setLocked(toLockInfo(key, memLock.get())).build());
                    allAcquired = false;
                    continue;
                }

                // Check existing lock in LOCK CF.
                var lockOpt = reader.readLock(key);
                if (lockOpt.isPresent() && lockOpt.get().startTs() != req.getStartVersion()) {
                    resp.addErrors(Kvrpcpb.KeyError.newBuilder()
                            .setLocked(toLockInfo(key, lockOpt.get())).build());
                    allAcquired = false;
                    continue;
                }

                // Write conflict: any commit at commitTs >= forUpdateTs?
                var latestW = reader.readLatestWriteWithTs(key);
                if (latestW.isPresent() && latestW.get().commitTs() >= req.getForUpdateTs()) {
                    long conflictTs = latestW.get().commitTs();
                    resp.addErrors(Kvrpcpb.KeyError.newBuilder()
                            .setConflict(Kvrpcpb.WriteConflict.newBuilder()
                                    .setStartTs(req.getStartVersion())
                                    .setConflictTs(conflictTs)
                                    .setConflictCommitTs(conflictTs)
                                    .setKey(m.getKey())
                                    .setPrimary(req.getPrimaryLock())
                                    .setReason(Kvrpcpb.WriteConflict.Reason.PessimisticRetry))
                            .build());
                    allAcquired = false;
                    continue;
                }

                // Check self-rollback.
                var rb = reader.findWriteByStartTs(key, req.getStartVersion());
                if (rb.isPresent() && rb.get().type() == io.github.xinfra.lab.xkv.kv.mvcc.Write.Type.ROLLBACK) {
                    resp.addErrors(Kvrpcpb.KeyError.newBuilder()
                            .setConflict(Kvrpcpb.WriteConflict.newBuilder()
                                    .setStartTs(req.getStartVersion())
                                    .setConflictTs(0)
                                    .setConflictCommitTs(0)
                                    .setKey(m.getKey())
                                    .setPrimary(req.getPrimaryLock())
                                    .setReason(Kvrpcpb.WriteConflict.Reason.PessimisticRetry))
                            .build());
                    allAcquired = false;
                    continue;
                }

                // Insert into in-memory lock table.
                var lock = Lock.builder()
                        .type(Lock.Type.PESSIMISTIC)
                        .primary(req.getPrimaryLock().toByteArray())
                        .startTs(req.getStartVersion())
                        .forUpdateTs(req.getForUpdateTs())
                        .ttlMs(req.getLockTtl())
                        .build();
                var conflict = inMemoryLockTable.put(key, lock);
                if (conflict.isPresent()) {
                    resp.addErrors(Kvrpcpb.KeyError.newBuilder()
                            .setLocked(toLockInfo(key, conflict.get())).build());
                    allAcquired = false;
                    continue;
                }
            }
        }

        // Deadlock detection on locked errors.
        var dc = deadlockClient;
        if (dc != null && resp.getErrorsCount() > 0) {
            for (int i = 0; i < resp.getErrorsCount(); i++) {
                var err = resp.getErrors(i);
                if (!err.hasLocked()) continue;
                var lock = err.getLocked();
                long waiter = req.getStartVersion();
                long holder = lock.getLockVersion();
                if (waiter == 0L || holder == 0L || waiter == holder) continue;
                var res = dc.detect(waiter, holder, lock.getKey().toByteArray());
                if (!res.isDeadlock()) continue;
                var dl = Kvrpcpb.Deadlock.newBuilder()
                        .setLockTs(holder)
                        .setLockKey(lock.getKey())
                        .setDeadlockKeyHash(res.deadlockKeyHash())
                        .addAllWaitChain(res.waitChain())
                        .build();
                resp.setErrors(i, Kvrpcpb.KeyError.newBuilder().setDeadlock(dl).build());
                log.info("deadlock detected: waiter={} holder={} chain_len={}",
                        waiter, holder, res.waitChain().size());
            }
        }

        // Fire-and-forget: propose through Raft for durability.
        if (allAcquired) {
            try {
                var envelope = ProposalCodec.encode(ProposalCodec.Kind.MVCC_PESSIMISTIC_LOCK,
                        0, req.toByteArray());
                peer.propose(new RegionPeer.Proposal(envelope, 0, 0));
            } catch (Exception e) {
                log.warn("pipelined pessimistic lock async propose failed: {}", e.getMessage());
            }
        }

        return resp.build();
    }

    public Kvrpcpb.PessimisticRollbackResponse kvPessimisticRollback(Kvrpcpb.PessimisticRollbackRequest req) {
        if (req.getKeysCount() == 0) return Kvrpcpb.PessimisticRollbackResponse.newBuilder().build();
        // Clean in-memory locks before proposing the Raft rollback.
        for (var k : req.getKeysList()) {
            inMemoryLockTable.remove(k.toByteArray(), req.getStartVersion());
        }
        byte[] sample = req.getKeys(0).toByteArray();
        var resp = propose(ProposalCodec.Kind.MVCC_PESSIMISTIC_ROLLBACK, req.toByteArray(), sample, req.getContext(),
                bytes -> {
                    try { return Kvrpcpb.PessimisticRollbackResponse.parseFrom(bytes); }
                    catch (Exception e) { throw new RuntimeException(e); }
                },
                err -> Kvrpcpb.PessimisticRollbackResponse.newBuilder()
                        .addErrors(Kvrpcpb.KeyError.newBuilder().setAbort(err))
                        .build(),
                re -> Kvrpcpb.PessimisticRollbackResponse.newBuilder().setRegionError(re).build());
        if (!resp.hasRegionError() && resp.getErrorsCount() == 0) {
            var dc = deadlockClient;
            if (dc != null) {
                dc.cleanupHolder(req.getStartVersion());
                dc.cleanupWaiter(req.getStartVersion());
            }
        }
        return resp;
    }

    public Kvrpcpb.ResolveLockResponse kvResolveLock(Kvrpcpb.ResolveLockRequest req) {
        // The apply path supports three input shapes (explicit keys / batched
        // txn_infos / single start_version full-scan). When no keys are
        // supplied we route by a sentinel low key — single-region world for
        // now; the multi-region worker will fan out per region.
        byte[] sample = req.getKeysCount() > 0 ? req.getKeys(0).toByteArray() : new byte[]{0};
        return propose(ProposalCodec.Kind.MVCC_RESOLVE, req.toByteArray(), sample, req.getContext(),
                bytes -> {
                    try { return Kvrpcpb.ResolveLockResponse.parseFrom(bytes); }
                    catch (Exception e) { throw new RuntimeException(e); }
                },
                err -> Kvrpcpb.ResolveLockResponse.newBuilder()
                        .setError(Kvrpcpb.KeyError.newBuilder().setAbort(err))
                        .build(),
                re -> Kvrpcpb.ResolveLockResponse.newBuilder().setRegionError(re).build());
    }

    /**
     * Legacy {@code Cleanup} API — implemented by routing through
     * CheckTxnStatus and translating the verdict to the older response
     * shape. Kept around because some older clients still call it.
     */
    public Kvrpcpb.CleanupResponse kvCleanup(Kvrpcpb.CleanupRequest req) {
        var status = kvCheckTxnStatus(Kvrpcpb.CheckTxnStatusRequest.newBuilder()
                .setPrimaryKey(req.getKey())
                .setLockTs(req.getStartVersion())
                .setCallerStartTs(0)
                .setCurrentTs(req.getCurrentTs())
                .setRollbackIfNotExist(true)
                .build());
        var resp = Kvrpcpb.CleanupResponse.newBuilder();
        if (status.hasError()) {
            return resp.setError(status.getError()).build();
        }
        // Already committed → return commit_version, no error.
        if (status.getCommitVersion() > 0) {
            return resp.setCommitVersion(status.getCommitVersion()).build();
        }
        // NoAction with a live lock → expose it as KeyError.locked so the
        // caller can wait + retry. (CheckTxnStatus surfaced lock_info.)
        if (status.getAction() == Kvrpcpb.Action.NoAction && status.hasLockInfo()) {
            return resp.setError(Kvrpcpb.KeyError.newBuilder()
                    .setLocked(status.getLockInfo())).build();
        }
        // Anything else (rollback / lock-not-exist) is a clean cleanup.
        return resp.build();
    }

    public Kvrpcpb.CheckTxnStatusResponse kvCheckTxnStatus(Kvrpcpb.CheckTxnStatusRequest req) {
        return propose(ProposalCodec.Kind.MVCC_CHECK_TXN_STATUS, req.toByteArray(),
                req.getPrimaryKey().toByteArray(), req.getContext(),
                bytes -> {
                    try { return Kvrpcpb.CheckTxnStatusResponse.parseFrom(bytes); }
                    catch (Exception e) { throw new RuntimeException(e); }
                },
                err -> Kvrpcpb.CheckTxnStatusResponse.newBuilder()
                        .setError(Kvrpcpb.KeyError.newBuilder().setAbort(err))
                        .build(),
                re -> Kvrpcpb.CheckTxnStatusResponse.newBuilder().setRegionError(re).build());
    }

    public Kvrpcpb.DeleteRangeResponse kvDeleteRange(Kvrpcpb.DeleteRangeRequest req) {
        // Routed through Raft so all replicas drop the same range.
        // Routes by start_key — single-region world for now; multi-region
        // dispatch will fan out per-region.
        byte[] sample = req.getStartKey().toByteArray();
        if (sample.length == 0) sample = new byte[]{0};
        return propose(ProposalCodec.Kind.TXN_DELETE_RANGE, req.toByteArray(), sample, req.getContext(),
                bytes -> {
                    try { return Kvrpcpb.DeleteRangeResponse.parseFrom(bytes); }
                    catch (Exception e) { throw new RuntimeException(e); }
                },
                err -> Kvrpcpb.DeleteRangeResponse.newBuilder().setError(err).build(),
                re -> Kvrpcpb.DeleteRangeResponse.newBuilder().setRegionError(re).build());
    }

    public Kvrpcpb.GCResponse kvGC(Kvrpcpb.GCRequest req) {
        // GC is admin; route by an empty key (Phase 1 single-region setup).
        return propose(ProposalCodec.Kind.MVCC_GC, req.toByteArray(), new byte[]{0}, req.getContext(),
                bytes -> {
                    try { return Kvrpcpb.GCResponse.parseFrom(bytes); }
                    catch (Exception e) { throw new RuntimeException(e); }
                },
                err -> Kvrpcpb.GCResponse.newBuilder()
                        .setError(Kvrpcpb.KeyError.newBuilder().setAbort(err))
                        .build(),
                re -> Kvrpcpb.GCResponse.newBuilder().setRegionError(re).build());
    }

    public Kvrpcpb.CheckSecondaryLocksResponse kvCheckSecondaryLocks(Kvrpcpb.CheckSecondaryLocksRequest req) {
        // Routed through raft because the apply path may write protective
        // ROLLBACK records (when neither lock nor commit/rollback exists for
        // a secondary, we stamp a rollback to neutralise late prewrites).
        // Empty key list is a degenerate case that would be a client bug;
        // route by a sentinel low key.
        byte[] sample = req.getKeysCount() > 0 ? req.getKeys(0).toByteArray() : new byte[]{0};
        return propose(ProposalCodec.Kind.MVCC_CHECK_SECONDARY_LOCKS, req.toByteArray(), sample, req.getContext(),
                bytes -> {
                    try { return Kvrpcpb.CheckSecondaryLocksResponse.parseFrom(bytes); }
                    catch (Exception e) { throw new RuntimeException(e); }
                },
                err -> Kvrpcpb.CheckSecondaryLocksResponse.newBuilder()
                        .setError(Kvrpcpb.KeyError.newBuilder().setAbort(err))
                        .build(),
                re -> Kvrpcpb.CheckSecondaryLocksResponse.newBuilder().setRegionError(re).build());
    }

    public Kvrpcpb.TxnHeartBeatResponse kvTxnHeartBeat(Kvrpcpb.TxnHeartBeatRequest req) {
        // Routed through raft so the TTL refresh is durable + replicated.
        // The apply path takes max(existing.ttl, advise_lock_ttl) and rewrites
        // the lock; if the lock is gone or belongs to a different start_ts
        // the response carries a TxnNotFound KeyError (matches TiKV).
        return propose(ProposalCodec.Kind.MVCC_TXN_HEARTBEAT, req.toByteArray(),
                req.getPrimaryLock().toByteArray(), req.getContext(),
                bytes -> {
                    try { return Kvrpcpb.TxnHeartBeatResponse.parseFrom(bytes); }
                    catch (Exception e) { throw new RuntimeException(e); }
                },
                err -> Kvrpcpb.TxnHeartBeatResponse.newBuilder()
                        .setError(Kvrpcpb.KeyError.newBuilder().setAbort(err))
                        .build(),
                re -> Kvrpcpb.TxnHeartBeatResponse.newBuilder().setRegionError(re).build());
    }

    // =====================================================================
    // Helpers
    // =====================================================================

    @FunctionalInterface
    private interface ResponseDecoder<T> { T decode(byte[] bytes); }
    @FunctionalInterface
    private interface ErrorBuilder<T> { T build(String error); }
    @FunctionalInterface
    private interface RegionErrorBuilder<T> { T build(io.github.xinfra.lab.xkv.proto.Errorpb.Error err); }

    /**
     * Build a {@code RegionError} of the appropriate type from a routing
     * failure. The client's {@code RegionRequestSender} reacts to these
     * by updating its leader cache / re-fetching from PD; without proper
     * typing the client falls through to {@code UNKNOWN_COMMIT_STATE}.
     */
    private static io.github.xinfra.lab.xkv.proto.Errorpb.Error notLeader(RegionPeer peer) {
        var nl = io.github.xinfra.lab.xkv.proto.Errorpb.NotLeader.newBuilder()
                .setRegionId(peer == null ? 0L : peer.regionId());
        return io.github.xinfra.lab.xkv.proto.Errorpb.Error.newBuilder()
                .setMessage("not leader")
                .setNotLeader(nl)
                .build();
    }

    private static io.github.xinfra.lab.xkv.proto.Errorpb.Error regionNotFound(byte[] key) {
        var rnf = io.github.xinfra.lab.xkv.proto.Errorpb.RegionNotFound.newBuilder();
        return io.github.xinfra.lab.xkv.proto.Errorpb.Error.newBuilder()
                .setMessage("region not found")
                .setRegionNotFound(rnf)
                .build();
    }

    private <T> T propose(ProposalCodec.Kind kind,
                          byte[] payload,
                          byte[] keyForLocator,
                          ResponseDecoder<T> decoder,
                          ErrorBuilder<T> errorBuilder,
                          RegionErrorBuilder<T> regionErrorBuilder) {
        return propose(kind, payload, keyForLocator, null, decoder, errorBuilder, regionErrorBuilder);
    }

    private <T> T propose(ProposalCodec.Kind kind,
                          byte[] payload,
                          byte[] keyForLocator,
                          Kvrpcpb.Context reqCtx,
                          ResponseDecoder<T> decoder,
                          ErrorBuilder<T> errorBuilder,
                          RegionErrorBuilder<T> regionErrorBuilder) {
        var peer = locator.peerForKey(keyForLocator);
        if (peer == null) return regionErrorBuilder.build(regionNotFound(keyForLocator));
        if (!peer.isLeader()) return regionErrorBuilder.build(notLeader(peer));

        var regionErr = validateRegion(peer, keyForLocator, reqCtx);
        if (regionErr != null) return regionErrorBuilder.build(regionErr);

        try {
            var envelope = ProposalCodec.encode(kind, /* seq= */ 0, payload);
            var future = peer.propose(new RegionPeer.Proposal(envelope, 0, 0));
            long waitMs = effectiveProposeTimeoutMs();
            var result = future.get(waitMs, TimeUnit.MILLISECONDS);
            if (!result.success()) return errorBuilder.build(result.errorMessage());
            return decoder.decode(result.response());
        } catch (Exception e) {
            log.warn("propose {} failed: {}", kind, e.toString());
            String msg = e.getMessage();
            if (msg == null || msg.isEmpty()) msg = e.getClass().getSimpleName();
            return errorBuilder.build(msg);
        }
    }

    /**
     * Validate the request's region epoch and key range against the peer's
     * live region descriptor. Returns a region error if validation fails;
     * {@code null} if all checks pass.
     */
    private static io.github.xinfra.lab.xkv.proto.Errorpb.Error validateRegion(
            RegionPeer peer, byte[] key, Kvrpcpb.Context reqCtx) {
        var region = peer.region();

        // Key-range check: key must be in [startKey, endKey).
        if (key != null && key.length > 0) {
            byte[] startKey = region.getStartKey().toByteArray();
            byte[] endKey = region.getEndKey().toByteArray();
            if (startKey.length > 0 && java.util.Arrays.compareUnsigned(key, startKey) < 0) {
                return keyNotInRegion(key, region);
            }
            if (endKey.length > 0 && java.util.Arrays.compareUnsigned(key, endKey) >= 0) {
                return keyNotInRegion(key, region);
            }
        }

        // Epoch check: if the request carries a region_epoch, it must match
        // the server's live epoch. A stale epoch means the client's region
        // cache is outdated (split/merge/conf-change happened since).
        if (reqCtx != null && reqCtx.hasRegionEpoch()) {
            var reqEpoch = reqCtx.getRegionEpoch();
            var liveEpoch = region.getRegionEpoch();
            if (reqEpoch.getVersion() != liveEpoch.getVersion()
                    || reqEpoch.getConfVer() != liveEpoch.getConfVer()) {
                return epochNotMatch(region);
            }
        }
        return null;
    }

    private static io.github.xinfra.lab.xkv.proto.Errorpb.Error keyNotInRegion(
            byte[] key, io.github.xinfra.lab.xkv.proto.Metapb.Region region) {
        return io.github.xinfra.lab.xkv.proto.Errorpb.Error.newBuilder()
                .setMessage("key not in region")
                .setKeyNotInRegion(io.github.xinfra.lab.xkv.proto.Errorpb.KeyNotInRegion.newBuilder()
                        .setKey(com.google.protobuf.ByteString.copyFrom(key))
                        .setRegionId(region.getId())
                        .setStartKey(region.getStartKey())
                        .setEndKey(region.getEndKey()))
                .build();
    }

    private static io.github.xinfra.lab.xkv.proto.Errorpb.Error epochNotMatch(
            io.github.xinfra.lab.xkv.proto.Metapb.Region region) {
        return io.github.xinfra.lab.xkv.proto.Errorpb.Error.newBuilder()
                .setMessage("epoch not match")
                .setEpochNotMatch(io.github.xinfra.lab.xkv.proto.Errorpb.EpochNotMatch.newBuilder()
                        .addCurrentRegions(region))
                .build();
    }

    /**
     * Effective propose wait: the lesser of the server-default
     * {@code proposeTimeoutMs} and any deadline the calling gRPC client
     * imposed (via {@code stub.withDeadlineAfter}). Honoring the client
     * deadline lets the server fail fast instead of holding a stale
     * proposal slot for {@code proposeTimeoutMs} when the caller has
     * already given up.
     *
     * <p>Clamps to at least 1ms so we don't issue a non-blocking
     * {@code future.get(0)} which would always time out.
     */
    private long effectiveProposeTimeoutMs() {
        var ctxDeadline = io.grpc.Context.current().getDeadline();
        if (ctxDeadline == null) return proposeTimeoutMs;
        long remaining = ctxDeadline.timeRemaining(TimeUnit.MILLISECONDS);
        if (remaining <= 0) return 1;       // already expired — fail almost immediately
        return Math.max(1, Math.min(proposeTimeoutMs, remaining));
    }


    private static Kvrpcpb.LockInfo toLockInfo(byte[] key, Lock lock) {
        var b = Kvrpcpb.LockInfo.newBuilder()
                .setPrimaryLock(ByteString.copyFrom(lock.primary()))
                .setLockVersion(lock.startTs())
                .setKey(ByteString.copyFrom(key))
                .setLockTtl(lock.ttlMs())
                .setTxnSize(lock.txnSize())
                .setMinCommitTs(lock.minCommitTs())
                .setUseAsyncCommit(lock.useAsyncCommit())
                .setLockForUpdateTs(lock.forUpdateTs());
        switch (lock.type()) {
            case PUT -> b.setLockType(Kvrpcpb.Op.Put);
            case DELETE -> b.setLockType(Kvrpcpb.Op.Del);
            case LOCK -> b.setLockType(Kvrpcpb.Op.Lock);
            case PESSIMISTIC -> b.setLockType(Kvrpcpb.Op.PessimisticLock);
        }
        for (var s : lock.secondaries()) {
            b.addSecondaries(ByteString.copyFrom(s));
        }
        return b.build();
    }

}
