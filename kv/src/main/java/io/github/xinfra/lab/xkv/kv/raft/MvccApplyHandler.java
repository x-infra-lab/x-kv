package io.github.xinfra.lab.xkv.kv.raft;

import com.google.protobuf.InvalidProtocolBufferException;
import io.github.xinfra.lab.xkv.kv.cdc.CdcEvent;
import io.github.xinfra.lab.xkv.kv.cdc.CdcEventBus;
import io.github.xinfra.lab.xkv.kv.engine.StorageEngine;
import io.github.xinfra.lab.xkv.kv.mvcc.ConcurrencyManager;
import io.github.xinfra.lab.xkv.kv.mvcc.Lock;
import io.github.xinfra.lab.xkv.kv.mvcc.MaxTsTracker;
import io.github.xinfra.lab.xkv.kv.mvcc.MvccKey;
import io.github.xinfra.lab.xkv.kv.mvcc.MvccReader;
import io.github.xinfra.lab.xkv.kv.mvcc.MvccTxn;
import io.github.xinfra.lab.xkv.proto.Cdcpb;
import io.github.xinfra.lab.xkv.proto.Kvrpcpb;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;

import java.util.Map;

/**
 * Apply-time dispatcher for MVCC / Percolator entries.
 *
 * <h3>All-or-nothing prewrite (the v1 fix)</h3>
 *
 * <p>For every {@code PrewriteRequest} entry the apply path runs TWO
 * passes against the same {@link StorageEngine.WriteBatch}:
 * <ol>
 *   <li><b>Pass 1</b> — for each mutation: {@link MvccTxn#checkPrewrite}.
 *       Pure read; nothing is staged in the batch.</li>
 *   <li><b>Pass 2</b> — only if every check returned ok / alreadyPrewritten:
 *       for each mutation: {@link MvccTxn#writePrewrite}, staging both the
 *       lock CF entry and the default CF value.</li>
 * </ol>
 *
 * <p>If any check failed, the apply emits per-key errors and writes
 * NOTHING to any CF for this entry. v1 wrote partial results and orphaned
 * locks for every mutation that succeeded before the first failure —
 * those orphan locks then bottlenecked unrelated readers.
 *
 * <h3>Wire format</h3>
 *
 * <p>Inner payload of each {@code ProposalCodec} kind is a serialized
 * {@code kvrpcpb.*Request} protobuf. The handler decodes it directly so
 * we can reuse the wire fields without rolling another encoding.
 */
public final class MvccApplyHandler implements ApplyHandler {
    private static final Logger log = LoggerFactory.getLogger(MvccApplyHandler.class);

    private final StorageEngine engine;
    private final ConcurrencyManager cm;
    private final long regionId;
    private final CdcEventBus cdcEventBus;

    public MvccApplyHandler(StorageEngine engine) {
        this(engine, new ConcurrencyManager(new MaxTsTracker()));
    }

    /** Backward-compatible: wraps a bare MaxTsTracker in a fresh ConcurrencyManager. */
    public MvccApplyHandler(StorageEngine engine, MaxTsTracker maxTs) {
        this(engine, new ConcurrencyManager(maxTs));
    }

    public MvccApplyHandler(StorageEngine engine, ConcurrencyManager cm) {
        this(engine, cm, 0, null);
    }

    public MvccApplyHandler(StorageEngine engine, ConcurrencyManager cm,
                            long regionId, CdcEventBus cdcEventBus) {
        this.engine = engine;
        this.cm = cm;
        this.regionId = regionId;
        this.cdcEventBus = cdcEventBus;
    }

    public MaxTsTracker maxTs() { return cm.maxTs(); }
    public ConcurrencyManager concurrencyManager() { return cm; }

    @Override
    public Result apply(ProposalCodec.Decoded decoded, StorageEngine.WriteBatch batch) {
        try {
            return switch (decoded.kind()) {
                case MVCC_PREWRITE -> applyPrewrite(decoded.payload(), batch);
                case MVCC_COMMIT -> applyCommit(decoded.payload(), batch);
                case MVCC_ROLLBACK -> applyRollback(decoded.payload(), batch);
                case MVCC_PESSIMISTIC_LOCK -> applyPessimisticLock(decoded.payload(), batch);
                case MVCC_PESSIMISTIC_ROLLBACK -> applyPessimisticRollback(decoded.payload(), batch);
                case MVCC_RESOLVE -> applyResolveLock(decoded.payload(), batch);
                case MVCC_GC -> applyGc(decoded.payload(), batch);
                case MVCC_CHECK_TXN_STATUS -> applyCheckTxnStatus(decoded.payload(), batch);
                case MVCC_TXN_HEARTBEAT -> applyTxnHeartBeat(decoded.payload(), batch);
                case MVCC_CHECK_SECONDARY_LOCKS -> applyCheckSecondaryLocks(decoded.payload(), batch);
                case TXN_DELETE_RANGE -> applyTxnDeleteRange(decoded.payload(), batch);
                default -> Result.err("unsupported kind: " + decoded.kind());
            };
        } catch (InvalidProtocolBufferException e) {
            log.warn("decode failure for {}: {}", decoded.kind(), e.getMessage());
            return Result.err("decode failed: " + e.getMessage());
        }
    }

    // ===================================================================
    // Prewrite
    // ===================================================================

    private static final int MAX_KEY_SIZE = 4096;
    private static final int MAX_VALUE_SIZE = 8 * 1024 * 1024;

    private Result applyPrewrite(byte[] payload, StorageEngine.WriteBatch batch)
            throws InvalidProtocolBufferException {
        var req = Kvrpcpb.PrewriteRequest.parseFrom(payload);

        for (var m : req.getMutationsList()) {
            if (m.getKey().size() > MAX_KEY_SIZE) {
                return Result.ok(Kvrpcpb.PrewriteResponse.newBuilder()
                        .addErrors(Kvrpcpb.KeyError.newBuilder()
                                .setAbort("key too large: " + m.getKey().size()))
                        .build().toByteArray());
            }
            if (m.getValue().size() > MAX_VALUE_SIZE) {
                return Result.ok(Kvrpcpb.PrewriteResponse.newBuilder()
                        .addErrors(Kvrpcpb.KeyError.newBuilder()
                                .setAbort("value too large: " + m.getValue().size()))
                        .build().toByteArray());
            }
        }

        try (var snap = engine.newSnapshot(); var reader = new MvccReader(engine, snap, true)) {
        var txn = new MvccTxn(batch, reader);

        // Pass 1: check every mutation. Pessimistic-locked mutations skip
        // the write-conflict scan and instead validate the existing
        // PESSIMISTIC lock — see MvccTxn.checkPessimisticPrewrite.
        var keys = new ArrayList<byte[]>(req.getMutationsCount());
        var checks = new ArrayList<MvccTxn.PrewriteOutcome>(req.getMutationsCount());
        var errors = new ArrayList<Kvrpcpb.KeyError>();

        for (int i = 0; i < req.getMutationsCount(); i++) {
            var m = req.getMutations(i);
            byte[] key = m.getKey().toByteArray();
            keys.add(key);
            boolean isPessimistic = i < req.getIsPessimisticLockCount()
                    && req.getIsPessimisticLock(i) != 0L;
            var oc = isPessimistic
                    ? txn.checkPessimisticPrewrite(key, req.getStartVersion())
                    : txn.checkPrewrite(key, req.getStartVersion(), mapOp(m.getOp()));
            checks.add(oc);
            if (oc instanceof MvccTxn.PrewriteWriteConflict wc) {
                errors.add(Kvrpcpb.KeyError.newBuilder()
                        .setConflict(Kvrpcpb.WriteConflict.newBuilder()
                                .setStartTs(wc.startTs())
                                .setConflictTs(wc.conflictCommitTs())
                                .setConflictCommitTs(wc.conflictCommitTs())
                                .setKey(com.google.protobuf.ByteString.copyFrom(key))
                                .setPrimary(req.getPrimaryLock()))
                        .build());
            } else if (oc instanceof MvccTxn.PrewriteKeyLocked kl) {
                errors.add(Kvrpcpb.KeyError.newBuilder()
                        .setLocked(toLockInfo(key, kl.lock()))
                        .build());
            } else if (oc instanceof MvccTxn.PrewriteSelfRolledBack sr) {
                errors.add(Kvrpcpb.KeyError.newBuilder()
                        .setAbort("self-rolled-back at startTs=" + sr.startTs())
                        .build());
            } else if (oc instanceof MvccTxn.PrewritePessimisticLockNotFound nf) {
                errors.add(Kvrpcpb.KeyError.newBuilder()
                        .setAbort("pessimistic-lock-not-found startTs=" + nf.startTs())
                        .build());
            } else if (oc instanceof MvccTxn.PrewriteAlreadyExist ae) {
                errors.add(Kvrpcpb.KeyError.newBuilder()
                        .setAlreadyExist(Kvrpcpb.AlreadyExist.newBuilder()
                                .setKey(com.google.protobuf.ByteString.copyFrom(ae.key())))
                        .build());
            }
        }

        if (!errors.isEmpty()) {
            // Bail out — write nothing.
            var resp = Kvrpcpb.PrewriteResponse.newBuilder().addAllErrors(errors).build();
            return Result.ok(resp.toByteArray());
        }

        // === Async-commit max_commit_ts feasibility check ===
        //
        // The lock floor is max(startTs+1, max_ts+1). If the client capped
        // commit_ts via max_commit_ts and the floor already exceeds that
        // cap, async-commit is impossible. TiKV's response: fall back to
        // standard 2PC by clearing useAsyncCommit on the prewrite (locks
        // get stamped non-async; min_commit_ts=0 in response so the client
        // sends an explicit Commit RPC instead of deriving commit_ts).
        long roundFloor = Math.max(req.getStartVersion() + 1, cm.maxTs().minCommitTsFloor());
        boolean useAsync = req.getUseAsyncCommit();
        if (useAsync && req.getMaxCommitTs() > 0 && roundFloor > req.getMaxCommitTs()) {
            useAsync = false;
        }

        // Pass 2: write every mutation. Note that PrewritePessimisticUpgrade
        // requires writing — it OVERWRITES the existing PESSIMISTIC lock with
        // a PUT/DELETE/LOCK lock so commit can promote it to a Write record.
        long minCommitTs = 0;
        for (int i = 0; i < req.getMutationsCount(); i++) {
            var m = req.getMutationsList().get(i);
            byte[] key = keys.get(i);
            // Skip ONLY if already prewritten (post-upgrade replay). Do NOT
            // skip PrewritePessimisticUpgrade — that's the upgrade case.
            if (checks.get(i) instanceof MvccTxn.PrewriteAlready) continue;

            // for_update_ts: per-mutation only when is_pessimistic_lock[i] != 0;
            // proto allows empty list to mean "all optimistic".
            long forUpdateTs = 0;
            if (i < req.getIsPessimisticLockCount() && req.getIsPessimisticLock(i) != 0L) {
                forUpdateTs = req.getForUpdateTs();
            }

            // Critical: min_commit_ts floor must include max_ts+1 so that
            // any read at read_ts <= current max_ts cannot be invalidated
            // by a later commit at commit_ts <= read_ts. Without this the
            // SI invariant breaks under concurrent reads + writes.
            long lockMinCommitTs = roundFloor;

            txn.writePrewrite(key,
                    m.getValue().toByteArray(),
                    mapOp(m.getOp()),
                    req.getPrimaryLock().toByteArray(),
                    req.getStartVersion(),
                    req.getLockTtl(),
                    req.getTxnSize(),
                    lockMinCommitTs,
                    forUpdateTs,
                    useAsync,
                    req.getSecondariesList().stream().map(s -> s.toByteArray()).toList());

            if (useAsync) {
                minCommitTs = Math.max(minCommitTs, lockMinCommitTs);
            }
        }

        var resp = Kvrpcpb.PrewriteResponse.newBuilder()
                .setMinCommitTs(minCommitTs)
                .build();
        return Result.ok(resp.toByteArray());
        } // close reader
    }

    // ===================================================================
    // Commit
    // ===================================================================

    private Result applyCommit(byte[] payload, StorageEngine.WriteBatch batch)
            throws InvalidProtocolBufferException {
        var req = Kvrpcpb.CommitRequest.parseFrom(payload);
        try (var snap = engine.newSnapshot(); var reader = new MvccReader(engine, snap, true)) {

        // Pass 1: probe every key into a throwaway batch so failure on a
        // late key doesn't leak a partial commit through. The throwaway
        // batch is closed without flushing; only Pass 2's writes hit disk.
        try (var probe = engine.newWriteBatch()) {
            var probeTxn = new MvccTxn(probe, reader);
            for (var k : req.getKeysList()) {
                var oc = probeTxn.commit(k.toByteArray(),
                        req.getStartVersion(), req.getCommitVersion());
                if (oc instanceof MvccTxn.CommitInvalidCommitTs) {
                    return Result.ok(Kvrpcpb.CommitResponse.newBuilder()
                            .setError(Kvrpcpb.KeyError.newBuilder()
                                    .setCommitTsExpired(Kvrpcpb.CommitTsExpired.newBuilder()
                                            .setStartTs(req.getStartVersion())
                                            .setKey(k)
                                            .setAttemptedCommitTs(req.getCommitVersion())))
                            .build()
                            .toByteArray());
                }
                if (oc instanceof MvccTxn.CommitAlreadyRolledBack) {
                    return Result.ok(Kvrpcpb.CommitResponse.newBuilder()
                            .setError(Kvrpcpb.KeyError.newBuilder()
                                    .setAbort("txn already rolled back"))
                            .build()
                            .toByteArray());
                }
                if (oc instanceof MvccTxn.CommitTxnNotFound) {
                    return Result.ok(Kvrpcpb.CommitResponse.newBuilder()
                            .setError(Kvrpcpb.KeyError.newBuilder()
                                    .setTxnNotFound(Kvrpcpb.TxnNotFound.newBuilder()
                                            .setStartTs(req.getStartVersion())
                                            .setPrimaryKey(k)))
                            .build()
                            .toByteArray());
                }
            }
            // probe close drops staged writes — Pass 2 re-runs on the real batch.
        }

        // CDC: snapshot lock types AND values before commit deletes them.
        Map<com.google.protobuf.ByteString, Lock> cdcLocks = null;
        Map<com.google.protobuf.ByteString, byte[]> cdcValues = null;
        if (cdcEventBus != null && cdcEventBus.hasSubscribers(regionId)) {
            cdcLocks = new HashMap<>();
            cdcValues = new HashMap<>();
            for (var k : req.getKeysList()) {
                var lockOpt = reader.readLock(k.toByteArray());
                if (lockOpt.isPresent() && lockOpt.get().startTs() == req.getStartVersion()) {
                    cdcLocks.put(k, lockOpt.get());
                    if (lockOpt.get().type() == Lock.Type.PUT) {
                        byte[] v = engine.get(StorageEngine.Cf.DEFAULT,
                                MvccKey.encode(k.toByteArray(), req.getStartVersion()));
                        if (v != null) cdcValues.put(k, v);
                    }
                }
            }
        }

        // Pass 2: real commit, all keys.
        var realTxn = new MvccTxn(batch, reader);
        for (var k : req.getKeysList()) {
            var oc = realTxn.commit(k.toByteArray(),
                    req.getStartVersion(), req.getCommitVersion());
            if (!(oc instanceof MvccTxn.CommitCommitted)
                    && !(oc instanceof MvccTxn.CommitAlreadyCommitted)) {
                log.error("commit pass2 unexpected outcome key={} startTs={}: {}",
                        k, req.getStartVersion(), oc);
            }
        }

        cm.observeSafeTs(req.getCommitVersion());

        // CDC: publish commit events.
        if (cdcLocks != null) {
            for (var k : req.getKeysList()) {
                var lock = cdcLocks.get(k);
                if (lock == null) continue;
                Cdcpb.Row.OpType opType = switch (lock.type()) {
                    case PUT -> Cdcpb.Row.OpType.PUT;
                    case DELETE -> Cdcpb.Row.OpType.DELETE;
                    default -> null;
                };
                if (opType == null) continue;
                byte[] key = k.toByteArray();
                byte[] value = (opType == Cdcpb.Row.OpType.PUT) ? cdcValues.get(k) : null;
                cdcEventBus.publish(new CdcEvent(regionId, opType, key, value, null,
                        req.getStartVersion(), req.getCommitVersion()));
            }
        }

        return Result.ok(Kvrpcpb.CommitResponse.newBuilder()
                .setCommitVersion(req.getCommitVersion())
                .build()
                .toByteArray());
        } // close reader
    }

    // ===================================================================
    // Rollback
    // ===================================================================

    private Result applyRollback(byte[] payload, StorageEngine.WriteBatch batch)
            throws InvalidProtocolBufferException {
        var req = Kvrpcpb.BatchRollbackRequest.parseFrom(payload);
        try (var snap = engine.newSnapshot(); var reader = new MvccReader(engine, snap, true)) {
        var txn = new MvccTxn(batch, reader);

        for (var k : req.getKeysList()) {
            var oc = txn.rollback(k.toByteArray(), req.getStartVersion());
            if (oc instanceof MvccTxn.RollbackAlreadyCommitted ac) {
                return Result.ok(Kvrpcpb.BatchRollbackResponse.newBuilder()
                        .setError(Kvrpcpb.KeyError.newBuilder()
                                .setAbort("already-committed:commit_ts=" + ac.commitTs()))
                        .build()
                        .toByteArray());
            }
        }

        // CDC: publish rollback events.
        if (cdcEventBus != null && cdcEventBus.hasSubscribers(regionId)) {
            for (var k : req.getKeysList()) {
                cdcEventBus.publish(new CdcEvent(regionId, Cdcpb.Row.OpType.ROLLBACK,
                        k.toByteArray(), null, null, req.getStartVersion(), 0));
            }
        }

        return Result.ok(Kvrpcpb.BatchRollbackResponse.newBuilder().build().toByteArray());
        } // close reader
    }

    // ===================================================================
    // Pessimistic lock / rollback
    // ===================================================================

    private Result applyPessimisticLock(byte[] payload, StorageEngine.WriteBatch batch)
            throws InvalidProtocolBufferException {
        var req = Kvrpcpb.PessimisticLockRequest.parseFrom(payload);
        try (var snap = engine.newSnapshot(); var reader = new MvccReader(engine, snap, true)) {
        var txn = new MvccTxn(batch, reader);
        // SI: bump max_ts to at least for_update_ts so that any subsequent
        // prewrite for THIS txn (or another) computes min_commit_ts ≥
        // for_update_ts+1. Without this a pessimistic txn can commit at a
        // commit_ts ≤ for_update_ts, and a reader who was blocked at the
        // lock (read_ts ∈ [start_ts, for_update_ts]) would see the commit
        // when it retries — violating linearizability against the lock's
        // logical timestamp.
        if (req.getForUpdateTs() != 0) {
            cm.maxTs().observe(req.getForUpdateTs());
        }
        var resp = Kvrpcpb.PessimisticLockResponse.newBuilder();
        for (var m : req.getMutationsList()) {
            var oc = txn.acquirePessimisticLock(m.getKey().toByteArray(),
                    req.getPrimaryLock().toByteArray(),
                    req.getStartVersion(),
                    req.getForUpdateTs(),
                    req.getLockTtl());
            if (oc instanceof MvccTxn.PessimisticWriteConflict wc) {
                resp.addErrors(Kvrpcpb.KeyError.newBuilder()
                        .setConflict(Kvrpcpb.WriteConflict.newBuilder()
                                .setStartTs(wc.startTs())
                                .setConflictTs(wc.conflictCommitTs())
                                .setConflictCommitTs(wc.conflictCommitTs())
                                .setKey(m.getKey())
                                .setPrimary(req.getPrimaryLock())
                                .setReason(Kvrpcpb.WriteConflict.Reason.PessimisticRetry))
                        .build());
            } else if (oc instanceof MvccTxn.PessimisticKeyLocked kl) {
                resp.addErrors(Kvrpcpb.KeyError.newBuilder()
                        .setLocked(toLockInfo(m.getKey().toByteArray(), kl.lock()))
                        .build());
            }
        }
        return Result.ok(resp.build().toByteArray());
        } // close reader
    }

    private Result applyPessimisticRollback(byte[] payload, StorageEngine.WriteBatch batch)
            throws InvalidProtocolBufferException {
        var req = Kvrpcpb.PessimisticRollbackRequest.parseFrom(payload);
        try (var snap = engine.newSnapshot(); var reader = new MvccReader(engine, snap, true)) {
        var txn = new MvccTxn(batch, reader);
        for (var k : req.getKeysList()) {
            txn.pessimisticRollback(k.toByteArray(), req.getStartVersion(), req.getForUpdateTs());
        }
        return Result.ok(Kvrpcpb.PessimisticRollbackResponse.newBuilder().build().toByteArray());
        } // close reader
    }

    // ===================================================================
    // ResolveLock + GC
    // ===================================================================

    /**
     * Resolve every lock owned by one or more transactions inside this
     * region's range. Three input shapes, in priority order:
     *
     * <ol>
     *   <li>{@code keys} non-empty — explicit subset; resolve just those
     *       keys at {@code (start_version, commit_version)}. Used by the
     *       client when it has specifically observed those locks.</li>
     *   <li>{@code txn_infos} non-empty — batched; each entry's
     *       {@code (txn, status)} maps to (start_ts, commit_ts):
     *       {@code status > 0} commits, {@code status == 0} rolls back.
     *       The lock CF is scanned end-to-end and every match resolves.
     *       This is the path the lock resolver hits after a
     *       CheckTxnStatus verdict.</li>
     *   <li>{@code start_version} only — scan the lock CF and resolve
     *       every lock for that single start_ts at {@code commit_version}
     *       (or rollback if 0). Convenience shape used by tests.</li>
     * </ol>
     *
     * <p>The scan is full-CF for now (single-region world). Once split
     * lands, the iterator must be bounded by the region's [start_key,
     * end_key) so a stale request from a peer that doesn't own the range
     * doesn't damage another region's locks.
     */
    private Result applyResolveLock(byte[] payload, StorageEngine.WriteBatch batch)
            throws InvalidProtocolBufferException {
        var req = Kvrpcpb.ResolveLockRequest.parseFrom(payload);
        try (var snap = engine.newSnapshot(); var reader = new MvccReader(engine, snap, true)) {
        var txn = new MvccTxn(batch, reader);

        // Path 1: explicit keys.
        if (req.getKeysCount() > 0) {
            long startTs = req.getStartVersion();
            long commitTs = req.getCommitVersion();
            for (var k : req.getKeysList()) {
                if (commitTs > 0) {
                    txn.resolveLockCommit(k.toByteArray(), startTs, commitTs);
                } else {
                    txn.resolveLockRollback(k.toByteArray(), startTs);
                }
            }
            return Result.ok(Kvrpcpb.ResolveLockResponse.newBuilder().build().toByteArray());
        }

        // Path 2 + 3: build start_ts → commit_ts map; commit_ts == 0 means rollback.
        var verdict = new java.util.HashMap<Long, Long>();
        if (req.getTxnInfosCount() > 0) {
            for (var ti : req.getTxnInfosList()) {
                verdict.put(ti.getTxn(), ti.getStatus());
            }
        } else if (req.getStartVersion() != 0) {
            verdict.put(req.getStartVersion(), req.getCommitVersion());
        }
        if (verdict.isEmpty()) {
            return Result.ok(Kvrpcpb.ResolveLockResponse.newBuilder().build().toByteArray());
        }

        // Snapshot the lock CF iterator so a concurrent prewrite
        // doesn't fight us (apply is already serialized by the writer
        // lock). Collect target keys first, then resolve — never iterate
        // and mutate the same iterator.
        //
        // Bounded to keep one apply round from holding the writer lock
        // arbitrarily long. Caller re-issues until the lock CF is drained
        // (the verdict cache makes follow-up calls cheap).
        record Target(byte[] userKey, long startTs, long commitTs) {}
        var targets = new ArrayList<Target>();
        try (var lockRo = engine.newReadOptions();
             var it = engine.newIterator(StorageEngine.Cf.LOCK, lockRo)) {
            for (it.seek(new byte[]{0}); it.isValid()
                    && targets.size() < MAX_RESOLVE_LOCKS_PER_APPLY; it.next()) {
                io.github.xinfra.lab.xkv.kv.mvcc.Lock lock;
                try { lock = io.github.xinfra.lab.xkv.kv.mvcc.Lock.decode(it.value()); }
                catch (Throwable t) {
                    log.warn("ResolveLock: undecodable lock at key {}", new String(it.key()));
                    continue;
                }
                Long ct = verdict.get(lock.startTs());
                if (ct == null) continue;
                byte[] userKey = MvccKey.userKeyFromLockKey(it.key());
                targets.add(new Target(userKey, lock.startTs(), ct));
            }
        }

        for (var t : targets) {
            if (t.commitTs() > 0) {
                txn.resolveLockCommit(t.userKey(), t.startTs(), t.commitTs());
            } else {
                txn.resolveLockRollback(t.userKey(), t.startTs());
            }
        }
        return Result.ok(Kvrpcpb.ResolveLockResponse.newBuilder().build().toByteArray());
        } // close reader
    }

    /**
     * Honest CheckTxnStatus apply path — respects lock TTL.
     *
     * <p>v1's TransactionServiceImpl shortcut routed CheckTxnStatus through
     * BatchRollback, which forcibly killed every lock regardless of TTL.
     * That broke in-flight transactions and caused BankTransferTxnTest to
     * lose money. v2 wires the real {@link MvccTxn#checkTxnStatus} via a
     * dedicated apply kind.
     */
    private Result applyCheckTxnStatus(byte[] payload, StorageEngine.WriteBatch batch)
            throws InvalidProtocolBufferException {
        var req = Kvrpcpb.CheckTxnStatusRequest.parseFrom(payload);
        try (var snap = engine.newSnapshot(); var reader = new MvccReader(engine, snap, true)) {
        var txn = new MvccTxn(batch, reader);
        var oc = txn.checkTxnStatus(req.getPrimaryKey().toByteArray(),
                req.getLockTs(),
                /* currentTsMs */ req.getCurrentTs() >>> 18 /* HLC physical */);

        var resp = Kvrpcpb.CheckTxnStatusResponse.newBuilder();
        if (oc instanceof MvccTxn.CtsCommitted c) {
            resp.setCommitVersion(c.commitTs()).setAction(Kvrpcpb.Action.NoAction);
        } else if (oc instanceof MvccTxn.CtsRolledBack) {
            resp.setAction(Kvrpcpb.Action.LockNotExistRollback);
        } else if (oc instanceof MvccTxn.CtsTtlExpireRollback) {
            resp.setAction(Kvrpcpb.Action.TtlExpireRollback);
        } else if (oc instanceof MvccTxn.CtsLockNotExistRollback) {
            resp.setAction(Kvrpcpb.Action.LockNotExistRollback);
        } else if (oc instanceof MvccTxn.CtsNoAction n) {
            resp.setAction(Kvrpcpb.Action.NoAction).setLockTtl(n.remainingTtlMs());
            // Surface lock_info so callers (e.g. legacy Cleanup) can return
            // KeyError.locked instead of synthesising one from the bare TTL.
            if (n.lock() != null) {
                resp.setLockInfo(toLockInfo(req.getPrimaryKey().toByteArray(), n.lock()));
            }
        } else if (oc instanceof MvccTxn.CtsAsyncCommitPrimary ac) {
            // Don't rollback. Surface the lock so the caller can run
            // CheckSecondaryLocks on the secondaries it lists.
            resp.setAction(Kvrpcpb.Action.NoAction)
                .setLockTtl(ac.lock().ttlMs())
                .setLockInfo(toLockInfo(req.getPrimaryKey().toByteArray(), ac.lock()));
        }
        return Result.ok(resp.build().toByteArray());
        } // close reader
    }

    /**
     * TxnHeartBeat — refresh a locked transaction's TTL.
     *
     * <p>Looks up the primary's lock in LOCK CF; if it belongs to {@code
     * req.startVersion}, takes {@code max(existing.ttlMs, adviseLockTtl)}
     * and rewrites the lock. Long-running coprocessor / batch-load
     * transactions rely on this to avoid being killed by the lock
     * resolver's TTL-expire path.
     *
     * <p>Returns {@link Kvrpcpb.TxnNotFound} when the lock is gone or
     * belongs to a different start_ts — same as TiKV.
     */
    private Result applyTxnHeartBeat(byte[] payload, StorageEngine.WriteBatch batch)
            throws InvalidProtocolBufferException {
        var req = Kvrpcpb.TxnHeartBeatRequest.parseFrom(payload);
        try (var snap = engine.newSnapshot(); var reader = new MvccReader(engine, snap, true)) {
        var primary = req.getPrimaryLock().toByteArray();
        var lockOpt = reader.readLock(primary);

        if (lockOpt.isEmpty() || lockOpt.get().startTs() != req.getStartVersion()) {
            return Result.ok(Kvrpcpb.TxnHeartBeatResponse.newBuilder()
                    .setError(Kvrpcpb.KeyError.newBuilder()
                            .setTxnNotFound(Kvrpcpb.TxnNotFound.newBuilder()
                                    .setStartTs(req.getStartVersion())
                                    .setPrimaryKey(req.getPrimaryLock())))
                    .build()
                    .toByteArray());
        }

        var existing = lockOpt.get();
        long newTtl = Math.max(existing.ttlMs(), req.getAdviseLockTtl());
        if (newTtl > existing.ttlMs()) {
            var refreshed = io.github.xinfra.lab.xkv.kv.mvcc.Lock.builder()
                    .type(existing.type())
                    .primary(existing.primary())
                    .startTs(existing.startTs())
                    .ttlMs(newTtl)
                    .txnSize(existing.txnSize())
                    .minCommitTs(existing.minCommitTs())
                    .forUpdateTs(existing.forUpdateTs())
                    .useAsyncCommit(existing.useAsyncCommit())
                    .secondaries(existing.secondaries())
                    .build();
            batch.put(StorageEngine.Cf.LOCK, MvccKey.lockKey(primary), refreshed.encode());
        }

        return Result.ok(Kvrpcpb.TxnHeartBeatResponse.newBuilder()
                .setLockTtl(newTtl)
                .build()
                .toByteArray());
        } // close reader
    }

    /**
     * CheckSecondaryLocks — async-commit recovery probe.
     *
     * <p>For each requested {@code key} the apply path inspects state for
     * the supplied {@code start_version}:
     *
     * <ul>
     *   <li>Lock present at this {@code start_version}: collect it. The
     *       client uses the locks' {@code min_commit_ts} fields to derive
     *       the txn's true commit_ts when async-commit was used.</li>
     *   <li>No matching lock but WRITE CF has a non-ROLLBACK record at
     *       this {@code start_version}: the txn already committed. Record
     *       the highest commit_ts observed and return early — the client
     *       only needs one positive commit signal to ResolveLock at that
     *       commit_ts.</li>
     *   <li>WRITE CF has a ROLLBACK record: txn rolled back. Return
     *       {@code commit_ts = 0}.</li>
     *   <li>Neither lock nor any write record exists: write a ROLLBACK
     *       protectively (so a stale prewrite that arrives after
     *       resolution can't bring the txn back to life) and continue.</li>
     * </ul>
     *
     * <p>Matches TiKV's {@code CheckSecondaryLocks} command, which is the
     * load-bearing piece of async-commit recovery. Without it, the lock
     * resolver wrongly rolls back transactions that have actually
     * committed via the async-commit path, causing data loss.
     */
    private Result applyCheckSecondaryLocks(byte[] payload, StorageEngine.WriteBatch batch)
            throws InvalidProtocolBufferException {
        var req = Kvrpcpb.CheckSecondaryLocksRequest.parseFrom(payload);
        try (var snap = engine.newSnapshot(); var reader = new MvccReader(engine, snap, true)) {
        long startTs = req.getStartVersion();
        var resp = Kvrpcpb.CheckSecondaryLocksResponse.newBuilder();
        long highestCommitTs = 0;
        boolean rolledBack = false;

        for (var k : req.getKeysList()) {
            byte[] key = k.toByteArray();

            // Path 1: lock at this start_ts — collect.
            var lockOpt = reader.readLock(key);
            if (lockOpt.isPresent() && lockOpt.get().startTs() == startTs) {
                resp.addLocks(toLockInfo(key, lockOpt.get()));
                continue;
            }

            // Path 2/3: look for a write record at this start_ts.
            var w = reader.findWriteByStartTs(key, startTs);
            if (w.isPresent()) {
                if (w.get().type() == io.github.xinfra.lab.xkv.kv.mvcc.Write.Type.ROLLBACK) {
                    rolledBack = true;
                } else {
                    long ct = findCommitTsByStartTs(key, startTs, reader.snapshotReadOpts());
                    if (ct > highestCommitTs) highestCommitTs = ct;
                }
                continue;
            }

            // Path 4: no lock, no write — protective rollback.
            byte[] writeKey = io.github.xinfra.lab.xkv.kv.mvcc.MvccKey.encode(key, startTs);
            batch.put(StorageEngine.Cf.WRITE, writeKey,
                    io.github.xinfra.lab.xkv.kv.mvcc.Write.rollback(startTs).encode());
            rolledBack = true;
        }

        if (highestCommitTs > 0) {
            // Any committed key wins — the txn committed.
            resp.setCommitTs(highestCommitTs);
        } else if (rolledBack) {
            // Any rolled-back signal AND no commit record means the txn is
            // gone. Return commit_ts=0; client treats as rolled back.
            resp.setCommitTs(0);
        }
        // Else: locks list is non-empty / commit_ts unset — txn still
        // in flight; the client decides by inspecting the locks (e.g.,
        // takes max of min_commit_ts for async-commit).

        return Result.ok(resp.build().toByteArray());
        } // close reader
    }

    /**
     * Walk this user-key's WRITE CF block and return the commit_ts of the
     * first non-ROLLBACK record whose {@code startTs} matches. Returns 0
     * if none found.
     */
    private long findCommitTsByStartTs(byte[] userKey, long startTs,
                                       StorageEngine.ReadOptions ro) {
        try (var it = engine.newIterator(StorageEngine.Cf.WRITE, ro)) {
            it.seek(io.github.xinfra.lab.xkv.kv.mvcc.MvccKey.firstVersionFor(userKey));
            while (it.isValid()) {
                if (!io.github.xinfra.lab.xkv.kv.mvcc.MvccKey.userKeyEquals(it.key(), userKey)) break;
                var w = io.github.xinfra.lab.xkv.kv.mvcc.Write.decode(it.value());
                if (w.startTs() == startTs
                        && w.type() != io.github.xinfra.lab.xkv.kv.mvcc.Write.Type.ROLLBACK) {
                    return io.github.xinfra.lab.xkv.kv.mvcc.MvccKey.ts(it.key());
                }
                it.next();
            }
        }
        return 0L;
    }

    /**
     * DDL-style bulk delete for a user-key range. Drops every artefact
     * (lock, write, default CF entries) for any user-key in the range.
     * <strong>Bypasses MVCC</strong> — caller is expected to be the SQL
     * layer's DROP TABLE / TRUNCATE path that has already coordinated
     * with running transactions (e.g., via service safe-points).
     *
     * <p>Implementation: {@code deleteRange(lockKey(start), lockKey(end))}
     * on all three CFs. This works because {@link KeyCodec#encodeBytes}
     * is prefix-free: {@code encodeBytes(x)} is never a prefix of
     * {@code encodeBytes(y)} for {@code x != y}, so MVCC keys
     * ({@code encodeBytes(userKey) || ts_suffix}) for any {@code userKey}
     * in {@code [start, end)} fall within {@code [lockKey(start), lockKey(end))}.
     */
    private Result applyTxnDeleteRange(byte[] payload, StorageEngine.WriteBatch batch)
            throws InvalidProtocolBufferException {
        var req = Kvrpcpb.DeleteRangeRequest.parseFrom(payload);
        if (req.getNotifyOnly()) {
            // notify_only=true means "just notify, don't delete" — used for
            // replication notification. We have no CDC subscribers here yet,
            // so this is a no-op.
            return Result.ok(Kvrpcpb.DeleteRangeResponse.newBuilder().build().toByteArray());
        }
        byte[] start = req.getStartKey().toByteArray();
        byte[] end   = req.getEndKey().toByteArray();
        if (end.length == 0) {
            return Result.ok(Kvrpcpb.DeleteRangeResponse.newBuilder()
                    .setError("end_key is required for DeleteRange")
                    .build()
                    .toByteArray());
        }
        byte[] encodedStart = MvccKey.lockKey(start);
        byte[] encodedEnd = MvccKey.lockKey(end);
        batch.deleteRange(StorageEngine.Cf.DEFAULT, encodedStart, encodedEnd);
        batch.deleteRange(StorageEngine.Cf.WRITE, encodedStart, encodedEnd);
        batch.deleteRange(StorageEngine.Cf.LOCK, encodedStart, encodedEnd);
        return Result.ok(Kvrpcpb.DeleteRangeResponse.newBuilder().build().toByteArray());
    }

    /**
     * GC: collapse MVCC history so old versions don't accumulate forever.
     *
     * <p>For each user-key, retain the latest PUT or DELETE record whose
     * {@code commitTs ≤ safePoint} (so that a reader at safePoint still
     * gets the right answer); delete every version below it. For PUTs
     * whose value lives in DEFAULT CF (not inlined as short-value), also
     * delete the DEFAULT CF entry. Old ROLLBACK / LOCK records below the
     * cut also drop.
     *
     * <p>Records with {@code commitTs &gt; safePoint} are preserved
     * verbatim — readers at {@code start_ts &gt; safePoint} still need
     * them. The PD-side GC worker drives {@code safePoint} forward by
     * intersecting min(live txn start_ts) with all service safe-points,
     * so by the time we get here no live txn reads below {@code safePoint}.
     *
     * <p>Bounded by {@code MAX_GC_DELETES_PER_APPLY} (100 000) so a giant
     * GC pass doesn't bloat one Raft entry; the scheduler re-issues GC
     * until a pass deletes &lt; cap.
     */
    private static final int MAX_GC_DELETES_PER_APPLY = 100_000;
    private static final int MAX_RESOLVE_LOCKS_PER_APPLY = 16_384;

    private Result applyGc(byte[] payload, StorageEngine.WriteBatch batch)
            throws InvalidProtocolBufferException {
        var req = Kvrpcpb.GCRequest.parseFrom(payload);
        long safePoint = req.getSafePoint();
        if (safePoint == 0) {
            log.warn("GC apply: zero safePoint — refusing to run");
            return Result.ok(Kvrpcpb.GCResponse.newBuilder().build().toByteArray());
        }

        long inspected = 0, deletedWrite = 0, deletedDefault = 0;
        byte[] currentUserKey = null;
        boolean keptVisibleForCurrent = false;

        try (var gcRo = engine.newReadOptions();
             var it = engine.newIterator(StorageEngine.Cf.WRITE, gcRo)) {
            it.seek(new byte[]{0});
            while (it.isValid() && deletedWrite < MAX_GC_DELETES_PER_APPLY) {
                byte[] mvccKey = it.key();
                byte[] userKey = io.github.xinfra.lab.xkv.kv.mvcc.MvccKey.userKey(mvccKey);
                long commitTs = io.github.xinfra.lab.xkv.kv.mvcc.MvccKey.ts(mvccKey);
                inspected++;

                if (currentUserKey == null
                        || !java.util.Arrays.equals(currentUserKey, userKey)) {
                    // New user-key: reset state. The iterator visits commitTs
                    // descending within a user-key (because ts is bit-inverted
                    // in the encoded key), so the first version we encounter
                    // for each user-key is the highest commitTs.
                    currentUserKey = userKey;
                    keptVisibleForCurrent = false;
                }

                if (commitTs > safePoint) {
                    // Above the cut: preserve. Move on.
                    it.next();
                    continue;
                }

                io.github.xinfra.lab.xkv.kv.mvcc.Write w;
                try { w = io.github.xinfra.lab.xkv.kv.mvcc.Write.decode(it.value()); }
                catch (Throwable t) {
                    log.warn("GC: undecodable write record at userKey={} commitTs={}",
                            new String(userKey), commitTs);
                    it.next();
                    continue;
                }

                boolean isContentful =
                        w.type() == io.github.xinfra.lab.xkv.kv.mvcc.Write.Type.PUT
                                || w.type() == io.github.xinfra.lab.xkv.kv.mvcc.Write.Type.DELETE;

                if (!keptVisibleForCurrent && isContentful) {
                    // First contentful version below safePoint — keep it.
                    keptVisibleForCurrent = true;
                    it.next();
                    continue;
                }

                // Drop. Also drop the DEFAULT CF backing entry if this was a
                // non-inlined PUT.
                batch.delete(StorageEngine.Cf.WRITE, mvccKey);
                deletedWrite++;
                if (w.type() == io.github.xinfra.lab.xkv.kv.mvcc.Write.Type.PUT
                        && !w.hasShortValue()) {
                    batch.delete(StorageEngine.Cf.DEFAULT,
                            io.github.xinfra.lab.xkv.kv.mvcc.MvccKey.encode(userKey, w.startTs()));
                    deletedDefault++;
                }
                it.next();
            }
        }

        log.info("GC apply: safePoint={} inspected={} deletedWrite={} deletedDefault={}",
                safePoint, inspected, deletedWrite, deletedDefault);
        return Result.ok(Kvrpcpb.GCResponse.newBuilder().build().toByteArray());
    }

    // ===================================================================
    // Helpers
    // ===================================================================

    private static MvccTxn.Op mapOp(Kvrpcpb.Op op) {
        return switch (op) {
            case Put -> MvccTxn.Op.PUT;
            case Del -> MvccTxn.Op.DELETE;
            case Lock -> MvccTxn.Op.LOCK;
            case Insert -> MvccTxn.Op.INSERT;
            case CheckNotExists -> MvccTxn.Op.CHECK_NOT_EXISTS;
            default -> MvccTxn.Op.PUT;
        };
    }

    private static Kvrpcpb.LockInfo toLockInfo(byte[] key, io.github.xinfra.lab.xkv.kv.mvcc.Lock lock) {
        var b = Kvrpcpb.LockInfo.newBuilder()
                .setPrimaryLock(com.google.protobuf.ByteString.copyFrom(lock.primary()))
                .setLockVersion(lock.startTs())
                .setKey(com.google.protobuf.ByteString.copyFrom(key))
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
            b.addSecondaries(com.google.protobuf.ByteString.copyFrom(s));
        }
        return b.build();
    }
}
