package io.github.xinfra.lab.xkv.kv.mvcc;

import io.github.xinfra.lab.xkv.kv.engine.StorageEngine;

import java.util.List;

/**
 * Server-side accumulator for one Percolator round of one txn.
 *
 * <p>Lifecycle: a fresh {@link MvccTxn} is constructed per Raft entry apply
 * round, with a {@link StorageEngine.WriteBatch} that the caller will flush
 * at the end of the round (one fsync per Raft entry — Inv-1).
 *
 * <h3>Two-phase prewrite (the v1 all-or-nothing fix)</h3>
 *
 * <p>Multi-mutation prewrite must be atomic: either every mutation gets a
 * lock + default-cf entry, or none. The caller MUST run
 * {@link #checkPrewrite} for every mutation first; only if every check
 * returns {@link PrewriteOutcome#ok()} does the caller proceed to
 * {@link #writePrewrite} for each. v1 mixed checks and writes in one pass
 * and left orphan locks when a later mutation conflicted.
 *
 * <h3>commit_ts validation (the v1 async-commit gap)</h3>
 *
 * <p>{@link #commit} rejects {@code commitTs <= startTs} and
 * {@code commitTs < lock.minCommitTs}. The latter is critical for async
 * commit: the resolver derives {@code commit_ts = max(min_commit_ts of
 * all keys)}; a server that committed below {@code min_commit_ts} would
 * silently break read-write atomicity.
 *
 * <h3>CheckTxnStatus race fix (the v1 secondary-rollback bug)</h3>
 *
 * <p>{@link #checkTxnStatus} returns the apply outcome rather than the
 * pre-decision: if a concurrent commit landed first, we propagate
 * {@link CheckTxnStatusOutcome.Committed}, NOT a "rolled back" verdict.
 * v1 returned the pre-decision and the lock resolver then corrupted
 * secondary regions by force-rollback.
 */
public final class MvccTxn {

    /** Mutation operation kinds. Mirrors {@code kvrpcpb.Op} but lives here
     *  so the mvcc package has no proto dependency. */
    public enum Op { PUT, DELETE, LOCK, INSERT, CHECK_NOT_EXISTS }

    private final StorageEngine.WriteBatch batch;
    private final MvccReader reader;
    private final InMemoryLockTable inMemoryLockTable;

    public MvccTxn(StorageEngine.WriteBatch batch, MvccReader reader) {
        this(batch, reader, null);
    }

    public MvccTxn(StorageEngine.WriteBatch batch, MvccReader reader,
                   InMemoryLockTable inMemoryLockTable) {
        this.batch = batch;
        this.reader = reader;
        this.inMemoryLockTable = inMemoryLockTable;
    }

    // =====================================================================
    // Prewrite (pass 1: read-only check)
    // =====================================================================

    /**
     * Check whether the txn can prewrite {@code key}.
     *
     * <p>Three failure modes:
     * <ul>
     *   <li><b>Same-txn rollback</b> — write CF has a ROLLBACK record at
     *       (key, startTs); this txn was already aborted by a prior reader.</li>
     *   <li><b>Write conflict</b> — write CF has a non-rollback record with
     *       {@code commitTs >= startTs}; the txn's snapshot is stale.</li>
     *   <li><b>Key locked</b> — lock CF has a lock from a different txn.</li>
     * </ul>
     *
     * <p>Idempotent OK: if lock CF has a lock from THIS txn (matching
     * startTs), the prewrite has already happened — return ok and skip.
     */
    public PrewriteOutcome checkPrewrite(byte[] key, long startTs, Op op) {
        // 1) Same-txn ROLLBACK record — either a standalone ROLLBACK or
        //    an overlapping rollback collapsed into another txn's write at
        //    MvccKey(key, startTs).
        var rb = reader.findWriteByStartTs(key, startTs);
        if (rb.isPresent() && rb.get().type() == Write.Type.ROLLBACK) {
            return PrewriteOutcome.selfRolledBack(key, startTs);
        }
        if (rb.isEmpty()) {
            byte[] writeKey = MvccKey.encode(key, startTs);
            byte[] raw = readerEngine().get(StorageEngine.Cf.WRITE, writeKey, reader.snapshotReadOpts());
            if (raw != null) {
                Write w = Write.decode(raw);
                if (w.hasOverlappedRollback() && w.startTs() != startTs) {
                    return PrewriteOutcome.selfRolledBack(key, startTs);
                }
            }
        }

        // 2) Write-conflict scan: single seek for latest non-ROLLBACK write + commitTs.
        var latest = reader.readLatestWriteWithTs(key);
        if (latest.isPresent()) {
            if (latest.get().commitTs() >= startTs) {
                return PrewriteOutcome.writeConflict(key, startTs, latest.get().commitTs());
            }
        }

        // 3) INSERT / CHECK_NOT_EXISTS: reject if a committed PUT already exists.
        if (op == Op.INSERT || op == Op.CHECK_NOT_EXISTS) {
            if (latest.isPresent() && latest.get().write().type() == Write.Type.PUT) {
                return PrewriteOutcome.alreadyExist(key);
            }
        }

        // 4) Lock conflict.
        var lockOpt = reader.readLock(key);
        if (lockOpt.isPresent()) {
            var lock = lockOpt.get();
            if (lock.startTs() != startTs) {
                return PrewriteOutcome.keyLocked(key, lock);
            }
            // same-txn lock — prewrite is idempotent; flag ok-but-skip-writes.
            return PrewriteOutcome.alreadyPrewritten(key, lock);
        }

        return PrewriteOutcome.ok();
    }

    /**
     * Pessimistic prewrite check.
     *
     * <p>The caller already acquired a PESSIMISTIC lock on this key via
     * {@link #acquirePessimisticLock} (which ran the write-conflict check
     * at acquire time). This method ONLY validates the existing lock state;
     * it does NOT re-run the write-conflict check, since SI is already
     * preserved by the pessimistic lock.
     *
     * <p>Outcomes:
     * <ul>
     *   <li>{@link PrewritePessimisticUpgrade}: existing PESSIMISTIC lock
     *       from this txn — caller's writePrewrite must overwrite it with
     *       a {@code PUT/DELETE/LOCK} lock.</li>
     *   <li>{@link PrewriteAlready}: existing non-PESSIMISTIC lock from this
     *       txn — already prewritten (replay); caller skips.</li>
     *   <li>{@link PrewriteKeyLocked}: lock from a different txn.</li>
     *   <li>{@link PrewritePessimisticLockNotFound}: no lock at all — the
     *       pessimistic lock was lost (TTL expire + resolver), txn must
     *       abort. We do NOT issue a self-rolled-back response: TiKV
     *       distinguishes this case so the client can return
     *       {@code PessimisticLockNotFound} and abort cleanly.</li>
     *   <li>{@link PrewriteSelfRolledBack}: a ROLLBACK record exists at
     *       this start_ts — txn was already aborted by another path.</li>
     * </ul>
     */
    public PrewriteOutcome checkPessimisticPrewrite(byte[] key, long startTs) {
        // Same-txn ROLLBACK record — standalone or overlapping-collapsed.
        var rb = reader.findWriteByStartTs(key, startTs);
        if (rb.isPresent() && rb.get().type() == Write.Type.ROLLBACK) {
            return PrewriteOutcome.selfRolledBack(key, startTs);
        }
        if (rb.isEmpty()) {
            byte[] writeKey = MvccKey.encode(key, startTs);
            byte[] raw = readerEngine().get(StorageEngine.Cf.WRITE, writeKey, reader.snapshotReadOpts());
            if (raw != null) {
                Write w = Write.decode(raw);
                if (w.hasOverlappedRollback() && w.startTs() != startTs) {
                    return PrewriteOutcome.selfRolledBack(key, startTs);
                }
            }
        }

        var lockOpt = reader.readLock(key);
        if (lockOpt.isEmpty()) {
            // Pipelined path: lock may be in-memory but not yet persisted.
            if (inMemoryLockTable != null) {
                var memLock = inMemoryLockTable.get(key);
                if (memLock.isPresent()) {
                    var ml = memLock.get();
                    if (ml.startTs() != startTs) {
                        return PrewriteOutcome.keyLocked(key, ml);
                    }
                    if (ml.type() == Lock.Type.PESSIMISTIC) {
                        return PrewriteOutcome.pessimisticUpgrade(key, ml);
                    }
                    return PrewriteOutcome.alreadyPrewritten(key, ml);
                }
            }
            return PrewriteOutcome.pessimisticLockNotFound(key, startTs);
        }
        var lock = lockOpt.get();
        if (lock.startTs() != startTs) {
            return PrewriteOutcome.keyLocked(key, lock);
        }
        if (lock.type() == Lock.Type.PESSIMISTIC) {
            return PrewriteOutcome.pessimisticUpgrade(key, lock);
        }
        // Already promoted from PESSIMISTIC to PUT/DELETE/LOCK by an
        // earlier replay — idempotent skip.
        return PrewriteOutcome.alreadyPrewritten(key, lock);
    }


    // =====================================================================
    // Prewrite (pass 2: write to batch)
    // =====================================================================

    /**
     * Stage prewrite mutations into the batch. Caller MUST have run
     * {@link #checkPrewrite} on every key in the txn first and confirmed
     * all returned ok / alreadyPrewritten.
     *
     * @param value the mutation value; ignored if {@code op != PUT && op != INSERT}
     */
    public void writePrewrite(byte[] key,
                              byte[] value,
                              Op op,
                              byte[] primary,
                              long startTs,
                              long ttlMs,
                              long txnSize,
                              long minCommitTs,
                              long forUpdateTs,
                              boolean useAsyncCommit,
                              List<byte[]> secondaries) {
        Lock.Type lockType = switch (op) {
            case PUT, INSERT -> Lock.Type.PUT;
            case DELETE -> Lock.Type.DELETE;
            case LOCK -> Lock.Type.LOCK;
            case CHECK_NOT_EXISTS -> Lock.Type.LOCK;
        };

        // The default CF always stores the value; commit-time optimization
        // hoists short values into the Write record and drops the default
        // entry. Storing unconditionally keeps prewrite + commit independent
        // (commit can run with no extra batch staging needed for the value).
        if ((op == Op.PUT || op == Op.INSERT) && value != null) {
            batch.put(StorageEngine.Cf.DEFAULT, MvccKey.encode(key, startTs), value);
        }

        // Lock record. We tuck the inline-eligible value into a short-value
        // pocket on the lock so the commit step can promote it into the
        // Write record without re-reading default CF. The serialised lock
        // does NOT carry value bytes; instead the apply path stashes the
        // value separately via stagedShortValue() (see MvccApplyHandler).
        var lock = Lock.builder()
                .type(lockType)
                .primary(primary == null ? new byte[0] : primary)
                .startTs(startTs)
                .ttlMs(ttlMs)
                .txnSize(txnSize)
                .minCommitTs(minCommitTs)
                .forUpdateTs(forUpdateTs)
                .useAsyncCommit(useAsyncCommit)
                .secondaries(secondaries)
                .build();
        batch.put(StorageEngine.Cf.LOCK, MvccKey.lockKey(key), lock.encode());
    }

    // =====================================================================
    // Commit
    // =====================================================================

    /**
     * Commit one key. Idempotent — if the lock is already gone but a
     * commit record exists, return COMMITTED with the existing commitTs.
     */
    public CommitOutcome commit(byte[] key, long startTs, long commitTs) {
        if (commitTs <= startTs) {
            return CommitOutcome.invalidCommitTs("commitTs (" + commitTs + ") <= startTs (" + startTs + ")");
        }

        var lockOpt = reader.readLock(key);
        if (lockOpt.isEmpty()) {
            // No lock → either already committed (find write at startTs) or
            // already rolled back (write at startTs is ROLLBACK).
            var w = reader.findWriteByStartTs(key, startTs);
            if (w.isPresent()) {
                if (w.get().type() == Write.Type.ROLLBACK) {
                    return CommitOutcome.alreadyRolledBack();
                }
                // Already committed; idempotent.
                return CommitOutcome.alreadyCommitted();
            }
            return CommitOutcome.txnNotFound();
        }

        var lock = lockOpt.get();
        if (lock.startTs() != startTs) {
            // Some other txn's lock — caller's view is stale.
            return CommitOutcome.lockMismatch(lock);
        }
        if (commitTs < lock.minCommitTs()) {
            return CommitOutcome.invalidCommitTs(
                    "commitTs (" + commitTs + ") < lock.minCommitTs (" + lock.minCommitTs() + ")");
        }

        // Promote the lock to a Write record. Type maps:
        //   PUT  → Write.PUT
        //   DELETE → Write.DELETE
        //   LOCK → Write.LOCK (gives a tombstone-like marker for SELECT FOR SHARE)
        //   PESSIMISTIC → cannot commit directly; apply layer must upgrade first
        Write.Type wt = switch (lock.type()) {
            case PUT -> Write.Type.PUT;
            case DELETE -> Write.Type.DELETE;
            case LOCK -> Write.Type.LOCK;
            case PESSIMISTIC -> null;
        };
        if (wt == null) {
            return CommitOutcome.lockMismatch(lock);
        }

        Write writeRecord;
        if (wt == Write.Type.PUT) {
            // Try to inline a short value to save the secondary read on Get.
            byte[] sv = readerEngine().get(StorageEngine.Cf.DEFAULT,
                    MvccKey.encode(key, startTs), reader.snapshotReadOpts());
            if (sv != null && sv.length <= Write.SHORT_VALUE_MAX_LEN) {
                writeRecord = Write.put(startTs, sv);
                // Drop the default CF entry to avoid duplication.
                batch.delete(StorageEngine.Cf.DEFAULT, MvccKey.encode(key, startTs));
            } else {
                writeRecord = Write.put(startTs);
            }
        } else if (wt == Write.Type.DELETE) {
            writeRecord = Write.delete(startTs);
        } else {
            writeRecord = Write.lock(startTs);
        }

        batch.put(StorageEngine.Cf.WRITE, MvccKey.encode(key, commitTs), writeRecord.encode());
        batch.delete(StorageEngine.Cf.LOCK, MvccKey.lockKey(key));
        return CommitOutcome.committed(commitTs);
    }

    // =====================================================================
    // Rollback
    // =====================================================================

    /**
     * Rollback one key. Idempotent — second rollback is a no-op; rollback
     * of an already-committed key returns {@link RollbackOutcome.AlreadyCommitted}.
     */
    public RollbackOutcome rollback(byte[] key, long startTs) {
        // 1) If a commit already landed on this startTs, refuse.
        var existing = reader.findWriteByStartTs(key, startTs);
        if (existing.isPresent()) {
            var w = existing.get();
            if (w.type() != Write.Type.ROLLBACK) {
                // Already committed — return the commit_ts so CheckTxnStatus
                // can propagate COMMITTED upstream (the v1 race-fix path).
                long ct = findCommitTsForStartTs(key, startTs);
                return RollbackOutcome.alreadyCommitted(ct);
            }
            // Already rolled back — idempotent ok.
            return RollbackOutcome.ok();
        }

        // 2) Overlapping rollback collapse: if another txn's non-ROLLBACK
        //    write record sits at MvccKey(key, startTs) — i.e. some txn
        //    committed at commitTs == this txn's startTs — piggyback onto
        //    that record instead of creating a separate ROLLBACK entry.
        byte[] writeKey = MvccKey.encode(key, startTs);
        byte[] existingBytes = readerEngine().get(StorageEngine.Cf.WRITE, writeKey, reader.snapshotReadOpts());
        if (existingBytes != null) {
            Write existingWrite = Write.decode(existingBytes);
            if (existingWrite.type() != Write.Type.ROLLBACK && !existingWrite.hasOverlappedRollback()) {
                batch.put(StorageEngine.Cf.WRITE, writeKey, existingWrite.withOverlappedRollback().encode());
                cleanupLockOnRollback(key, startTs);
                return RollbackOutcome.ok();
            }
            if (existingWrite.type() == Write.Type.ROLLBACK || existingWrite.hasOverlappedRollback()) {
                cleanupLockOnRollback(key, startTs);
                return RollbackOutcome.ok();
            }
        }

        // 3) No overlapping write — write a standalone ROLLBACK record.
        batch.put(StorageEngine.Cf.WRITE, writeKey, Write.rollback(startTs).encode());

        cleanupLockOnRollback(key, startTs);
        return RollbackOutcome.ok();
    }

    private void cleanupLockOnRollback(byte[] key, long startTs) {
        var lockOpt = reader.readLock(key);
        if (lockOpt.isPresent() && lockOpt.get().startTs() == startTs) {
            batch.delete(StorageEngine.Cf.LOCK, MvccKey.lockKey(key));
            var t = lockOpt.get().type();
            if (t == Lock.Type.PUT || t == Lock.Type.PESSIMISTIC) {
                batch.delete(StorageEngine.Cf.DEFAULT, MvccKey.encode(key, startTs));
            }
        }
    }

    /** Find the commitTs for a given startTs by linear scan of the userKey block. */
    public long findCommitTsForStartTs(byte[] key, long startTs) {
        var iter = readerEngine().newIterator(StorageEngine.Cf.WRITE, reader.snapshotReadOpts());
        try (iter) {
            iter.seek(MvccKey.firstVersionFor(key));
            while (iter.isValid()) {
                if (!MvccKey.userKeyEquals(iter.key(), key)) break;
                Write w = Write.decode(iter.value());
                if (w.startTs() == startTs && w.type() != Write.Type.ROLLBACK) {
                    return MvccKey.ts(iter.key());
                }
                iter.next();
            }
            return 0L;
        }
    }

    // =====================================================================
    // Pessimistic lock
    // =====================================================================

    public PessimisticLockOutcome acquirePessimisticLock(byte[] key,
                                                          byte[] primary,
                                                          long startTs,
                                                          long forUpdateTs,
                                                          long ttlMs) {
        // Reject if a ROLLBACK record at this startTs already exists.
        var rb = reader.findWriteByStartTs(key, startTs);
        if (rb.isPresent() && rb.get().type() == Write.Type.ROLLBACK) {
            return PessimisticLockOutcome.writeConflict(key, startTs, 0);
        }

        // Write conflict: any commit at commitTs >= forUpdateTs ?
        var latestOpt = reader.readLatestWriteWithTs(key);
        if (latestOpt.isPresent() && latestOpt.get().commitTs() >= forUpdateTs) {
            return PessimisticLockOutcome.writeConflict(key, startTs, latestOpt.get().commitTs());
        }

        var lockOpt = reader.readLock(key);
        if (lockOpt.isPresent()) {
            var lock = lockOpt.get();
            if (lock.startTs() != startTs) {
                return PessimisticLockOutcome.keyLocked(key, lock);
            }
            // Same txn — refresh forUpdateTs (re-lock).
        }

        var lock = Lock.builder()
                .type(Lock.Type.PESSIMISTIC)
                .primary(primary == null ? new byte[0] : primary)
                .startTs(startTs)
                .forUpdateTs(forUpdateTs)
                .ttlMs(ttlMs)
                .build();
        batch.put(StorageEngine.Cf.LOCK, MvccKey.lockKey(key), lock.encode());
        return PessimisticLockOutcome.acquired();
    }

    public void pessimisticRollback(byte[] key, long startTs, long forUpdateTs) {
        var lockOpt = reader.readLock(key);
        if (lockOpt.isPresent()
                && lockOpt.get().startTs() == startTs
                && lockOpt.get().isPessimistic()
                && lockOpt.get().forUpdateTs() <= forUpdateTs) {
            batch.delete(StorageEngine.Cf.LOCK, MvccKey.lockKey(key));
        }
    }

    // =====================================================================
    // CheckTxnStatus
    // =====================================================================

    /**
     * Determine the fate of a txn whose lock the caller observed.
     *
     * <p>If the lock has expired, write a ROLLBACK and return
     * {@link CheckTxnStatusOutcome.TtlExpireRollback} — UNLESS we discover
     * a concurrent commit landed first; in that case return
     * {@link CheckTxnStatusOutcome.Committed} instead. v1 always returned
     * "rollback" and the resolver then force-rolled-back secondaries that
     * had actually committed.
     */
    public CheckTxnStatusOutcome checkTxnStatus(byte[] primaryKey, long lockTs, long currentTsMs) {
        var lockOpt = reader.readLock(primaryKey);
        if (lockOpt.isEmpty() || lockOpt.get().startTs() != lockTs) {
            // No lock — txn already terminated. Look for write at lockTs.
            var w = reader.findWriteByStartTs(primaryKey, lockTs);
            if (w.isPresent()) {
                if (w.get().type() == Write.Type.ROLLBACK) {
                    return CheckTxnStatusOutcome.rolledBack();
                }
                long ct = findCommitTsForStartTs(primaryKey, lockTs);
                return CheckTxnStatusOutcome.committed(ct);
            }
            // Check overlapping rollback: another txn committed at commitTs==lockTs
            // and our rollback was collapsed into it.
            byte[] writeKey = MvccKey.encode(primaryKey, lockTs);
            byte[] raw = readerEngine().get(StorageEngine.Cf.WRITE, writeKey, reader.snapshotReadOpts());
            if (raw != null) {
                Write wr = Write.decode(raw);
                if (wr.hasOverlappedRollback() && wr.startTs() != lockTs) {
                    return CheckTxnStatusOutcome.rolledBack();
                }
            }
            // No lock, no write — txn was never seen. Use overlapping rollback
            // collapse for the optimistic ROLLBACK stamp.
            writeRollbackCollapsed(primaryKey, lockTs);
            return CheckTxnStatusOutcome.lockNotExistRollback();
        }

        var lock = lockOpt.get();
        // TTL check using caller's currentTsMs (PD-derived; not local wall clock).
        long lockBornAtMs = lock.startTs() >>> 18;     // HLC physical part
        long ageMs = currentTsMs - lockBornAtMs;
        if (ageMs >= lock.ttlMs()) {
            // Race-fix: re-check after the TTL verdict — if a commit has
            // already landed (e.g. raced with apply order on this region),
            // return COMMITTED rather than rolling back a committed txn.
            var commitWrite = reader.findWriteByStartTs(primaryKey, lockTs);
            if (commitWrite.isPresent() && commitWrite.get().type() != Write.Type.ROLLBACK) {
                long ct = findCommitTsForStartTs(primaryKey, lockTs);
                return CheckTxnStatusOutcome.committed(ct);
            }

            // ASYNC-COMMIT GATE: never force-rollback an async-commit primary
            // — the txn may have committed via the secondaries' min_commit_ts
            // path without this primary's WRITE CF entry being written yet.
            // Caller must escalate to CheckSecondaryLocks. (TiKV behavior.)
            if (lock.useAsyncCommit()) {
                return CheckTxnStatusOutcome.asyncCommitPrimary(lock);
            }

            // Optimistic / pessimistic primary, expired: stamp ROLLBACK
            // (collapsed if possible) and drop the lock.
            writeRollbackCollapsed(primaryKey, lockTs);
            batch.delete(StorageEngine.Cf.LOCK, MvccKey.lockKey(primaryKey));
            if (lock.type() == Lock.Type.PUT || lock.type() == Lock.Type.PESSIMISTIC) {
                batch.delete(StorageEngine.Cf.DEFAULT, MvccKey.encode(primaryKey, lockTs));
            }
            return CheckTxnStatusOutcome.ttlExpireRollback();
        }
        // Alive — caller backs off.
        return CheckTxnStatusOutcome.noAction(lock.ttlMs() - ageMs, lock);
    }

    // =====================================================================
    // Resolve lock (commit or rollback all locks for a given startTs)
    // =====================================================================

    public void resolveLockCommit(byte[] key, long startTs, long commitTs) {
        var outcome = commit(key, startTs, commitTs);
        // Handle the pessimistic-leftover case: an async-commit
        // recovery path may decide "commit at T" on a key whose
        // PESSIMISTIC lock never got upgraded to a PUT/DELETE lock
        // (e.g., the prewrite that would upgrade it crashed midway).
        // The lock can't commit cleanly — fall back to rollback so the
        // lock CF doesn't leak the entry indefinitely.
        if (outcome instanceof CommitLockMismatch m
                && m.lock().type() == Lock.Type.PESSIMISTIC) {
            rollback(key, startTs);
        }
    }

    public void resolveLockRollback(byte[] key, long startTs) {
        rollback(key, startTs);
    }

    // =====================================================================
    // Helpers
    // =====================================================================

    /** Tunnel to the reader's underlying engine. Internal. */
    private StorageEngine readerEngine() {
        return reader.engine();
    }

    /**
     * Write a ROLLBACK stamp at {@code (key, startTs)}, collapsing it into
     * an existing non-ROLLBACK write record if one sits at the same MvccKey
     * position. This prevents ROLLBACK records from accumulating in the write
     * CF and degrading conflict-check performance on hot keys.
     */
    private void writeRollbackCollapsed(byte[] key, long startTs) {
        byte[] writeKey = MvccKey.encode(key, startTs);
        byte[] existing = readerEngine().get(StorageEngine.Cf.WRITE, writeKey, reader.snapshotReadOpts());
        if (existing != null) {
            Write w = Write.decode(existing);
            if (w.type() != Write.Type.ROLLBACK && !w.hasOverlappedRollback()) {
                batch.put(StorageEngine.Cf.WRITE, writeKey, w.withOverlappedRollback().encode());
                return;
            }
            if (w.type() == Write.Type.ROLLBACK || w.hasOverlappedRollback()) {
                return;
            }
        }
        batch.put(StorageEngine.Cf.WRITE, writeKey, Write.rollback(startTs).encode());
    }

    // =====================================================================
    // Companion outcome types
    // =====================================================================

    public sealed interface PrewriteOutcome
            permits PrewriteOk, PrewriteAlready, PrewriteWriteConflict,
                    PrewriteKeyLocked, PrewriteSelfRolledBack,
                    PrewritePessimisticUpgrade, PrewritePessimisticLockNotFound,
                    PrewriteAlreadyExist {
        static PrewriteOk ok() { return new PrewriteOk(); }
        static PrewriteAlready alreadyPrewritten(byte[] key, Lock l) { return new PrewriteAlready(key, l); }
        static PrewriteWriteConflict writeConflict(byte[] key, long startTs, long conflictCt) {
            return new PrewriteWriteConflict(key, startTs, conflictCt);
        }
        static PrewriteKeyLocked keyLocked(byte[] key, Lock lock) { return new PrewriteKeyLocked(key, lock); }
        static PrewriteSelfRolledBack selfRolledBack(byte[] key, long startTs) {
            return new PrewriteSelfRolledBack(key, startTs);
        }
        static PrewritePessimisticUpgrade pessimisticUpgrade(byte[] key, Lock pessLock) {
            return new PrewritePessimisticUpgrade(key, pessLock);
        }
        static PrewritePessimisticLockNotFound pessimisticLockNotFound(byte[] key, long startTs) {
            return new PrewritePessimisticLockNotFound(key, startTs);
        }
        static PrewriteAlreadyExist alreadyExist(byte[] key) {
            return new PrewriteAlreadyExist(key);
        }
    }
    public record PrewriteOk() implements PrewriteOutcome {}
    public record PrewriteAlready(byte[] key, Lock lock) implements PrewriteOutcome {}
    public record PrewriteWriteConflict(byte[] key, long startTs, long conflictCommitTs) implements PrewriteOutcome {}
    public record PrewriteKeyLocked(byte[] key, Lock lock) implements PrewriteOutcome {}
    public record PrewriteSelfRolledBack(byte[] key, long startTs) implements PrewriteOutcome {}
    /**
     * Pessimistic-prewrite path: the existing PESSIMISTIC lock from this
     * txn is to be upgraded into a {@code PUT/DELETE/LOCK} lock by the
     * caller's writePrewrite. Caller MUST run writePrewrite even though the
     * "lock already exists" — this is the upgrade step.
     */
    public record PrewritePessimisticUpgrade(byte[] key, Lock pessimisticLock) implements PrewriteOutcome {}
    /**
     * Pessimistic-prewrite path: this txn's PESSIMISTIC lock for the key is
     * gone (never acquired or asynchronously rolled back by a TTL-expire
     * resolver). The txn must abort — committing without the lock would be
     * unsound because no write-conflict check ran for this key.
     */
    public record PrewritePessimisticLockNotFound(byte[] key, long startTs) implements PrewriteOutcome {}
    /** INSERT / CHECK_NOT_EXISTS: a committed PUT already exists for this key. */
    public record PrewriteAlreadyExist(byte[] key) implements PrewriteOutcome {}

    public sealed interface CommitOutcome
            permits CommitCommitted, CommitAlreadyCommitted, CommitAlreadyRolledBack,
                    CommitTxnNotFound, CommitLockMismatch, CommitInvalidCommitTs {
        static CommitCommitted committed(long ct) { return new CommitCommitted(ct); }
        static CommitAlreadyCommitted alreadyCommitted() { return new CommitAlreadyCommitted(); }
        static CommitAlreadyRolledBack alreadyRolledBack() { return new CommitAlreadyRolledBack(); }
        static CommitTxnNotFound txnNotFound() { return new CommitTxnNotFound(); }
        static CommitLockMismatch lockMismatch(Lock l) { return new CommitLockMismatch(l); }
        static CommitInvalidCommitTs invalidCommitTs(String why) { return new CommitInvalidCommitTs(why); }
    }
    public record CommitCommitted(long commitTs) implements CommitOutcome {}
    public record CommitAlreadyCommitted() implements CommitOutcome {}
    public record CommitAlreadyRolledBack() implements CommitOutcome {}
    public record CommitTxnNotFound() implements CommitOutcome {}
    public record CommitLockMismatch(Lock lock) implements CommitOutcome {}
    public record CommitInvalidCommitTs(String message) implements CommitOutcome {}

    public sealed interface RollbackOutcome permits RollbackOk, RollbackAlreadyCommitted {
        static RollbackOk ok() { return new RollbackOk(); }
        static RollbackAlreadyCommitted alreadyCommitted(long ct) { return new RollbackAlreadyCommitted(ct); }
    }
    public record RollbackOk() implements RollbackOutcome {}
    public record RollbackAlreadyCommitted(long commitTs) implements RollbackOutcome {}

    public sealed interface PessimisticLockOutcome
            permits PessimisticAcquired, PessimisticWriteConflict, PessimisticKeyLocked {
        static PessimisticAcquired acquired() { return new PessimisticAcquired(); }
        static PessimisticWriteConflict writeConflict(byte[] key, long startTs, long ct) {
            return new PessimisticWriteConflict(key, startTs, ct);
        }
        static PessimisticKeyLocked keyLocked(byte[] key, Lock l) { return new PessimisticKeyLocked(key, l); }
    }
    public record PessimisticAcquired() implements PessimisticLockOutcome {}
    public record PessimisticWriteConflict(byte[] key, long startTs,
            long conflictCommitTs) implements PessimisticLockOutcome {}
    public record PessimisticKeyLocked(byte[] key, Lock lock) implements PessimisticLockOutcome {}

    public sealed interface CheckTxnStatusOutcome
            permits CtsCommitted, CtsRolledBack, CtsTtlExpireRollback,
                    CtsLockNotExistRollback, CtsNoAction, CtsAsyncCommitPrimary {
        static CtsCommitted committed(long ct) { return new CtsCommitted(ct); }
        static CtsRolledBack rolledBack() { return new CtsRolledBack(); }
        static CtsTtlExpireRollback ttlExpireRollback() { return new CtsTtlExpireRollback(); }
        static CtsLockNotExistRollback lockNotExistRollback() { return new CtsLockNotExistRollback(); }
        static CtsNoAction noAction(long remainingTtlMs, Lock lock) { return new CtsNoAction(remainingTtlMs, lock); }
        static CtsAsyncCommitPrimary asyncCommitPrimary(Lock lock) { return new CtsAsyncCommitPrimary(lock); }
    }
    public record CtsCommitted(long commitTs) implements CheckTxnStatusOutcome {}
    public record CtsRolledBack() implements CheckTxnStatusOutcome {}
    public record CtsTtlExpireRollback() implements CheckTxnStatusOutcome {}
    public record CtsLockNotExistRollback() implements CheckTxnStatusOutcome {}
    public record CtsNoAction(long remainingTtlMs, Lock lock) implements CheckTxnStatusOutcome {}
    /**
     * Primary lock has {@code use_async_commit = true}: we DO NOT
     * force-rollback even if the TTL is expired, because the txn may have
     * committed via the async-commit path (commit_ts decided from
     * secondaries' min_commit_ts). The caller must escalate to
     * {@code CheckSecondaryLocks} to discover the truth.
     */
    public record CtsAsyncCommitPrimary(Lock lock) implements CheckTxnStatusOutcome {}
}
