package io.github.xinfra.lab.xkv.kv.mvcc;

import io.github.xinfra.lab.xkv.kv.engine.StorageEngine;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;

/**
 * Snapshot reader against the three MVCC CFs.
 *
 * <h3>Get protocol</h3>
 *
 * <ol>
 *   <li>Read {@code lock} CF for {@code userKey}. If present and
 *       {@code lock.startTs <= readTs}, throw {@link KeyLockedException}
 *       UNLESS {@code lock.type == LOCK} (LOCK locks don't block reads).</li>
 *   <li>Seek {@code write} CF at {@code MvccKey.encode(userKey, readTs)}.
 *       That places the iterator at the first version with
 *       {@code commitTs <= readTs}. Walk forward (still inside the
 *       userKey's prefix), skipping ROLLBACK records, until the first
 *       PUT or DELETE.</li>
 *   <li>If PUT and short-value inlined: return inline.<br>
 *       If PUT and not inlined: read {@code default} CF at
 *       {@code MvccKey.encode(userKey, write.startTs)}.<br>
 *       If DELETE: return not-found.<br>
 *       If no qualifying write: return not-found.</li>
 * </ol>
 *
 * <h3>Why {@code findFirstNonRollbackAtOrAfter}, not just "first write"</h3>
 *
 * <p>v1's bug — fixed in v2 from the start: a ROLLBACK record at the same
 * userKey can have {@code commitTs > readTs} but a real PUT below it has
 * {@code commitTs >= readTs}. If you stop at the first hit and it happens
 * to be ROLLBACK you miss the conflicting PUT for write-conflict detection
 * during prewrite. The reader iterates past ROLLBACKs explicitly.
 */
public final class MvccReader {

    private final StorageEngine engine;
    private final StorageEngine.Snapshot snapshot;
    private final boolean ignoreLocks;

    /**
     * Construct a reader bound to a specific snapshot for cross-CF
     * consistency. Pass {@code null} to read at "now" (less consistent but
     * useful for {@code resolveLock} which intentionally races).
     */
    public MvccReader(StorageEngine engine, StorageEngine.Snapshot snapshot, boolean ignoreLocks) {
        this.engine = engine;
        this.snapshot = snapshot;
        this.ignoreLocks = ignoreLocks;
    }

    /** Underlying engine. Exposed for txn helpers that need their own iterator. */
    public StorageEngine engine() { return engine; }

    /** Get visible value at {@code readTs}. Empty ⇒ not found. */
    public Optional<byte[]> get(byte[] userKey, long readTs) {
        if (!ignoreLocks) {
            checkLockBlocking(userKey, readTs);
        }
        var write = findVisibleWrite(userKey, readTs);
        if (write == null) return Optional.empty();
        return resolveValue(userKey, write);
    }

    /**
     * Read the lock CF entry for {@code userKey}, if any.
     *
     * <p>When this reader is bound to a snapshot, the lookup is performed
     * AT THAT SNAPSHOT — not against live state. Without this, a reader's
     * lock CF check could see "lock cleared" while its write CF iterator is
     * still pinned to a moment in time before that clear's commit was
     * visible — yielding a stale value with no lock to retry against. This
     * is the cross-CF consistency invariant that per-txn snapshot pinning
     * is supposed to give us.
     */
    public Optional<Lock> readLock(byte[] userKey) {
        byte[] v = (snapshot != null)
                ? engine.get(StorageEngine.Cf.LOCK, userKey, readOpts())
                : engine.get(StorageEngine.Cf.LOCK, userKey);
        return v == null ? Optional.empty() : Optional.of(Lock.decode(v));
    }

    /**
     * Read the latest write record for {@code userKey} (any commitTs ≥ 0).
     * Used by CheckTxnStatus / Cleanup to inspect prior commit history.
     */
    public Optional<Write> readLatestWrite(byte[] userKey) {
        var write = findVisibleWrite(userKey, Long.MAX_VALUE);
        return Optional.ofNullable(write);
    }

    /**
     * Find a write record by exact {@code startTs}. Used by CheckTxnStatus
     * — the lock resolver looks up "did this txn commit or get rolled back"
     * by scanning the userKey's history for a record with the queried
     * startTs.
     */
    public Optional<Write> findWriteByStartTs(byte[] userKey, long startTs) {
        var ro = readOpts();
        var lower = MvccKey.firstVersionFor(userKey);
        try (var it = engine.newIterator(StorageEngine.Cf.WRITE, ro)) {
            for (it.seek(lower); it.isValid(); it.next()) {
                if (!MvccKey.userKeyEquals(it.key(), userKey)) break;
                Write w = Write.decode(it.value());
                if (w.startTs() == startTs) return Optional.of(w);
            }
        }
        return Optional.empty();
    }

    /**
     * Forward range scan starting at {@code start} (inclusive) up to
     * {@code end} (exclusive). Returns at most {@code limit} (k, v) pairs
     * visible to {@code readTs}.
     */
    public List<KvPair> scan(byte[] start, byte[] end, int limit, long readTs) {
        // Single forward sweep: iterator visits the WRITE CF in order; we
        // pick the FIRST visible (commitTs <= readTs, non-rollback) write
        // for each userKey, then leap past that userKey using a fresh seek.
        var out = new ArrayList<KvPair>();
        var ro = readOpts();
        try (var it = engine.newIterator(StorageEngine.Cf.WRITE, ro)) {
            byte[] cursor = MvccKey.encode(start, readTs);
            while (out.size() < limit) {
                it.seek(cursor);
                if (!it.isValid()) break;
                byte[] currentUser = MvccKey.userKey(it.key());
                if (end != null && end.length > 0 && Arrays.compareUnsigned(currentUser, end) >= 0) {
                    break;
                }
                if (!ignoreLocks) {
                    checkLockBlocking(currentUser, readTs);
                }
                Write visible = walkForVisible(it, currentUser, readTs);
                if (visible != null && visible.type() == Write.Type.PUT) {
                    var v = resolveValue(currentUser, visible);
                    v.ifPresent(value -> out.add(new KvPair(currentUser, value)));
                }
                // Leap past every version of currentUser to the next userKey.
                cursor = MvccKey.afterAllVersionsOf(currentUser);
            }
        }
        return out;
    }

    /**
     * Reverse range scan starting at {@code start} (exclusive upper bound)
     * down to {@code end} (inclusive lower bound). Returns at most
     * {@code limit} (k, v) pairs visible to {@code readTs}, in descending
     * key order.
     *
     * <p>Implementation: reverse-iterate the WRITE CF to discover userKeys,
     * then for each discovered key use the forward {@link #findVisibleWrite}
     * helper (same correctness path as point-get) to resolve the visible
     * version.
     */
    public List<KvPair> reverseScan(byte[] start, byte[] end, int limit, long readTs) {
        var out = new ArrayList<KvPair>();
        var ro = readOpts();
        try (var it = engine.newIterator(StorageEngine.Cf.WRITE, ro)) {
            // Position just before the exclusive upper bound (start).
            byte[] seekKey = MvccKey.encode(start, 0);
            it.seekForPrev(seekKey);
            while (out.size() < limit && it.isValid()) {
                byte[] currentUser = MvccKey.userKey(it.key());
                // If the current userKey is >= start (exclusive upper bound), skip backwards.
                if (Arrays.compareUnsigned(currentUser, start) >= 0) {
                    // Jump to just before the first version of currentUser.
                    byte[] before = MvccKey.encode(currentUser, Long.MAX_VALUE);
                    it.seekForPrev(decrement(before));
                    continue;
                }
                // If below the inclusive lower bound, stop.
                if (end != null && end.length > 0 && Arrays.compareUnsigned(currentUser, end) < 0) {
                    break;
                }
                if (!ignoreLocks) {
                    checkLockBlocking(currentUser, readTs);
                }
                // Find the visible version via the standard forward path.
                Write visible = findVisibleWrite(currentUser, readTs);
                if (visible != null && visible.type() == Write.Type.PUT) {
                    var v = resolveValue(currentUser, visible);
                    v.ifPresent(value -> out.add(new KvPair(currentUser, value)));
                }
                // Move to the previous userKey: seek to just before the first
                // version of currentUser.
                byte[] firstVer = MvccKey.firstVersionFor(currentUser);
                it.seekForPrev(decrement(firstVer));
            }
        }
        return out;
    }

    /**
     * Decrement a byte array by 1. If all bytes are 0x00, returns an empty
     * array (signals no key before this). Used to position seekForPrev just
     * before a boundary.
     */
    private static byte[] decrement(byte[] key) {
        byte[] result = key.clone();
        for (int i = result.length - 1; i >= 0; i--) {
            if (result[i] != 0) {
                result[i]--;
                return result;
            }
            result[i] = (byte) 0xFF;
        }
        return new byte[0];
    }

    public record KvPair(byte[] key, byte[] value) {}

    // ============================================================
    // Internal helpers
    // ============================================================

    private void checkLockBlocking(byte[] userKey, long readTs) {
        var locked = readLock(userKey);
        if (locked.isEmpty()) return;
        Lock lock = locked.get();
        // LOCK type doesn't block reads.
        if (lock.type() == Lock.Type.LOCK) return;
        // A pessimistic placeholder lock without a paired prewrite also
        // doesn't carry a value; treat as non-blocking for snapshot reads
        // — they'll see the most recent committed version.
        if (lock.type() == Lock.Type.PESSIMISTIC) return;
        if (lock.startTs() <= readTs) {
            throw new KeyLockedException(userKey, lock);
        }
    }

    private Write findVisibleWrite(byte[] userKey, long readTs) {
        // We deliberately do NOT use iterateLowerBound / iterateUpperBound
        // here. RocksDB Slice references the supplied byte[] by reference;
        // a short-lived ReadOptions in a try-with-resources stack frame
        // makes lifetime accounting fragile. The walk loop below already
        // bails out via userKeyEquals.
        var ro = readOpts();
        var lower = MvccKey.encode(userKey, readTs);
        try (var it = engine.newIterator(StorageEngine.Cf.WRITE, ro)) {
            it.seek(lower);
            return walkForVisible(it, userKey, readTs);
        }
    }

    /**
     * Caller has positioned {@code it} at {@code MvccKey.encode(userKey, readTs)}
     * or earlier. Walks forward through the userKey's versions, skipping
     * ROLLBACKs, until the first PUT/DELETE OR until iteration leaves the
     * userKey's prefix.
     *
     * <p>On return, {@code it}'s position is undefined (caller must reseek
     * for the next userKey).
     */
    private Write walkForVisible(StorageEngine.Iterator it, byte[] userKey, long readTs) {
        while (it.isValid()) {
            byte[] k = it.key();
            if (!MvccKey.userKeyEquals(k, userKey)) return null;
            long commitTs = MvccKey.ts(k);
            if (commitTs > readTs) {
                // Should not happen because we seeked at readTs; but defensively
                // skip forward to the userKey's actually-visible version.
                it.next();
                continue;
            }
            Write w = Write.decode(it.value());
            switch (w.type()) {
                case ROLLBACK, LOCK -> { /* invisible — keep walking */
                    it.next();
                    continue;
                }
                case PUT, DELETE -> { return w; }
            }
        }
        return null;
    }

    /**
     * Resolve a PUT's value: inline shortValue or a read against default CF.
     *
     * <p>The DEFAULT CF read uses the same snapshot as the surrounding write
     * CF lookup. MVCC values are usually immutable once written, so live
     * reads happen to return the right bytes — but GC can asynchronously
     * delete them, so a snapshot bound read is the only safe path.
     */
    private Optional<byte[]> resolveValue(byte[] userKey, Write w) {
        if (w.type() != Write.Type.PUT) return Optional.empty();
        if (w.hasShortValue()) return Optional.of(w.shortValue());
        byte[] valueKey = MvccKey.encode(userKey, w.startTs());
        byte[] v = (snapshot != null)
                ? engine.get(StorageEngine.Cf.DEFAULT, valueKey, readOpts())
                : engine.get(StorageEngine.Cf.DEFAULT, valueKey);
        return v == null ? Optional.empty() : Optional.of(v);
    }

    private StorageEngine.ReadOptions readOpts() {
        var ro = engine.newReadOptions();
        if (snapshot != null) ro.snapshot(snapshot);
        return ro;
    }

    /** Lex-next userKey: append a single 0x00 byte (always sorts strictly after). */
    private static byte[] nextUserKey(byte[] u) {
        var dst = new byte[u.length + 1];
        System.arraycopy(u, 0, dst, 0, u.length);
        dst[u.length] = 0;
        return dst;
    }
}
