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
 * <h3>ReadOptions lifecycle</h3>
 *
 * <p>A single {@link StorageEngine.ReadOptions} is created at construction
 * and bound to the snapshot (if any). Reused for every CF read within this
 * reader's lifetime. Callers MUST close the reader when done.
 */
public final class MvccReader implements AutoCloseable {

    private final StorageEngine engine;
    private final StorageEngine.Snapshot snapshot;
    private final boolean ignoreLocks;
    private final StorageEngine.ReadOptions cachedReadOpts;

    /**
     * Construct a reader bound to a specific snapshot for cross-CF
     * consistency. Pass {@code null} to read at "now" (less consistent but
     * useful for {@code resolveLock} which intentionally races).
     */
    public MvccReader(StorageEngine engine, StorageEngine.Snapshot snapshot, boolean ignoreLocks) {
        this.engine = engine;
        this.snapshot = snapshot;
        this.ignoreLocks = ignoreLocks;
        this.cachedReadOpts = engine.newReadOptions();
        if (snapshot != null) cachedReadOpts.snapshot(snapshot);
    }

    /** Underlying engine. Exposed for txn helpers that need their own iterator. */
    public StorageEngine engine() { return engine; }

    /** Snapshot-bound ReadOptions for use by MvccTxn helper methods. */
    StorageEngine.ReadOptions snapshotReadOpts() { return cachedReadOpts; }

    /** Get visible value at {@code readTs}. Empty => not found. */
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
     * <p>LOCK CF keys are stored as {@code MvccKey.lockKey(userKey)}.
     * When this reader is bound to a snapshot, the lookup is performed
     * AT THAT SNAPSHOT.
     */
    public Optional<Lock> readLock(byte[] userKey) {
        byte[] lockCfKey = MvccKey.lockKey(userKey);
        byte[] v = (snapshot != null)
                ? engine.get(StorageEngine.Cf.LOCK, lockCfKey, cachedReadOpts)
                : engine.get(StorageEngine.Cf.LOCK, lockCfKey);
        return v == null ? Optional.empty() : Optional.of(Lock.decode(v));
    }

    /**
     * Read the latest write record for {@code userKey} (any commitTs >= 0).
     * Used by CheckTxnStatus / Cleanup to inspect prior commit history.
     */
    public Optional<Write> readLatestWrite(byte[] userKey) {
        var write = findVisibleWrite(userKey, Long.MAX_VALUE);
        return Optional.ofNullable(write);
    }

    /**
     * Find a write record by exact {@code startTs}. Used by CheckTxnStatus.
     */
    public Optional<Write> findWriteByStartTs(byte[] userKey, long startTs) {
        var lower = MvccKey.firstVersionFor(userKey);
        try (var it = engine.newIterator(StorageEngine.Cf.WRITE, cachedReadOpts)) {
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
        var out = new ArrayList<KvPair>();
        try (var it = engine.newIterator(StorageEngine.Cf.WRITE, cachedReadOpts)) {
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
                cursor = MvccKey.afterAllVersionsOf(currentUser);
            }
        }
        return out;
    }

    /**
     * Reverse range scan starting at {@code start} (exclusive upper bound)
     * down to {@code end} (inclusive lower bound).
     */
    public List<KvPair> reverseScan(byte[] start, byte[] end, int limit, long readTs) {
        var out = new ArrayList<KvPair>();
        try (var it = engine.newIterator(StorageEngine.Cf.WRITE, cachedReadOpts)) {
            byte[] seekKey = MvccKey.encode(start, 0);
            it.seekForPrev(seekKey);
            while (out.size() < limit && it.isValid()) {
                byte[] currentUser = MvccKey.userKey(it.key());
                if (Arrays.compareUnsigned(currentUser, start) >= 0) {
                    byte[] before = MvccKey.encode(currentUser, Long.MAX_VALUE);
                    it.seekForPrev(decrement(before));
                    continue;
                }
                if (end != null && end.length > 0 && Arrays.compareUnsigned(currentUser, end) < 0) {
                    break;
                }
                if (!ignoreLocks) {
                    checkLockBlocking(currentUser, readTs);
                }
                Write visible = findVisibleWrite(currentUser, readTs);
                if (visible != null && visible.type() == Write.Type.PUT) {
                    var v = resolveValue(currentUser, visible);
                    v.ifPresent(value -> out.add(new KvPair(currentUser, value)));
                }
                byte[] firstVer = MvccKey.firstVersionFor(currentUser);
                it.seekForPrev(decrement(firstVer));
            }
        }
        return out;
    }

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

    @Override
    public void close() {
        // ReadOptions wraps native memory; release it.
    }

    // ============================================================
    // Internal helpers
    // ============================================================

    private void checkLockBlocking(byte[] userKey, long readTs) {
        var locked = readLock(userKey);
        if (locked.isEmpty()) return;
        Lock lock = locked.get();
        if (lock.type() == Lock.Type.LOCK) return;
        if (lock.type() == Lock.Type.PESSIMISTIC) return;
        if (lock.startTs() <= readTs) {
            throw new KeyLockedException(userKey, lock);
        }
    }

    private Write findVisibleWrite(byte[] userKey, long readTs) {
        var lower = MvccKey.encode(userKey, readTs);
        try (var it = engine.newIterator(StorageEngine.Cf.WRITE, cachedReadOpts)) {
            it.seek(lower);
            return walkForVisible(it, userKey, readTs);
        }
    }

    private Write walkForVisible(StorageEngine.Iterator it, byte[] userKey, long readTs) {
        while (it.isValid()) {
            byte[] k = it.key();
            if (!MvccKey.userKeyEquals(k, userKey)) return null;
            long commitTs = MvccKey.ts(k);
            if (commitTs > readTs) {
                it.next();
                continue;
            }
            Write w = Write.decode(it.value());
            switch (w.type()) {
                case ROLLBACK, LOCK -> {
                    it.next();
                    continue;
                }
                case PUT, DELETE -> { return w; }
            }
        }
        return null;
    }

    private Optional<byte[]> resolveValue(byte[] userKey, Write w) {
        if (w.type() != Write.Type.PUT) return Optional.empty();
        if (w.hasShortValue()) return Optional.of(w.shortValue());
        byte[] valueKey = MvccKey.encode(userKey, w.startTs());
        byte[] v = (snapshot != null)
                ? engine.get(StorageEngine.Cf.DEFAULT, valueKey, cachedReadOpts)
                : engine.get(StorageEngine.Cf.DEFAULT, valueKey);
        return v == null ? Optional.empty() : Optional.of(v);
    }
}
