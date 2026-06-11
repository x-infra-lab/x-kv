package io.github.xinfra.lab.xkv.client.txn;

import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * A transactional handle.
 *
 * <p>Created by {@link TxnClient#begin}. Implements {@link AutoCloseable}
 * so try-with-resources auto-rollbacks any uncommitted txn.
 *
 * <h3>Lifecycle states</h3>
 * <pre>
 *   ACTIVE ──commit────► COMMITTED
 *      │
 *      ├──rollback────► ROLLED_BACK
 *      │
 *      └──crash/timeout──► UNKNOWN  (resolved later by background workers)
 * </pre>
 *
 * <p>Reading after commit/rollback throws; multi-commit is rejected
 * idempotently. The {@link TwoPhaseCommitter} owns the actual protocol.
 */
public interface Transaction extends AutoCloseable {

    long startTs();

    // ---- Reads ----
    Optional<byte[]> get(byte[] key);

    /** Auto-grouped by region; returns map keyed by request key. */
    Map<byte[], byte[]> batchGet(List<byte[]> keys);

    Iterable<KvPair> scan(byte[] start, byte[] end, int limit);
    Iterable<KvPair> reverseScan(byte[] start, byte[] end, int limit);

    // ---- Writes (buffered locally; flushed at commit) ----
    void put(byte[] key, byte[] value);
    void delete(byte[] key);
    /** Insert; commit fails with {@code AlreadyExist} if key exists. */
    void insert(byte[] key, byte[] value);

    // ---- Pessimistic locking (SELECT ... FOR UPDATE) ----
    void lockKeysForUpdate(List<byte[]> keys);

    // ---- Two-phase commit ----

    /** Commit. Returns the chosen commit_ts or throws on failure. */
    long commit();

    /** Best-effort rollback; idempotent. */
    void rollback();

    @Override void close();

    record KvPair(byte[] key, byte[] value) {}
}
