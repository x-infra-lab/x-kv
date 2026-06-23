package io.github.xinfra.lab.xkv.kv.mvcc;

import java.nio.ByteBuffer;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

/**
 * In-memory lock table for pipelined pessimistic locks.
 *
 * <p>When a pessimistic lock is acquired via the pipelined path, the lock
 * is inserted here BEFORE the Raft proposal completes. Reads and prewrites
 * consult this table so they see pipelined locks immediately, without
 * waiting for Raft round-trip + LOCK CF persist.
 *
 * <p>TiKV uses the same design: the in-memory lock table holds pessimistic
 * locks that have been returned to the client but may not yet be durable.
 * On crash, the table is lost; prewrite detects {@code PessimisticLockNotFound}
 * and the client retries the transaction. This is correct because pessimistic
 * locks are advisory — they prevent write conflicts but are not part of the
 * commit protocol's durability guarantee.
 *
 * <p>Thread safety: all operations are lock-free via ConcurrentHashMap.
 * ByteBuffer keys use {@code wrap()} for correct equals/hashCode on the
 * underlying byte array content.
 */
public final class InMemoryLockTable {

    private final ConcurrentHashMap<ByteBuffer, Lock> table = new ConcurrentHashMap<>();

    /**
     * Insert a pipelined pessimistic lock. Returns the existing lock if one
     * is already present for this key from a different transaction.
     */
    public Optional<Lock> put(byte[] key, Lock lock) {
        var k = ByteBuffer.wrap(key.clone());
        var existing = table.putIfAbsent(k, lock);
        if (existing == null) return Optional.empty();
        if (existing.startTs() == lock.startTs()) {
            table.put(k, lock);
            return Optional.empty();
        }
        return Optional.of(existing);
    }

    /**
     * Look up a lock by key. Returns empty if no in-memory lock exists.
     */
    public Optional<Lock> get(byte[] key) {
        var lock = table.get(ByteBuffer.wrap(key));
        return Optional.ofNullable(lock);
    }

    /**
     * Remove the in-memory lock for a key, but only if it belongs to the
     * specified transaction (by startTs). No-op if the lock is absent or
     * belongs to a different transaction.
     */
    public void remove(byte[] key, long startTs) {
        var k = ByteBuffer.wrap(key);
        table.computeIfPresent(k, (bk, existing) ->
                existing.startTs() == startTs ? null : existing);
    }

    /**
     * Remove the in-memory lock after the Raft apply has persisted it to
     * the LOCK CF. The LOCK CF is now the source of truth.
     */
    public void onPersisted(byte[] key, long startTs) {
        remove(key, startTs);
    }

    public int size() { return table.size(); }

    public void clear() { table.clear(); }
}
