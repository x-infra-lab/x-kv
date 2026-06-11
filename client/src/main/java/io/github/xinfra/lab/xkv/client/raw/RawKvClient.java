package io.github.xinfra.lab.xkv.client.raw;

import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Raw KV client. No MVCC, no transactions. Suitable for cache-style
 * workloads (TTL keys), feature flags, ops tooling.
 *
 * <p>v1's {@code TinyKvClient} only exposed get / put / delete / scan. v2
 * adds the full TiKV raw surface: batch ops, range delete, CAS (compare-
 * and-swap), and TTL keys. Cross-region BatchGet/BatchPut is auto-grouped
 * by region, then dispatched in parallel.
 */
public interface RawKvClient extends AutoCloseable {

    Optional<byte[]> get(byte[] key);

    /** Atomic batch fetch; auto-grouped by region. */
    Map<byte[], byte[]> batchGet(List<byte[]> keys);

    void put(byte[] key, byte[] value);

    /** Put with TTL (seconds); 0 ⇒ no TTL. */
    void put(byte[] key, byte[] value, long ttlSeconds);

    void batchPut(Map<byte[], byte[]> kvs);

    void delete(byte[] key);

    void batchDelete(List<byte[]> keys);

    /** Range delete; backed by a server-side {@code DeleteRange}. */
    void deleteRange(byte[] start, byte[] end);

    List<KvPair> scan(byte[] start, byte[] end, int limit);

    List<KvPair> reverseScan(byte[] start, byte[] end, int limit);

    /**
     * Atomic compare-and-swap. Insert when {@code expected.isEmpty()}.
     * Returns {@code (succeeded, previousValue)}.
     */
    CasResult cas(byte[] key, Optional<byte[]> expected, byte[] newValue);

    @Override void close();

    record KvPair(byte[] key, byte[] value) {}
    record CasResult(boolean succeeded, Optional<byte[]> previous) {}
}
