package io.github.xinfra.lab.xkv.kv.mvcc;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Supplier;

/**
 * Per-region read/write coordination for the {@link MaxTsTracker}, with
 * <strong>striped per-key latches</strong>.
 *
 * <h3>Why per-key (Phase 6 fix)</h3>
 *
 * <p>Phase 2's coarse-grained ConcurrencyManager held one {@link
 * ReentrantReadWriteLock} for the whole region: one apply round excluded
 * <em>every</em> reader, even those on unrelated keys. Throughput
 * suffered, and concurrent reads on disjoint keys still serialized for
 * no good reason.
 *
 * <p>Phase 6 sharded the latch into N stripes (default 32), keyed by
 * {@code Math.floorMod(Arrays.hashCode(key), N)}. Same-stripe key pairs
 * still serialize against writers (correctness); different-stripe pairs
 * run in parallel (perf).
 *
 * <h3>API</h3>
 *
 * <ul>
 *   <li>{@link #withReader(byte[], long, Supplier)} — point read on a
 *       single key. Acquires the read lock on that key's stripe only.</li>
 *   <li>{@link #withReader(long, Supplier)} — coarse read (e.g. scan
 *       covering an unknown set of keys). Acquires <em>every</em>
 *       stripe's read lock in id order.</li>
 *   <li>{@link #withWriter(List, Supplier)} — apply writes for a known
 *       set of keys. Acquires write locks for the distinct stripes the
 *       keys hash to, in id order (so deadlock-free).</li>
 *   <li>{@link #withWriter(Supplier)} — coarse writer (e.g. a snapshot
 *       install or full-region admin op). Acquires every stripe's write
 *       lock in id order.</li>
 * </ul>
 *
 * <h3>Lock ordering</h3>
 *
 * <p>All multi-stripe acquisitions go in ascending stripe-id order; this
 * is the only ordering rule needed since a thread never holds two locks
 * with the same id.
 */
public final class ConcurrencyManager {

    public static final int DEFAULT_STRIPES = 32;

    private final ReentrantReadWriteLock[] stripes;
    private final MaxTsTracker maxTs;
    private final AtomicLong safeTs = new AtomicLong();

    public ConcurrencyManager(MaxTsTracker maxTs) {
        this(maxTs, DEFAULT_STRIPES);
    }

    public ConcurrencyManager(MaxTsTracker maxTs, int stripeCount) {
        if (stripeCount <= 0) {
            throw new IllegalArgumentException("stripeCount > 0");
        }
        this.maxTs = maxTs;
        this.stripes = new ReentrantReadWriteLock[stripeCount];
        for (int i = 0; i < stripeCount; i++) {
            // Fair=false: high-throughput; fairness not needed since lock holds are short
            this.stripes[i] = new ReentrantReadWriteLock(false);
        }
    }

    public MaxTsTracker maxTs() { return maxTs; }
    public int stripeCount() { return stripes.length; }

    public void observeSafeTs(long ts) {
        safeTs.updateAndGet(prev -> Math.max(prev, ts));
    }

    public long safeTs() { return safeTs.get(); }

    // =====================================================================
    // Reader paths
    // =====================================================================

    /** Point reader: lock just this key's stripe. */
    public <T> T withReader(byte[] key, long readTs, Supplier<T> work) {
        var l = stripes[stripeFor(key)].readLock();
        l.lock();
        try {
            maxTs.observe(readTs);
            return work.get();
        } finally {
            l.unlock();
        }
    }

    /** Coarse reader: lock every stripe (e.g. scan / batch-get). */
    public <T> T withReader(long readTs, Supplier<T> work) {
        for (var s : stripes) s.readLock().lock();
        try {
            maxTs.observe(readTs);
            return work.get();
        } finally {
            // Release in reverse order — symmetric, still correct.
            for (int i = stripes.length - 1; i >= 0; i--) stripes[i].readLock().unlock();
        }
    }

    // =====================================================================
    // Writer paths
    // =====================================================================

    /** Multi-key writer: lock the distinct stripes for the supplied keys, in id order. */
    public <T> T withWriter(List<byte[]> keys, Supplier<T> work) {
        if (keys == null || keys.isEmpty()) {
            return withWriter(work);
        }
        var ids = distinctSortedStripes(keys);
        for (int id : ids) stripes[id].writeLock().lock();
        try {
            return work.get();
        } finally {
            for (int i = ids.length - 1; i >= 0; i--) stripes[ids[i]].writeLock().unlock();
        }
    }

    /** Coarse writer: lock every stripe (e.g. snapshot install / region admin). */
    public <T> T withWriter(Supplier<T> work) {
        for (var s : stripes) s.writeLock().lock();
        try {
            return work.get();
        } finally {
            for (int i = stripes.length - 1; i >= 0; i--) stripes[i].writeLock().unlock();
        }
    }

    // =====================================================================
    // Helpers
    // =====================================================================

    private int stripeFor(byte[] key) {
        return Math.floorMod(Arrays.hashCode(key), stripes.length);
    }

    private int[] distinctSortedStripes(List<byte[]> keys) {
        var seen = new java.util.BitSet(stripes.length);
        for (var k : keys) seen.set(stripeFor(k));
        int[] out = new int[seen.cardinality()];
        int idx = 0;
        for (int i = seen.nextSetBit(0); i >= 0; i = seen.nextSetBit(i + 1)) {
            out[idx++] = i;
        }
        return out;
    }
}
