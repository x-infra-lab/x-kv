package io.github.xinfra.lab.xkv.pd.state;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Function;

/**
 * Default {@link Tso} implementation. HLC encoding:
 * {@code (physical_ms << 18) | logical}.
 *
 * <h3>Monotonicity contract (load-bearing — see {@link Tso} doc)</h3>
 *
 * <ul>
 *   <li><strong>Constructor sets {@code currentPhysical = physicalBound + 1}</strong>
 *       (NOT {@code physicalBound}). v1 was off-by-one: a fresh leader could
 *       mint a TSO equal to a TSO already minted by the previous leader.</li>
 *   <li><strong>{@link #reloadAfterLeaderChange()} is mandatory</strong> on
 *       every leader-change callback. v1 forgot to wire it on the leader
 *       observer; new leaders ran with stale in-memory state.</li>
 *   <li><strong>Single-flight extend.</strong> Multiple concurrent allocators
 *       that hit {@code physical_bound} all wait on ONE in-flight extend
 *       proposal — never N concurrent Raft proposes.</li>
 * </ul>
 *
 * <p>Persisted state: {@code physical_bound} only. The bound is advanced via
 * a Raft proposal (the supplied {@link #extender}) before any TSO above
 * {@code physical_bound} is returned. Apply-side, the state machine takes
 * {@code max(stored, proposed)} so replay is idempotent.
 */
public final class HlcTsoOracle implements Tso {
    private static final Logger log = LoggerFactory.getLogger(HlcTsoOracle.class);

    /** Default look-ahead window persisted per Raft round-trip. */
    public static final long DEFAULT_SAVED_INTERVAL_MS = 50;
    /** Maximum window we'll ever ask for in one extend (safety cap). */
    public static final long MAX_SAVED_INTERVAL_MS = 1_000;

    private final ReentrantLock lock = new ReentrantLock();

    /**
     * Most-recently-persisted-via-Raft physical bound. Allocator may issue
     * TSOs whose physical &le; this value freely; physical &gt; this value
     * requires extending via Raft first.
     */
    private volatile long physicalBound;

    /** Cursor (physical, logical) of the next-to-issue TSO (monotonic). */
    private long currentPhysical;
    private long currentLogical;

    /** Wall-clock supplier, parameterised for tests. */
    private final java.util.function.LongSupplier wallClockMs;

    /**
     * Raft-extender callback. Given a desired upper bound (ms), proposes a
     * Raft entry that advances persisted {@code physicalBound} and resolves
     * the returned future once apply has confirmed.
     */
    private final Function<Long, CompletableFuture<Long>> extender;

    private final AtomicReference<CompletableFuture<Long>> inFlightExtend = new AtomicReference<>();

    /**
     * Backing {@link AtomicReference} replacement (avoids importing
     * {@link java.util.concurrent.atomic.AtomicReference} and keeps the
     * type local). We use a one-slot CAS-able container.
     */
    private static final class AtomicReference<T> {
        private volatile T value;
        public T get() { return value; }
        public boolean compareAndSet(T expect, T update) {
            // Best-effort CAS via synchronized — extends are rare events.
            synchronized (this) {
                if (this.value == expect) { this.value = update; return true; }
                return false;
            }
        }
        public void set(T t) { this.value = t; }
    }

    private final AtomicBoolean shutdown = new AtomicBoolean(false);
    private final long savedIntervalMs;

    public HlcTsoOracle(long initialPhysicalBound,
                        Function<Long, CompletableFuture<Long>> extender) {
        this(initialPhysicalBound, extender, System::currentTimeMillis, DEFAULT_SAVED_INTERVAL_MS);
    }

    public HlcTsoOracle(long initialPhysicalBound,
                        Function<Long, CompletableFuture<Long>> extender,
                        java.util.function.LongSupplier wallClockMs,
                        long savedIntervalMs) {
        this.physicalBound = initialPhysicalBound;
        this.currentPhysical = initialPhysicalBound + 1;     // CRITICAL +1
        this.currentLogical = 0;
        this.extender = extender;
        this.wallClockMs = wallClockMs;
        this.savedIntervalMs = savedIntervalMs;
    }

    @Override
    public long alloc(int count) {
        if (count <= 0) throw new IllegalArgumentException("count must be > 0: " + count);
        if (count > MAX_LOGICAL) throw new IllegalArgumentException("count exceeds 2^18: " + count);
        if (shutdown.get()) throw new IllegalStateException("oracle shut down");

        int attempts = 0;
        while (true) {
            long firstTs = tryAllocLocked(count);
            if (firstTs >= 0) return firstTs;
            if (++attempts > 100) {
                throw new IllegalStateException("TSO alloc failed after " + attempts + " extend attempts");
            }
            extendBound();
        }
    }

    /** Returns the first TSO if the allocation fits in the current window, else -1. */
    private long tryAllocLocked(int count) {
        lock.lock();
        try {
            long now = wallClockMs.getAsLong();
            // Track wall clock progress: bump physical (within window).
            if (now > currentPhysical) {
                if (now > physicalBound) {
                    return -1;     // need to extend
                }
                currentPhysical = now;
                currentLogical = 0;
            }
            if (currentLogical + count > MAX_LOGICAL) {
                long nextPhysical = currentPhysical + 1;
                if (nextPhysical > physicalBound) {
                    return -1;     // need to extend
                }
                currentPhysical = nextPhysical;
                currentLogical = 0;
            }
            long first = Tso.compose(currentPhysical, currentLogical + 1);
            currentLogical += count;
            return first;
        } finally {
            lock.unlock();
        }
    }

    /**
     * Drive a single in-flight extend. Concurrent callers all await the
     * same future — never propose two concurrent extends.
     */
    private void extendBound() {
        CompletableFuture<Long> existing = inFlightExtend.get();
        if (existing != null) {
            // Some other allocator is already extending; await its result.
            try { existing.join(); } catch (Throwable ignored) {}
            return;
        }
        var ours = new CompletableFuture<Long>();
        if (!inFlightExtend.compareAndSet(null, ours)) {
            // Lost the race; someone else is now extending. Await theirs.
            CompletableFuture<Long> live = inFlightExtend.get();
            if (live != null) {
                try { live.join(); } catch (Throwable ignored) {}
            }
            return;
        }

        long target;
        lock.lock();
        try {
            long now = wallClockMs.getAsLong();
            long base = Math.max(currentPhysical, now);
            target = base + savedIntervalMs;
        } finally {
            lock.unlock();
        }

        try {
            long newBound = extender.apply(target).get(5, TimeUnit.SECONDS);
            lock.lock();
            try {
                if (newBound > physicalBound) {
                    physicalBound = newBound;
                }
            } finally {
                lock.unlock();
            }
            ours.complete(newBound);
        } catch (Throwable t) {
            log.warn("TSO extend failed", t);
            ours.completeExceptionally(t);
            // Allow next caller to retry the extend.
        } finally {
            inFlightExtend.set(null);
        }
    }

    @Override public long currentPhysicalBound() { return physicalBound; }

    @Override
    public void reloadAfterLeaderChange() {
        // Cancel any stale in-flight extend from the previous leader term.
        var stale = inFlightExtend.get();
        if (stale != null) {
            stale.cancel(true);
            inFlightExtend.set(null);
        }
        lock.lock();
        try {
            currentPhysical = physicalBound + 1;
            currentLogical = 0;
        } finally {
            lock.unlock();
        }
        log.info("TSO reloaded after leader change: physicalBound={} cursor={}",
                physicalBound, currentPhysical);
    }

    /**
     * Called by the state machine on every {@code TSO} apply. Idempotent
     * under Raft replay: the bound moves only forward.
     */
    public void onPhysicalBoundApplied(long newBound) {
        lock.lock();
        try {
            if (newBound > physicalBound) {
                physicalBound = newBound;
            }
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void shutdown() {
        shutdown.set(true);
    }
}
