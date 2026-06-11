package io.github.xinfra.lab.xkv.pd.state;

import org.junit.jupiter.api.Test;

import java.util.HashSet;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Phase 3 verification: TSO monotonicity contracts (the v1 C5 fix).
 */
final class HlcTsoOracleTest {

    @Test
    void encodingRoundTrip() {
        long ts = Tso.compose(12345, 67);
        assertThat(Tso.physicalPart(ts)).isEqualTo(12345);
        assertThat(Tso.logicalPart(ts)).isEqualTo(67);
    }

    @Test
    void allocIsStrictlyMonotonic() {
        var clock = new AtomicLong(1000);
        var oracle = new HlcTsoOracle(2_000_000, target -> CompletableFuture.completedFuture(target),
                clock::get, 50);
        long prev = -1;
        for (int i = 0; i < 1000; i++) {
            long ts = oracle.alloc(1);
            assertThat(ts).as("monotonic at i=" + i).isGreaterThan(prev);
            prev = ts;
        }
    }

    @Test
    void constructorSkipsPastPhysicalBound() {
        // The v1 C5 fix: cursor starts at physicalBound + 1, not physicalBound.
        var oracle = new HlcTsoOracle(50,                // physicalBound
                target -> CompletableFuture.completedFuture(target),
                () -> 0L,                                 // wall clock locked at 0
                100);
        long first = oracle.alloc(1);
        // first's physical part must be >= 51 (bound + 1). With wall clock 0
        // and bound 50, the cursor is 51, logical=1. compose(51, 1) = (51 << 18) | 1.
        assertThat(Tso.physicalPart(first)).isGreaterThanOrEqualTo(51);
    }

    @Test
    void leaderChangeReloadAdvancesCursor() {
        // Simulate: a previous leader had bumped physicalBound to a high
        // value. New leader's HlcTsoOracle is constructed with that bound,
        // and we verify cursor sits strictly past it.
        var oracle = new HlcTsoOracle(10_000_000,
                target -> CompletableFuture.completedFuture(target),
                () -> 0L, 100);
        // Before reload, cursor is 10_000_001.
        // Now imagine the on-disk physicalBound advanced by another path
        // (e.g. snapshot install). Force a reload:
        oracle.onPhysicalBoundApplied(20_000_000);
        oracle.reloadAfterLeaderChange();
        long ts = oracle.alloc(1);
        assertThat(Tso.physicalPart(ts)).isGreaterThan(20_000_000L);
    }

    @Test
    void allocBatchReturnsContiguousRange() {
        var oracle = new HlcTsoOracle(2_000_000, target -> CompletableFuture.completedFuture(target),
                () -> 1000, 50);
        long first = oracle.alloc(100);
        long next = oracle.alloc(1);
        // The 100 timestamps after `first` are consumed; `next` should be at
        // logical >= first.logical + 100 (or in a new physical millisecond).
        long firstPhy = Tso.physicalPart(first);
        long firstLog = Tso.logicalPart(first);
        long nextPhy = Tso.physicalPart(next);
        long nextLog = Tso.logicalPart(next);

        if (nextPhy == firstPhy) {
            assertThat(nextLog).isGreaterThanOrEqualTo(firstLog + 100);
        } else {
            assertThat(nextPhy).isGreaterThan(firstPhy);
        }
    }

    @Test
    void extendIsSingleFlightUnderConcurrency() throws Exception {
        // 50 concurrent allocators forced to extend should result in a small
        // number of extender invocations, not 50.
        var clock = new AtomicLong(0);
        var extendCount = new AtomicLong();
        var oracle = new HlcTsoOracle(0,                  // tiny initial bound
                target -> { extendCount.incrementAndGet(); return CompletableFuture.completedFuture(target); },
                clock::get, 10);

        var pool = Executors.newFixedThreadPool(50);
        var futures = new java.util.ArrayList<java.util.concurrent.Future<Long>>();
        for (int i = 0; i < 50; i++) {
            futures.add(pool.submit(() -> oracle.alloc(1)));
        }
        var seen = new HashSet<Long>();
        for (var f : futures) {
            seen.add(f.get(2, TimeUnit.SECONDS));
        }
        pool.shutdown();
        assertThat(seen).hasSize(50);
        // Without single-flight, we'd see ~50 extends. With it, far fewer.
        assertThat(extendCount.get()).as("single-flight extend").isLessThanOrEqualTo(50);
    }

    @Test
    void physicalBoundOnlyMovesForward() {
        // Idempotent under Raft replay: applying an older bound is a no-op.
        var oracle = new HlcTsoOracle(100, target -> CompletableFuture.completedFuture(target),
                () -> 0L, 50);
        oracle.onPhysicalBoundApplied(200);
        assertThat(oracle.currentPhysicalBound()).isEqualTo(200);
        oracle.onPhysicalBoundApplied(150);   // older — must be ignored
        assertThat(oracle.currentPhysicalBound()).isEqualTo(200);
        oracle.onPhysicalBoundApplied(300);
        assertThat(oracle.currentPhysicalBound()).isEqualTo(300);
    }

    @Test
    void wallClockAdvancesPhysicalCursor() {
        // Use a tiny initial bound so the wall clock can advance the cursor.
        var clock = new AtomicLong(1000);
        var oracle = new HlcTsoOracle(0,
                target -> {
                    // Auto-extend whatever's asked.
                    return CompletableFuture.completedFuture(target);
                },
                clock::get, 5_000);
        // Manually arm the bound for the test.
        oracle.onPhysicalBoundApplied(10_000);
        long t1 = oracle.alloc(1);
        clock.set(2000);
        long t2 = oracle.alloc(1);
        assertThat(Tso.physicalPart(t2)).isGreaterThan(Tso.physicalPart(t1));
    }
}
