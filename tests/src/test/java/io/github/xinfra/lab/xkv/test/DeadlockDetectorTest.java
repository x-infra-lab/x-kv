package io.github.xinfra.lab.xkv.test;

import io.github.xinfra.lab.xkv.pd.state.DeadlockDetector;
import org.junit.jupiter.api.Test;

import java.util.concurrent.atomic.AtomicLong;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Coverage for {@link DeadlockDetector}: cycle detection (direct + 3-hop),
 * self-loop, TTL cleanup, holder/waiter removal.
 */
final class DeadlockDetectorTest {

    @Test
    void noCycleOnLinearChain() {
        var d = new DeadlockDetector();
        // A → B → C (linear, no cycle)
        assertThat(d.addWaitFor(edge(1, 2))).isEmpty();
        assertThat(d.addWaitFor(edge(2, 3))).isEmpty();
        assertThat(d.edgeCount()).isEqualTo(2);
        assertThat(d.totalCyclesFound()).isZero();
    }

    @Test
    void detectsTwoTxnCycle() {
        var d = new DeadlockDetector();
        assertThat(d.addWaitFor(edge(1, 2))).isEmpty();
        // Now 2 waits on 1: forms cycle 2 → 1 → 2.
        var cycle = d.addWaitFor(edge(2, 1));
        assertThat(cycle).isNotEmpty();
        assertThat(d.totalCyclesFound()).isEqualTo(1);
        // First edge is the one that would close the cycle (waiter 2 → 1);
        // remaining edges trace the back-path through the graph.
        assertThat(cycle.get(0).waiterTxn()).isEqualTo(2);
        assertThat(cycle.get(0).holderTxn()).isEqualTo(1);
        // The cycle was REJECTED (not inserted) so the graph is unchanged.
        assertThat(d.edgeCount()).isEqualTo(1);
    }

    @Test
    void detectsThreeTxnCycle() {
        var d = new DeadlockDetector();
        assertThat(d.addWaitFor(edge(1, 2))).isEmpty();
        assertThat(d.addWaitFor(edge(2, 3))).isEmpty();
        var cycle = d.addWaitFor(edge(3, 1));
        // Cycle 3 → 1 → 2 → 3
        assertThat(cycle).hasSize(3);
        assertThat(cycle.get(0).waiterTxn()).isEqualTo(3);
    }

    @Test
    void selfLoopFlaggedAsCycle() {
        var d = new DeadlockDetector();
        var cycle = d.addWaitFor(edge(7, 7));
        assertThat(cycle).hasSize(1);
        assertThat(cycle.get(0).waiterTxn()).isEqualTo(7);
    }

    @Test
    void removeHolderClearsEdgesIntoIt() {
        var d = new DeadlockDetector();
        d.addWaitFor(edge(1, 2));
        d.addWaitFor(edge(3, 2));
        assertThat(d.edgeCount()).isEqualTo(2);
        d.removeHolder(2);
        assertThat(d.edgeCount()).isZero();
    }

    @Test
    void removeWaiterClearsEdgesFromIt() {
        var d = new DeadlockDetector();
        d.addWaitFor(edge(1, 2));
        d.addWaitFor(edge(1, 3));
        d.addWaitFor(edge(2, 3));
        assertThat(d.edgeCount()).isEqualTo(3);
        d.removeWaiter(1);
        assertThat(d.edgeCount()).isEqualTo(1);
    }

    @Test
    void cleanupExpiredDropsStaleEdges() {
        var now = new AtomicLong(1000);
        var d = new DeadlockDetector(/* ttlMs= */ 100, now::get);
        d.addWaitFor(edge(1, 2));
        d.addWaitFor(edge(3, 4));
        assertThat(d.edgeCount()).isEqualTo(2);

        // 50 ms later: nothing expires (cutoff = now-ttl = 950, both edges
        // inserted at 1000 > 950 → still alive).
        now.set(1050);
        assertThat(d.cleanupExpired()).isZero();

        // 200 ms later: cutoff = 1100, both edges inserted at 1000 < 1100 →
        // both expired.
        now.set(1200);
        assertThat(d.cleanupExpired()).isEqualTo(2);
        assertThat(d.edgeCount()).isZero();
    }

    @Test
    void multipleEdgesBetweenSamePairOnDifferentKeysAreDistinct() {
        var d = new DeadlockDetector();
        // A waits for B on k1, also on k2 — two distinct edges.
        d.addWaitFor(new DeadlockDetector.WaitForEdge(1, 2, "k1".getBytes()));
        d.addWaitFor(new DeadlockDetector.WaitForEdge(1, 2, "k2".getBytes()));
        assertThat(d.edgeCount()).isEqualTo(2);
        d.removeHolder(2);
        assertThat(d.edgeCount()).isZero();
    }

    private static DeadlockDetector.WaitForEdge edge(long waiter, long holder) {
        return new DeadlockDetector.WaitForEdge(waiter, holder, ("k" + holder).getBytes());
    }
}
