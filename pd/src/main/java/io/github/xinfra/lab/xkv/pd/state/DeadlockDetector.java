package io.github.xinfra.lab.xkv.pd.state;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Centralised wait-for graph for distributed deadlock detection.
 *
 * <h3>Why centralised</h3>
 *
 * <p>A pessimistic-lock conflict is detected locally on the KV node
 * holding the lock: "txn A wants lock held by txn B". A deadlock spans
 * multiple txns and possibly multiple regions / stores; only a global
 * view can spot the cycle. Hence: each KV node REPORTS the local
 * wait-for edge upstream to a single elected detector (typically PD's
 * leader); the detector merges edges into one graph and runs cycle
 * detection per insertion.
 *
 * <h3>Algorithm</h3>
 *
 * <p>On {@link #addWaitFor(WaitForEdge)}, the detector inserts the edge
 * and DFS-walks from the waiter — if it can reach itself through the
 * graph, that's a cycle. The wait chain is returned (in waiter-first
 * order) so the caller can pick a victim (typically the youngest
 * {@code waiter_txn} by start_ts).
 *
 * <p>Cycle detection runs in O(V + E) per insertion. Real deployments
 * bound the graph with a TTL so a crashed client doesn't pin an edge
 * forever. {@link #cleanupExpired} purges entries older than
 * {@code entryTtlMs}.
 *
 * <h3>Concurrency</h3>
 *
 * <p>One monitor lock protects the graph. Insertions / lookups are O(V+E)
 * which is fine for thousands of in-flight pessimistic txns. For
 * Millions, partitioned-by-hash detectors are the production approach
 * (TiKV's "deadlock-region" design).
 */
public final class DeadlockDetector {
    private static final Logger log = LoggerFactory.getLogger(DeadlockDetector.class);

    /** Adjacency: waiter_txn → set of {@link WaitForEdge}s it's currently blocked on. */
    private final Map<Long, Set<WaitForEdge>> adj = new HashMap<>();
    /** Reverse-lookup for cleanup: insertion timestamp ms. */
    private final Map<WaitForEdge, Long> insertedAtMs = new HashMap<>();

    private final long entryTtlMs;
    private final java.util.function.LongSupplier nowMsSupplier;

    private final AtomicLong totalDetections = new AtomicLong();
    private final AtomicLong totalCyclesFound = new AtomicLong();

    public DeadlockDetector() { this(60_000L, System::currentTimeMillis); }

    public DeadlockDetector(long entryTtlMs, java.util.function.LongSupplier nowMs) {
        this.entryTtlMs = entryTtlMs;
        this.nowMsSupplier = nowMs;
    }

    /** Wait-for edge: {@code waiter} is blocked on a lock held by {@code holder}. */
    public record WaitForEdge(long waiterTxn, long holderTxn, byte[] key) {
        // Equality on (waiter, holder, key) — multiple keys between the same
        // pair are distinct edges (e.g., A waits for B on k1, also on k2).
        @Override public boolean equals(Object o) {
            if (!(o instanceof WaitForEdge e)) return false;
            return waiterTxn == e.waiterTxn && holderTxn == e.holderTxn
                    && java.util.Arrays.equals(key, e.key);
        }
        @Override public int hashCode() {
            int h = Long.hashCode(waiterTxn);
            h = 31 * h + Long.hashCode(holderTxn);
            h = 31 * h + java.util.Arrays.hashCode(key);
            return h;
        }
    }

    /**
     * Try to add a wait-for edge. Returns the cycle in waiter-first order
     * if adding the edge would close one; empty otherwise. When a cycle
     * is detected, the edge is NOT inserted (so the cycle stays observable
     * to a re-query, but doesn't pollute the graph).
     */
    public synchronized List<WaitForEdge> addWaitFor(WaitForEdge edge) {
        totalDetections.incrementAndGet();
        // Quick reject: self-loop (txn waits on itself) — never legal.
        if (edge.waiterTxn == edge.holderTxn) {
            totalCyclesFound.incrementAndGet();
            return List.of(edge);
        }
        // Does the holder transitively wait for the waiter? If yes,
        // adding this edge closes a cycle waiter → holder → ... → waiter.
        var path = findPath(edge.holderTxn, edge.waiterTxn);
        if (path != null) {
            totalCyclesFound.incrementAndGet();
            var fullCycle = new ArrayList<WaitForEdge>(path.size() + 1);
            fullCycle.add(edge);
            fullCycle.addAll(path);
            return fullCycle;
        }
        adj.computeIfAbsent(edge.waiterTxn, k -> new HashSet<>()).add(edge);
        insertedAtMs.put(edge, nowMsSupplier.getAsLong());
        return List.of();
    }

    /** Remove every edge whose holder == {@code txn} (the txn has released its locks). */
    public synchronized void removeHolder(long txn) {
        for (var entry : adj.entrySet()) {
            entry.getValue().removeIf(e -> {
                boolean drop = e.holderTxn == txn;
                if (drop) insertedAtMs.remove(e);
                return drop;
            });
        }
        adj.entrySet().removeIf(e -> e.getValue().isEmpty());
    }

    /** Remove every edge whose waiter == {@code txn} (the txn got its lock or aborted). */
    public synchronized void removeWaiter(long txn) {
        var edges = adj.remove(txn);
        if (edges != null) edges.forEach(insertedAtMs::remove);
    }

    /** Garbage-collect edges older than {@link #entryTtlMs} so a crashed client doesn't leak. */
    public synchronized int cleanupExpired() {
        long cutoff = nowMsSupplier.getAsLong() - entryTtlMs;
        int dropped = 0;
        var toRemove = new ArrayList<WaitForEdge>();
        for (var e : insertedAtMs.entrySet()) {
            if (e.getValue() < cutoff) toRemove.add(e.getKey());
        }
        for (var edge : toRemove) {
            var edges = adj.get(edge.waiterTxn);
            if (edges != null) edges.remove(edge);
            insertedAtMs.remove(edge);
            dropped++;
        }
        if (dropped > 0) {
            log.info("DeadlockDetector cleanup: dropped {} expired edges", dropped);
        }
        return dropped;
    }

    /** Diagnostic: total edges currently tracked. */
    public synchronized int edgeCount() { return insertedAtMs.size(); }
    public long totalDetections() { return totalDetections.get(); }
    public long totalCyclesFound() { return totalCyclesFound.get(); }

    /**
     * BFS from {@code from} to {@code to} along the wait-for graph.
     * Returns the path (edges in source-first order) if reachable, else null.
     */
    private List<WaitForEdge> findPath(long from, long to) {
        // (current_txn, edge_to_reach_it)
        record Step(long txn, WaitForEdge incomingEdge) {}
        var parent = new HashMap<Long, WaitForEdge>();
        var queue = new java.util.ArrayDeque<Long>();
        queue.add(from);
        var visited = new HashSet<Long>();
        visited.add(from);

        while (!queue.isEmpty()) {
            long cur = queue.poll();
            var outgoing = adj.get(cur);
            if (outgoing == null) continue;
            for (var edge : outgoing) {
                long next = edge.holderTxn;
                if (next == to) {
                    // Reconstruct path from `to` back to `from`.
                    var rev = new ArrayList<WaitForEdge>();
                    rev.add(edge);
                    long bt = cur;
                    while (bt != from) {
                        var inc = parent.get(bt);
                        rev.add(inc);
                        bt = inc.waiterTxn;
                    }
                    Collections.reverse(rev);
                    return rev;
                }
                if (visited.add(next)) {
                    parent.put(next, edge);
                    queue.add(next);
                }
            }
        }
        return null;
    }
}
