package io.github.xinfra.lab.xkv.pd.state;

/**
 * Hybrid-Logical-Clock timestamp oracle.
 *
 * <p>Encodes one TSO as {@code (physical_ms << 18) | logical} so a single
 * 64-bit integer carries both a wall-clock millisecond and a 18-bit
 * intra-millisecond counter (max {@code 262_143} per ms).
 *
 * <h3>Monotonicity contract</h3>
 *
 * <p>The most fragile invariant in the whole system: <strong>a TSO returned
 * by {@code alloc} on any leader of any term must be strictly greater than
 * every TSO previously returned by any leader of any earlier term.</strong>
 *
 * <p>Implementations satisfy this by:
 * <ol>
 *   <li>Persisting a {@code physicalBound} via Raft. Every TSO ever returned
 *       is {@code <= physicalBound << 18 | MAX_LOGICAL}.</li>
 *   <li>On leader change, calling {@link #reloadAfterLeaderChange()} to
 *       advance the in-memory cursor past {@code physicalBound + 1}. The
 *       v1 implementation skipped this — never repeat that mistake.</li>
 *   <li>On construction with a non-zero saved bound, starting the cursor
 *       at {@code physicalBound + 1} (NOT {@code physicalBound}). The v1
 *       constructor was off-by-one.</li>
 * </ol>
 *
 * <h3>Single-flight extend</h3>
 *
 * <p>When the cursor approaches {@code physicalBound} multiple concurrent
 * callers must NOT each propose an extension; only one extend Raft proposal
 * may be in flight. Other callers wait on its CompletableFuture.
 */
public interface Tso {

    int LOGICAL_BITS = 18;
    long MAX_LOGICAL = (1L << LOGICAL_BITS) - 1;
    long LOGICAL_MASK = MAX_LOGICAL;

    /** Compose {@code (physical, logical)} into a single TSO. */
    static long compose(long physicalMs, long logical) {
        return (physicalMs << LOGICAL_BITS) | (logical & LOGICAL_MASK);
    }

    static long physicalPart(long ts) { return ts >>> LOGICAL_BITS; }
    static long logicalPart(long ts)  { return ts & LOGICAL_MASK; }

    /**
     * Allocate {@code count} consecutive timestamps. Returns the first; the
     * caller may use {@code [first, first+count)}. Throws if not the leader.
     */
    long alloc(int count);

    /** The current physical bound (max physical_ms ever durable in Raft). */
    long currentPhysicalBound();

    /**
     * Called by the Raft state machine when this PD node becomes leader.
     * Reloads the persisted {@code physicalBound} and advances the cursor
     * to {@code bound + 1}, guaranteeing strictly-monotone TSO across
     * leader changes.
     */
    void reloadAfterLeaderChange();

    /** Stop background extender threads and reject further allocs. */
    void shutdown();
}
