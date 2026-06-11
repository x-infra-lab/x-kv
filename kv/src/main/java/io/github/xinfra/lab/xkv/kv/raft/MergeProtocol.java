package io.github.xinfra.lab.xkv.kv.raft;

/**
 * Region merge — the most fragile distributed protocol in the system.
 *
 * <p>v1's {@code MergeCoordinator} had a 30-second blanket timeout: if
 * {@code CommitMerge} did not finish in 30s the source proposed a unilateral
 * {@code RollbackMerge}. The target, meanwhile, may have already committed
 * the merge — leaving the same key range owned by two regions on different
 * stores, double-writing, and silently losing data.
 *
 * <h3>v2 protocol</h3>
 *
 * <p>Three Raft entries plus a strict ordering rule:
 *
 * <ol>
 *   <li><b>{@code PrepareMerge}</b> on source. Source freezes new writes,
 *       advances {@code commit_index} to the entry, attaches the source's
 *       full state. Source's {@code RegionEpoch.version} bumps.</li>
 *
 *   <li><b>{@code CommitMerge}</b> on target. Target verifies source's
 *       attached state matches the prepare, atomically extends its key
 *       range to cover source's range, and ingests source's data. Target's
 *       {@code RegionEpoch.version} bumps.</li>
 *
 *   <li><b>{@code FinalizeMerge}</b> on source. Only after observing the
 *       target's CommitMerge in PD's region tree (via heartbeat), source
 *       destroys itself. NEVER unilaterally on a timeout.</li>
 * </ol>
 *
 * <p>If source loses contact with the target it MUST verify with PD that
 * no CommitMerge has been applied before proposing rollback. The verify
 * step uses target's epoch as the idempotency key — if target's persisted
 * source epoch matches the one source is asking about, then commit happened
 * and rollback is forbidden.
 *
 * <p>This is one of the few areas where a TLA+ specification is genuinely
 * called for. Phase 4 will include {@code merge.tla} alongside the impl.
 */
public interface MergeProtocol {

    State state(long regionId);

    enum State {
        IDLE,
        PREPARED_AS_SOURCE,
        COMMITTING_AS_TARGET,
        COMMITTED,
        ROLLED_BACK
    }

    /** Begin a merge: source asks target to commit it. */
    java.util.concurrent.CompletableFuture<Void> prepareAsSource(long sourceId, long targetId);

    /** Target side: verify source state and commit. */
    java.util.concurrent.CompletableFuture<Void> commitAsTarget(long sourceId, long targetId);

    /** Source side: only after PD confirms target committed. */
    java.util.concurrent.CompletableFuture<Void> finalizeAsSource(long sourceId);

    /**
     * Source side: rollback. Implementation MUST query PD to confirm target
     * has not committed before proposing the rollback Raft entry.
     */
    java.util.concurrent.CompletableFuture<Void> rollbackAsSource(long sourceId);
}
