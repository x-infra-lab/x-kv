package io.github.xinfra.lab.xkv.client.txn;

import com.google.protobuf.ByteString;
import io.github.xinfra.lab.xkv.proto.Kvrpcpb.Mutation;

import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

/**
 * Executes one transaction's commit pipeline.
 *
 * <p>v1's {@code Transaction.commit} mixed protocol orchestration with
 * region routing, ExecutorService management, and lock-resolver fallback —
 * a 700-line method with multiple subtle bugs (commit primary failure
 * mishandled, region-split-during-commit silently dropped). v2 hoists this
 * into a dedicated component with one job: drive the protocol.
 *
 * <h3>Three commit paths</h3>
 * <ol>
 *   <li><b>1PC.</b> Single-region txn opts in via {@code try_one_pc}; server
 *       returns {@code one_pc_commit_ts} on Prewrite and the client never
 *       issues Commit.</li>
 *
 *   <li><b>Async commit.</b> All keys' Prewrite returns {@code min_commit_ts};
 *       the txn's commit_ts is {@code max(min_commit_ts)}; the client may
 *       declare success as soon as the primary's Prewrite is durable.
 *       Secondaries commit in the background, lock resolver tolerates
 *       inconsistencies.</li>
 *
 *   <li><b>Standard 2PC.</b> Prewrite (primary first, then secondaries),
 *       fetch commit_ts from PD, Commit primary (decision point), then
 *       async Commit secondaries.</li>
 * </ol>
 *
 * <h3>Commit-state machine (the v1 fix)</h3>
 *
 * <p>The committer exposes a strict three-state result:
 * <ul>
 *   <li>{@link CommitState#COMMITTED} — primary's Commit response observed</li>
 *   <li>{@link CommitState#ROLLED_BACK} — primary's Commit returned
 *       alreadyRolledBack OR Prewrite failed before primary touched</li>
 *   <li>{@link CommitState#UNKNOWN} — Commit primary RPC failed in a way
 *       that does not distinguish "wasn't applied" from "was applied but
 *       response lost". The caller reports UNKNOWN to the user; a background
 *       resolver retries.</li>
 * </ul>
 *
 * <p>v1 collapsed UNKNOWN into ROLLED_BACK and lost transactions.
 *
 * <h3>Region-split during commit (the other v1 fix)</h3>
 *
 * <p>If a Prewrite or Commit returns EpochNotMatch the committer re-fetches
 * the new region map from PD, re-groups the affected keys against the new
 * regions, and resumes — never silently drops a key like v1 did.
 */
public interface TwoPhaseCommitter {

    /** Submit the txn for commit. */
    CompletableFuture<CommitResult> commit(TxnContext ctx, List<Mutation> mutations);

    enum CommitState { COMMITTED, ROLLED_BACK, UNKNOWN }

    record CommitResult(CommitState state, long commitTs, String message) {
        public static CommitResult committed(long ts)  { return new CommitResult(CommitState.COMMITTED, ts, null); }
        public static CommitResult rolledBack(String why) { return new CommitResult(CommitState.ROLLED_BACK, 0, why); }
        public static CommitResult unknown(String why) { return new CommitResult(CommitState.UNKNOWN, 0, why); }
    }

    /**
     * Per-txn parameters carried into the committer. Kept narrow on purpose —
     * the committer is stateless besides this context.
     */
    record TxnContext(
            long startTs,
            long lockTtlMs,
            byte[] primaryKey,         // null ⇒ committer picks first sorted
            boolean tryOnePc,
            boolean useAsyncCommit,
            long maxCommitTs,
            long forUpdateTs,          // 0 ⇒ optimistic
            int txnSize,
            Set<ByteString> pessimisticLockedKeys) {}
}
