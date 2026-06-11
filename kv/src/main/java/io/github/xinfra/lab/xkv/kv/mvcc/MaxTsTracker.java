package io.github.xinfra.lab.xkv.kv.mvcc;

import java.util.concurrent.atomic.AtomicLong;

/**
 * Per-region in-memory tracker of the largest read timestamp ever served
 * by this leader.
 *
 * <h3>Why this exists (the v1 missing piece)</h3>
 *
 * <p>Without max-ts tracking, Snapshot Isolation can be violated even when
 * the TSO is strictly monotonic. The classic anomaly:
 *
 * <pre>
 *   1) T1 fetches start_ts=5
 *   2) R  fetches read_ts=15
 *   3) R  reads key A at ts=15 (no lock yet) → returns OLD value
 *   4) T1 prewrites A and B (locks placed)
 *   5) T1 fetches commit_ts=10  (note: monotonic TSO, so T1.commit_ts &gt; R.read_ts? NO — TSO calls happen in this order: 5, 15, 10? — that's not monotonic, so impossible)
 * </pre>
 *
 * <p>OK, with strictly monotonic TSO, T1's commit_ts &gt; R's read_ts, so R
 * CAN'T have read A before T1 prewrote AND T1's commit be invisible. Yet
 * BankTransferTxnTest empirically loses/creates money under high contention.
 * The actual culprit: <strong>follower / stale-read paths</strong> and
 * <strong>multi-key reads at the same read_ts</strong> where the second
 * read sees a fresher state than the first because the tracker isn't
 * pinning the read view.
 *
 * <p>The robust fix mirrors TiKV: every read at read_ts sets
 * {@code max_ts = max(max_ts, read_ts)}. Every prewrite then derives
 * {@code min_commit_ts = max(start_ts + 1, max_ts + 1)}. The commit step
 * validates commit_ts &gt;= lock.min_commit_ts, which guarantees no
 * "after-the-fact" commit can sneak below an already-served read_ts.
 *
 * <p>This makes the snapshot-read path AT-MOST-ONCE for any given read_ts:
 * once R has been served at read_ts=X, any txn that prewrote (or will
 * prewrite) MUST commit at commit_ts &gt; X. R's two reads at read_ts=X
 * therefore see exactly the same MVCC state.
 */
public final class MaxTsTracker {

    private final AtomicLong maxTs;

    public MaxTsTracker() { this(0L); }

    /**
     * Bootstrap from a persisted floor — typically the value reloaded from
     * {@code PerRegionRaftEngine.persistedMaxTs()} after a process restart.
     * Without this, a restarted leader starts at 0 and can serve a prewrite
     * whose commit_ts falls below a still-in-flight reader's read_ts, breaking
     * SI for that reader.
     */
    public MaxTsTracker(long initial) {
        this.maxTs = new AtomicLong(initial);
    }

    /** Bump max_ts to at least {@code ts}. Called on every snapshot read. */
    public void observe(long ts) {
        maxTs.updateAndGet(prev -> Math.max(prev, ts));
    }

    public long current() { return maxTs.get(); }

    /**
     * Compute the floor for a new prewrite's {@code min_commit_ts}. The
     * caller takes {@code max(this floor, request.min_commit_ts, start_ts+1)}.
     */
    public long minCommitTsFloor() {
        return maxTs.get() + 1;
    }
}
