package io.github.xinfra.lab.xkv.kv.raft;

import io.github.xinfra.lab.xkv.kv.engine.RaftEngine;
import io.github.xinfra.lab.xkv.kv.engine.StorageEngine;
import io.github.xinfra.lab.xkv.proto.Metapb;

/**
 * One Raft peer for one region. The unit of replication.
 *
 * <p>Owns:
 * <ul>
 *   <li>{@link RaftEngine} — log + meta + applied + dedup</li>
 *   <li>The Raft node from x-raft-lib (driven via the underlying SPI)</li>
 *   <li>The apply loop that materializes Raft entries into the
 *       {@link StorageEngine} CFs</li>
 * </ul>
 *
 * <h3>Apply contract</h3>
 *
 * <p>For every Raft entry with index {@code i}, the apply loop opens ONE
 * {@link StorageEngine.WriteBatch} and writes:
 * <ol>
 *   <li>The business mutations (default / lock / write CF puts/deletes)</li>
 *   <li>The new {@code appliedIndex = i} via
 *       {@link RaftEngine#saveAppliedIndex}</li>
 *   <li>The dedup map entry via {@link RaftEngine#recordDedup} (if any)</li>
 * </ol>
 * Then writes the batch via {@code engine.write(batch, sync=true)}. ONE
 * fsync. ONE crash-consistent post-condition. This is the central v2
 * invariant.
 */
public interface RegionPeer {

    long regionId();

    Metapb.Region region();

    Metapb.Peer self();

    /** True if this peer believes it is the current leader. */
    boolean isLeader();

    /**
     * Whether this leader's {@code max_ts} has been re-synced from PD since it
     * acquired leadership. Used to gate async-commit / 1PC prewrites so a
     * newly-elected leader never derives a {@code commit_ts} below a
     * {@code read_ts} a previous leader already served (TiKV's
     * {@code is_max_ts_synced}). Defaults to {@code true} for peer
     * implementations that do not track it (no gating).
     */
    default boolean isMaxTsSynced() { return true; }

    /** True if {@link RaftEngine} has been destroyed (region merged away). */
    boolean isDestroyed();

    /** First raft log index still on disk (post-compaction lower bound). */
    long firstIndex();

    /** Highest committed-and-applied raft index. */
    long appliedIndex();

    /** Highest observed timestamp (max of all read_ts and commit_ts). */
    default long maxTs() { return 0; }

    default long currentTerm() { return 0; }

    default long votedFor() { return 0; }

    default long commitIndex() { return 0; }

    default long lastIndex() { return 0; }

    /**
     * Submit a normal proposal (KV / MVCC mutation). Caller fills a
     * {@link Proposal} and awaits its future. Future completes when the
     * entry has been committed AND applied.
     */
    java.util.concurrent.CompletableFuture<ApplyResult> propose(Proposal p);

    /** Submit an admin proposal (split / merge / conf-change). */
    java.util.concurrent.CompletableFuture<ApplyResult> proposeAdmin(AdminProposal p);

    /**
     * Linearizable-read fence. Returns a future that completes when the
     * read may safely observe state at the moment of the call.
     */
    java.util.concurrent.CompletableFuture<Void> readIndex();

    /**
     * Request leadership transfer to {@code targetPeerId}. Best-effort —
     * the raft library schedules a campaign on the target; if the target
     * is unreachable or behind on log replication the transfer silently
     * times out and the current leader keeps leadership. Callers can poll
     * {@link #isLeader} on the target to confirm.
     */
    void transferLeader(long targetPeerId);

    /**
     * Refresh this peer's in-memory region descriptor after a split or
     * change-peer landed (the apply path has already persisted the new
     * descriptor; this call propagates it to the in-memory views the
     * {@code Store}'s {@code peerForKey} index relies on).
     */
    void updateRegion(Metapb.Region newRegion);

    /**
     * Pre-stage a Raft snapshot at the current {@code applied_index} so the
     * raft library has one to ship when a follower's {@code nextIndex} falls
     * below {@code first_index}. Idempotent — calling repeatedly is fine,
     * each call replaces the staged snapshot with a fresher one.
     *
     * <p>Called by the {@code LogCompactionWorker} immediately BEFORE it
     * proposes log compaction, so the snapshot's index >= the compaction
     * target. Otherwise compaction would leave fall-behind followers
     * unrecoverable (no log, no snapshot).
     */
    void maybeGenerateSnapshot();

    /**
     * Propose a conf change (add / remove / promote-learner peer) through
     * the raft library. Returns a future that completes when the change has
     * been committed by quorum AND applied locally.
     *
     * <p>The conf change's {@code context} bytes SHOULD carry the affected
     * {@code metapb.Peer}s (see {@code kv_serverpb.ConfChangeContext}) so
     * the apply path can update the region descriptor's peer list.
     */
    java.util.concurrent.CompletableFuture<ApplyResult> proposeConfChange(
            io.github.xinfra.lab.raft.proto.Eraftpb.ConfChangeV2 cc);

    /** Stop the peer; flush in-flight applies; close engines. */
    void shutdown();

    /**
     * Return the set of peer ids that the leader considers lagging (still
     * receiving a snapshot or whose match index hasn't caught up with the
     * leader's last index). Used to populate {@code pending_peers} in the
     * region heartbeat so PD knows when a learner is safe to promote.
     * Only meaningful on the leader; followers return empty.
     */
    default java.util.Set<Long> laggingPeerIds() { return java.util.Set.of(); }

    // ---- Companion types ----

    /**
     * Per-region runtime tunables. Defaults align with KvConfig.RaftConfig.
     *
     * <p>Lives on the interface (not any single implementation) because both
     * {@link BatchRegionPeer} and its test/bench standalone factory build a
     * peer from these settings.
     */
    record Settings(int electionTick, int heartbeatTick, long heartbeatTickMs, boolean leaseBasedRead) {
        public Settings(int electionTick, int heartbeatTick, long heartbeatTickMs) {
            this(electionTick, heartbeatTick, heartbeatTickMs, true);
        }
        public static Settings defaults() { return new Settings(10, 1, 100, true); }
    }

    /**
     * Notified after a conf-change has been applied locally. Use to spawn a
     * new peer (for AddNode targeting this store) or destroy an existing one
     * (for RemoveNode of this peer).
     */
    @FunctionalInterface
    interface ChangePeerObserver {
        void onChangePeer(io.github.xinfra.lab.raft.proto.Eraftpb.ConfChangeType type,
                          Metapb.Peer peer,
                          Metapb.Region updatedRegion);
    }

    /** Opaque Raft entry payload + dedup envelope. */
    record Proposal(byte[] payload, long clientId, long requestId) {}

    record AdminProposal(AdminKind kind, byte[] payload) {}

    enum AdminKind { SPLIT, MERGE_PREPARE, MERGE_COMMIT, MERGE_ROLLBACK, CONF_CHANGE, TRANSFER_LEADER, COMPACT_LOG, GC }

    record ApplyResult(boolean success, byte[] response, String errorMessage) {
        public static ApplyResult ok(byte[] resp) { return new ApplyResult(true, resp, null); }
        public static ApplyResult err(String msg) { return new ApplyResult(false, null, msg); }
    }
}
