package io.github.xinfra.lab.xkv.pd.state;

import io.github.xinfra.lab.xkv.proto.Metapb;

import java.util.Optional;

/**
 * The single Raft-replicated state machine that backs the PD cluster.
 *
 * <h3>Hard rule (lesson from v1)</h3>
 *
 * <p><strong>State CF MUST be written ONLY by Raft apply.</strong> The v1
 * implementation had {@code regionHeartbeat} sync-write the state CF on the
 * leader and then propose a Raft entry — a crash between the two left the
 * leader's local state ahead of the Raft log; on restart the node became a
 * follower and a stale snapshot from another node overwrote the lead state,
 * silently losing region updates. Never repeat that pattern.
 *
 * <p>Every change goes through {@link #propose} (which serializes a command,
 * runs it through Raft, and applies it on every node's state machine).
 */
public interface PdStateMachine {

    // ---- Cluster bootstrap ----

    boolean isBootstrapped();

    /**
     * Bootstrap the cluster atomically: persist the first store + the first
     * region in one Raft entry. Idempotent — second call returns the existing
     * cluster.
     */
    void bootstrap(Metapb.Store firstStore, Metapb.Region firstRegion);

    Metapb.Cluster cluster();

    // ---- Stores ----

    void putStore(Metapb.Store store);
    Optional<Metapb.Store> getStore(long storeId);
    Iterable<Metapb.Store> allStores();

    // ---- Regions ----

    Optional<Metapb.Region> getRegion(long regionId);

    /** O(log N) range-tree lookup; never linear scan. */
    Optional<Metapb.Region> getRegionByKey(byte[] key);

    /**
     * Update one region. Caller passes the {@code RegionEpoch} they observed
     * in the heartbeat; the apply logic compares against the persisted epoch
     * and ignores updates whose {@code (conf_ver, version)} is dominated by
     * the current epoch.
     */
    void updateRegion(Metapb.Region region);

    Iterable<Metapb.Region> scanRegions(byte[] startKey, byte[] endKey, int limit);

    /**
     * Record the region leader reported by the KV-side heartbeat.
     * Does NOT go through raft — leader identity is volatile routing
     * state that doesn't need durability.
     */
    void updateLeader(long regionId, Metapb.Peer leader);

    /**
     * Return the last-reported leader for a region, or {@code empty} if
     * no heartbeat has been received yet.
     */
    Optional<Metapb.Peer> getLeader(long regionId);

    /** All known regions — used by the scheduler. */
    default Iterable<Metapb.Region> allRegions() {
        return scanRegions(new byte[0], null, Integer.MAX_VALUE);
    }

    // ---- Region stats (volatile, not replicated) ----

    record RegionStats(long approximateSize, long approximateKeys) {}

    default void updateRegionStats(long regionId, long approximateSize, long approximateKeys) {}

    default Optional<RegionStats> getRegionStats(long regionId) { return Optional.empty(); }

    default java.util.Map<Long, RegionStats> allRegionStats() { return java.util.Map.of(); }

    // ---- Members (PD cluster membership) ----

    record MemberInfo(long id, String name, String raftAddress, String clientAddress) {}

    default void putMember(MemberInfo member) {}

    default void removeMember(long memberId) {}

    default Optional<MemberInfo> getMember(long memberId) { return Optional.empty(); }

    default java.util.List<MemberInfo> allMembers() { return java.util.List.of(); }

    // ---- ID alloc ----

    /** Allocate {@code count} consecutive ids; advances persisted counter. */
    long allocId(int count);

    // ---- Snapshot / replay ----

    byte[] dumpSnapshot();

    void installSnapshot(byte[] snapshot);

    /** Apply one serialized command (Raft entry payload). */
    void applyCommand(byte[] command);

    /**
     * Notify the state machine that this node has just become leader.
     * Implementations must wake up TSO ({@link Tso#reloadAfterLeaderChange})
     * and refresh background workers (scheduler, safe-point advancer).
     */
    void onBecomeLeader();

    /** Symmetric to {@link #onBecomeLeader()} — pause background work. */
    void onLoseLeader();

    default int storeCount() { return 0; }
    default int regionCount() { return 0; }

    default io.github.xinfra.lab.xkv.pd.state.placement.PlacementRuleManager placementRuleManager() {
        return null;
    }

    default io.github.xinfra.lab.xkv.pd.state.keyspace.KeyspaceManager keyspaceManager() {
        return null;
    }

    default io.github.xinfra.lab.xkv.pd.state.keyspace.ResourceGroupManager resourceGroupManager() {
        return null;
    }

    void close();
}
