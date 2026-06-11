package io.github.xinfra.lab.xkv.client.region;

import io.github.xinfra.lab.xkv.proto.Metapb;

import java.util.List;
import java.util.Optional;

/**
 * Client-side region routing cache.
 *
 * <p>Two indices, kept consistent under one lock:
 * <ul>
 *   <li>{@code byId}      — region_id ↦ {@link RegionInfo}</li>
 *   <li>{@code byStartKey} — start_key ↦ {@link RegionInfo} (NavigableMap)</li>
 * </ul>
 *
 * <h3>Invariants from v1 audit</h3>
 *
 * <ul>
 *   <li><b>Epoch-aware updates.</b> v1's {@code update()} blindly overwrote
 *       cached regions. A stale PD reply with a smaller {@code (conf_ver,
 *       version)} could clobber the freshest entry. v2 compares epochs
 *       inside the lock and discards stale updates.</li>
 *
 *   <li><b>Range invalidation via {@code subMap}.</b> v1's {@code
 *       removeOverlapping} did a full table scan, O(N) per update. v2 uses
 *       {@code byStartKey.subMap(floorKey(newStart), newEnd)} for an
 *       O(log N + k) update.</li>
 *
 *   <li><b>Non-blocking leader update.</b> {@code updateLeader} is the most
 *       common write; it touches only one entry and uses CAS to avoid
 *       blocking concurrent reads.</li>
 * </ul>
 */
public interface RegionCache {

    /** Locate the region serving {@code key}; null if not in cache. */
    Optional<RegionInfo> locateKey(byte[] key);

    Optional<RegionInfo> locateRegion(long regionId);

    /** Best-known leader for a region; null if unknown. */
    Optional<Metapb.Peer> leaderForRegion(long regionId);

    /**
     * Insert or refresh a region. The implementation MUST:
     * <ol>
     *   <li>Compare epoch — if {@code newRegion.epoch} is dominated by the
     *       cached region's epoch, drop the update.</li>
     *   <li>Remove every overlapping region entry from {@code byStartKey}
     *       via {@code subMap}.</li>
     *   <li>Insert the new region under both indices.</li>
     * </ol>
     */
    void update(Metapb.Region region, Metapb.Peer leader);

    /** Update only the leader of an already-cached region. */
    void updateLeader(long regionId, Metapb.Peer leader);

    /** Drop one region (e.g. after EpochNotMatch). */
    void invalidate(long regionId);

    /** Drop everything overlapping [start, end). */
    void invalidateRange(byte[] start, byte[] end);

    /** Order-preserving scan; used by BatchGet auto-grouping. */
    List<RegionInfo> scan(byte[] startKey, byte[] endKey, int limit);

    int size();

    void clear();

    /** Snapshot of one region + best-known leader at lookup time. */
    record RegionInfo(Metapb.Region region, Metapb.Peer leader) {}
}
