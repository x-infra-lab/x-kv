package io.github.xinfra.lab.xkv.client.region;

import io.github.xinfra.lab.xkv.client.config.ClientConfig;
import io.github.xinfra.lab.xkv.client.pd.PdClient;
import io.github.xinfra.lab.xkv.proto.Metapb;
import io.github.xinfra.lab.xkv.proto.Pdpb;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.NavigableMap;
import java.util.Optional;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Default {@link RegionCache} implementation, backed by a PD client.
 *
 * <p>Two indices kept consistent under one writer lock:
 * <ul>
 *   <li>{@code byId}      — region_id ↦ {@link RegionInfo}</li>
 *   <li>{@code byStartKey} — start_key ↦ {@link RegionInfo} (TreeMap)</li>
 * </ul>
 *
 * <p>v1 fixes encoded here:
 * <ul>
 *   <li><b>Epoch dominance check</b> — a region update with an older
 *       {@code (conf_ver, version)} is dropped. Prevents stale PD replies
 *       from clobbering a fresher cache.</li>
 *   <li><b>O(log N + k) range invalidation</b> via {@code TreeMap.subMap}
 *       — replaces v1's full table scan.</li>
 * </ul>
 *
 * <p>Phase 5 follow-ups: TTL-based positive cache, negative cache, lazy
 * scan-region prefetch.
 */
public final class RegionCacheImpl implements RegionCache {
    private static final Logger log = LoggerFactory.getLogger(RegionCacheImpl.class);

    private final PdClient pdClient;
    private final ClientConfig.RegionCacheConfig cfg;

    private final ReentrantReadWriteLock idxLock = new ReentrantReadWriteLock();
    private final ConcurrentHashMap<Long, RegionInfo> byId = new ConcurrentHashMap<>();
    private final NavigableMap<byte[], RegionInfo> byStartKey =
            new TreeMap<>(java.util.Arrays::compareUnsigned);

    public RegionCacheImpl(PdClient pdClient, ClientConfig.RegionCacheConfig cfg) {
        this.pdClient = pdClient;
        this.cfg = cfg;
    }

    @Override
    public Optional<RegionInfo> locateKey(byte[] key) {
        idxLock.readLock().lock();
        try {
            var info = lookupLocked(key);
            if (info != null) return Optional.of(info);
        } finally {
            idxLock.readLock().unlock();
        }
        // Cache miss: ask PD.
        return loadFromPd(key);
    }

    @Override
    public Optional<RegionInfo> locateRegion(long regionId) {
        var i = byId.get(regionId);
        if (i != null) return Optional.of(i);
        // Lazy refetch by id.
        try {
            var resp = pdClient.blockingStub().getRegionByID(Pdpb.GetRegionByIDRequest.newBuilder()
                    .setRegionId(regionId).build());
            if (!resp.hasRegion()) return Optional.empty();
            var leader = resp.hasLeader()
                    ? resp.getLeader() : Metapb.Peer.getDefaultInstance();
            var info = new RegionInfo(resp.getRegion(), leader);
            update(info.region(), info.leader());
            return Optional.of(info);
        } catch (io.grpc.StatusRuntimeException e) {
            log.warn("locateRegion id={} from PD failed, switching leader: {}", regionId, e.getMessage());
            pdClient.switchLeader();
            return Optional.empty();
        } catch (Throwable t) {
            throw new RuntimeException("locateRegion PD call failed for id=" + regionId, t);
        }
    }

    @Override
    public Optional<Metapb.Peer> leaderForRegion(long regionId) {
        var i = byId.get(regionId);
        return i == null ? Optional.empty() : Optional.of(i.leader());
    }

    @Override
    public void update(Metapb.Region region, Metapb.Peer leader) {
        idxLock.writeLock().lock();
        try {
            var prev = byId.get(region.getId());
            if (prev != null && !dominates(region.getRegionEpoch(), prev.region().getRegionEpoch())) {
                // Stale; drop.
                return;
            }
            // Range may have changed: drop overlapping entries (subMap-based).
            byte[] newStart = region.getStartKey().toByteArray();
            byte[] newEnd = region.getEndKey().toByteArray();
            invalidateOverlapLocked(newStart, newEnd);

            var info = new RegionInfo(region, leader == null ? Metapb.Peer.getDefaultInstance() : leader);
            byId.put(region.getId(), info);
            byStartKey.put(newStart, info);
        } finally {
            idxLock.writeLock().unlock();
        }
    }

    @Override
    public void updateLeader(long regionId, Metapb.Peer leader) {
        idxLock.writeLock().lock();
        try {
            var prev = byId.get(regionId);
            if (prev == null) return;
            var refreshed = new RegionInfo(prev.region(), leader);
            byId.put(regionId, refreshed);
            byStartKey.put(prev.region().getStartKey().toByteArray(), refreshed);
        } finally {
            idxLock.writeLock().unlock();
        }
    }

    @Override
    public void invalidate(long regionId) {
        idxLock.writeLock().lock();
        try {
            var info = byId.remove(regionId);
            if (info != null) byStartKey.remove(info.region().getStartKey().toByteArray());
        } finally {
            idxLock.writeLock().unlock();
        }
    }

    @Override
    public void invalidateRange(byte[] start, byte[] end) {
        idxLock.writeLock().lock();
        try {
            invalidateOverlapLocked(start, end == null ? new byte[0] : end);
        } finally {
            idxLock.writeLock().unlock();
        }
    }

    @Override
    public List<RegionInfo> scan(byte[] startKey, byte[] endKey) {
        var out = new ArrayList<RegionInfo>();
        byte[] cursor = startKey;
        while (true) {
            var info = locateKey(cursor).orElse(null);
            if (info == null) break;
            out.add(info);
            byte[] next = info.region().getEndKey().toByteArray();
            if (next.length == 0) break;
            if (endKey != null && Arrays.compareUnsigned(next, endKey) >= 0) break;
            cursor = next;
        }
        return out;
    }

    @Override public int size() { return byId.size(); }

    @Override
    public void clear() {
        idxLock.writeLock().lock();
        try {
            byId.clear();
            byStartKey.clear();
        } finally {
            idxLock.writeLock().unlock();
        }
    }

    // =====================================================================

    private RegionInfo lookupLocked(byte[] key) {
        var entry = byStartKey.floorEntry(key);
        if (entry == null) return null;
        var info = entry.getValue();
        byte[] end = info.region().getEndKey().toByteArray();
        if (end.length > 0 && Arrays.compareUnsigned(end, key) <= 0) return null;
        return info;
    }

    private Optional<RegionInfo> loadFromPd(byte[] key) {
        try {
            var resp = pdClient.blockingStub().getRegion(Pdpb.GetRegionRequest.newBuilder()
                    .setRegionKey(com.google.protobuf.ByteString.copyFrom(key))
                    .build());
            if (!resp.hasRegion()) return Optional.empty();
            var leader = resp.hasLeader() ? resp.getLeader() : Metapb.Peer.getDefaultInstance();
            update(resp.getRegion(), leader);
            return Optional.of(new RegionInfo(resp.getRegion(), leader));
        } catch (io.grpc.StatusRuntimeException e) {
            log.warn("locateKey from PD failed, switching leader: {}", e.getMessage());
            pdClient.switchLeader();
            return Optional.empty();
        } catch (Throwable t) {
            log.warn("locateKey from PD failed: {}", t.getMessage());
            return Optional.empty();
        }
    }

    /** Drop every cached region whose range overlaps [newStart, newEnd). */
    private void invalidateOverlapLocked(byte[] newStart, byte[] newEnd) {
        // Find the cached region covering newStart (its start_key is the
        // floor of newStart). Then iterate forward dropping anything that
        // starts before newEnd.
        var floor = byStartKey.floorEntry(newStart);
        var cursor = floor == null ? byStartKey.ceilingKey(newStart) : floor.getKey();
        while (cursor != null) {
            var info = byStartKey.get(cursor);
            if (info == null) break;
            // Stop if the cached region starts at or beyond newEnd (no overlap).
            if (newEnd.length > 0 && Arrays.compareUnsigned(cursor, newEnd) >= 0) break;
            // Skip cached regions that end before newStart.
            byte[] cachedEnd = info.region().getEndKey().toByteArray();
            if (cachedEnd.length > 0 && Arrays.compareUnsigned(cachedEnd, newStart) <= 0) {
                cursor = byStartKey.higherKey(cursor);
                continue;
            }
            // Overlap — remove.
            byId.remove(info.region().getId());
            var next = byStartKey.higherKey(cursor);
            byStartKey.remove(cursor);
            cursor = next;
        }
    }

    /** Strict dominance: returns true if a's (conf_ver, version) ≥ b's. */
    private static boolean dominates(Metapb.RegionEpoch a, Metapb.RegionEpoch b) {
        if (a.getConfVer() > b.getConfVer()) return true;
        if (a.getConfVer() < b.getConfVer()) return false;
        return a.getVersion() >= b.getVersion();
    }
}
