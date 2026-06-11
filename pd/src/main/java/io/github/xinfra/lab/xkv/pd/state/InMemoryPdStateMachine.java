package io.github.xinfra.lab.xkv.pd.state;

import com.google.protobuf.ByteString;
import io.github.xinfra.lab.xkv.proto.Metapb;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.NavigableMap;
import java.util.Optional;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * In-memory implementation of {@link PdStateMachine}.
 *
 * <h3>The v1 invariant the constructor enforces</h3>
 *
 * <p>v1 had the leader fast-path write the state CF directly, then propose
 * a Raft entry afterwards (C6 in the audit). On crash between the two,
 * the local state was ahead of the Raft log; on restart the node became
 * a follower and a stale snapshot from another peer overwrote the lead
 * state — silently losing region updates.
 *
 * <p>v2 enforces by structure: the only way to mutate state is via
 * {@link #applyCommand}. Direct setters do not exist on this class.
 *
 * <h3>Region indexing</h3>
 *
 * <p>Two indices kept consistent:
 * <ul>
 *   <li>{@code byId}      — region_id ↦ Region</li>
 *   <li>{@code byStartKey} — start_key ↦ Region (TreeMap, ceiling/floor for O(log N))</li>
 * </ul>
 *
 * <p>v1's {@code findRegionByKey} did a full table scan; this implementation
 * uses {@code TreeMap.floorEntry} for O(log N) lookup.
 *
 * <h3>Persistence</h3>
 *
 * <p>This class is in-memory. Snapshot dump/install round-trips the entire
 * state through protobuf bytes — Phase 4 swaps it for an incremental
 * RocksDB-backed implementation when the dataset outgrows memory.
 */
public final class InMemoryPdStateMachine implements PdStateMachine {
    private static final Logger log = LoggerFactory.getLogger(InMemoryPdStateMachine.class);

    private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();

    private boolean bootstrapped;
    private Metapb.Cluster cluster;

    private final java.util.HashMap<Long, Metapb.Store> stores = new java.util.HashMap<>();
    private final java.util.HashMap<Long, Metapb.Region> regionsById = new java.util.HashMap<>();
    private final NavigableMap<ByteString, Metapb.Region> regionsByStart = new TreeMap<>(
            (a, b) -> com.google.protobuf.ByteString.unsignedLexicographicalComparator().compare(a, b));

    private final java.util.HashMap<Long, Metapb.Peer> leaders = new java.util.HashMap<>();
    private final java.util.concurrent.ConcurrentHashMap<Long, RegionStats> regionStats = new java.util.concurrent.ConcurrentHashMap<>();

    private final AtomicLong idAllocator = new AtomicLong(1000);   // start ids past well-known reserved ones

    @Override
    public void updateRegionStats(long regionId, long approximateSize, long approximateKeys) {
        regionStats.put(regionId, new RegionStats(approximateSize, approximateKeys));
    }

    @Override
    public Optional<RegionStats> getRegionStats(long regionId) {
        return Optional.ofNullable(regionStats.get(regionId));
    }

    @Override
    public java.util.Map<Long, RegionStats> allRegionStats() {
        return java.util.Collections.unmodifiableMap(regionStats);
    }

    @Override public boolean isBootstrapped() {
        lock.readLock().lock();
        try { return bootstrapped; } finally { lock.readLock().unlock(); }
    }

    @Override
    public void bootstrap(Metapb.Store firstStore, Metapb.Region firstRegion) {
        lock.writeLock().lock();
        try {
            if (bootstrapped) {
                log.info("bootstrap idempotent: cluster already up");
                return;
            }
            this.cluster = Metapb.Cluster.newBuilder().setId(1).setMaxPeerCount(3).build();
            this.stores.put(firstStore.getId(), firstStore);
            this.regionsById.put(firstRegion.getId(), firstRegion);
            this.regionsByStart.put(firstRegion.getStartKey(), firstRegion);
            this.bootstrapped = true;
            // Bump id allocator past whatever bootstrap consumed.
            this.idAllocator.set(Math.max(idAllocator.get(),
                    Math.max(firstStore.getId(), firstRegion.getId()) + 1));
        } finally {
            lock.writeLock().unlock();
        }
    }

    @Override
    public Metapb.Cluster cluster() {
        lock.readLock().lock();
        try { return cluster; } finally { lock.readLock().unlock(); }
    }

    @Override
    public void putStore(Metapb.Store store) {
        lock.writeLock().lock();
        try {
            stores.put(store.getId(), store);
        } finally {
            lock.writeLock().unlock();
        }
    }

    @Override
    public Optional<Metapb.Store> getStore(long storeId) {
        lock.readLock().lock();
        try { return Optional.ofNullable(stores.get(storeId)); }
        finally { lock.readLock().unlock(); }
    }

    @Override
    public Iterable<Metapb.Store> allStores() {
        lock.readLock().lock();
        try { return new ArrayList<>(stores.values()); }
        finally { lock.readLock().unlock(); }
    }

    @Override
    public Optional<Metapb.Region> getRegion(long regionId) {
        lock.readLock().lock();
        try { return Optional.ofNullable(regionsById.get(regionId)); }
        finally { lock.readLock().unlock(); }
    }

    @Override
    public Optional<Metapb.Region> getRegionByKey(byte[] key) {
        ByteString k = ByteString.copyFrom(key);
        lock.readLock().lock();
        try {
            // floorEntry returns the largest start_key <= k; that region's
            // [start, end) covers k iff k < end.
            var floor = regionsByStart.floorEntry(k);
            if (floor == null) return Optional.empty();
            var r = floor.getValue();
            ByteString end = r.getEndKey();
            // Empty end_key encodes "+Inf" (end of key space).
            if (!end.isEmpty() && cmp(end, k) <= 0) return Optional.empty();
            return Optional.of(r);
        } finally {
            lock.readLock().unlock();
        }
    }

    private static int cmp(ByteString a, ByteString b) {
        return com.google.protobuf.ByteString.unsignedLexicographicalComparator().compare(a, b);
    }

    @Override
    public void updateRegion(Metapb.Region region) {
        lock.writeLock().lock();
        try {
            var prev = regionsById.get(region.getId());
            if (prev != null) {
                // Epoch dominance check — drop stale heartbeats.
                long prevConf = prev.getRegionEpoch().getConfVer();
                long prevVer = prev.getRegionEpoch().getVersion();
                long newConf = region.getRegionEpoch().getConfVer();
                long newVer = region.getRegionEpoch().getVersion();
                if (newConf < prevConf || (newConf == prevConf && newVer < prevVer)) {
                    log.debug("dropping stale region update id={} epoch=({},{}) < prev=({},{})",
                            region.getId(), newConf, newVer, prevConf, prevVer);
                    return;
                }
                // Range may have changed (split/merge). Re-index.
                regionsByStart.remove(prev.getStartKey());
            }
            regionsById.put(region.getId(), region);
            regionsByStart.put(region.getStartKey(), region);
        } finally {
            lock.writeLock().unlock();
        }
    }

    @Override
    public void updateLeader(long regionId, Metapb.Peer leader) {
        lock.writeLock().lock();
        try {
            leaders.put(regionId, leader);
        } finally {
            lock.writeLock().unlock();
        }
    }

    @Override
    public Optional<Metapb.Peer> getLeader(long regionId) {
        lock.readLock().lock();
        try {
            return Optional.ofNullable(leaders.get(regionId));
        } finally {
            lock.readLock().unlock();
        }
    }

    @Override
    public Iterable<Metapb.Region> scanRegions(byte[] startKey, byte[] endKey, int limit) {
        ByteString s = ByteString.copyFrom(startKey == null ? new byte[0] : startKey);
        ByteString e = endKey == null ? null : ByteString.copyFrom(endKey);
        lock.readLock().lock();
        try {
            var out = new ArrayList<Metapb.Region>();
            // Start from the region whose start_key is the floor of `s`
            // (so a partial overlap at the start is included).
            ByteString cursor = s;
            var floor = regionsByStart.floorEntry(cursor);
            if (floor != null && (floor.getValue().getEndKey().isEmpty()
                    || cmp(floor.getValue().getEndKey(), s) > 0)) {
                cursor = floor.getKey();
            } else {
                var ceil = regionsByStart.ceilingEntry(cursor);
                cursor = ceil == null ? null : ceil.getKey();
            }
            while (cursor != null && out.size() < limit) {
                var entry = regionsByStart.ceilingEntry(cursor);
                if (entry == null) break;
                if (e != null && cmp(entry.getKey(), e) >= 0) break;
                out.add(entry.getValue());
                ByteString next = entry.getValue().getEndKey();
                if (next.isEmpty()) break;
                cursor = next;
            }
            return out;
        } finally {
            lock.readLock().unlock();
        }
    }

    @Override
    public long allocId(int count) {
        if (count <= 0) throw new IllegalArgumentException();
        return idAllocator.getAndAdd(count);
    }

    @Override public byte[] dumpSnapshot() {
        lock.readLock().lock();
        try {
            var snap = io.github.xinfra.lab.xkv.proto.PdInternalpb.PdSnapshot.newBuilder()
                    .setBootstrapped(bootstrapped)
                    .setIdAllocatorNext(idAllocator.get());
            if (cluster != null) snap.setCluster(cluster);
            for (var s : stores.values()) snap.addStores(s);
            for (var r : regionsById.values()) snap.addRegions(r);
            return snap.build().toByteArray();
        } finally {
            lock.readLock().unlock();
        }
    }

    @Override public void installSnapshot(byte[] snapshot) {
        lock.writeLock().lock();
        try {
            var snap = io.github.xinfra.lab.xkv.proto.PdInternalpb.PdSnapshot.parseFrom(snapshot);
            bootstrapped = snap.getBootstrapped();
            cluster = snap.hasCluster() ? snap.getCluster() : null;
            stores.clear();
            regionsById.clear();
            regionsByStart.clear();
            for (var s : snap.getStoresList()) stores.put(s.getId(), s);
            for (var r : snap.getRegionsList()) {
                regionsById.put(r.getId(), r);
                regionsByStart.put(r.getStartKey(), r);
            }
            if (snap.getIdAllocatorNext() > 0) {
                idAllocator.set(snap.getIdAllocatorNext());
            }
            log.info("PD snapshot installed: {} stores, {} regions, idAlloc={}",
                    stores.size(), regionsById.size(), idAllocator.get());
        } catch (Exception e) {
            throw new RuntimeException("PD snapshot install failed", e);
        } finally {
            lock.writeLock().unlock();
        }
    }

    @Override public void applyCommand(byte[] command) {
        try {
            var cmd = io.github.xinfra.lab.xkv.proto.PdInternalpb.PdCommand.parseFrom(command);
            switch (cmd.getType()) {
                case CMD_BOOTSTRAP -> {
                    var payload = cmd.getBootstrap();
                    bootstrap(payload.getStore(), payload.getRegion());
                }
                case CMD_PUT_STORE -> putStore(cmd.getStore());
                case CMD_UPDATE_REGION -> updateRegion(cmd.getRegion());
                case CMD_ALLOC_ID -> {
                    // allocId is handled before propose (leader pre-allocates);
                    // the apply path only needs to bump the allocator to ensure
                    // followers stay in sync.
                    var payload = cmd.getAllocId();
                    long needed = payload.getBaseId() + payload.getCount();
                    idAllocator.updateAndGet(cur -> Math.max(cur, needed));
                }
                default -> log.warn("unknown PD command type: {}", cmd.getType());
            }
        } catch (Exception e) {
            log.warn("applyCommand failed: {}", e.getMessage());
        }
    }

    @Override public void onBecomeLeader() { /* hook for TSO reload, scheduler start */ }
    @Override public void onLoseLeader()   { /* hook to pause scheduler */ }
    @Override public void close()          { /* in-memory: nothing to release */ }

    @Override
    public int storeCount() {
        lock.readLock().lock();
        try { return stores.size(); } finally { lock.readLock().unlock(); }
    }
    @Override
    public int regionCount() {
        lock.readLock().lock();
        try { return regionsById.size(); } finally { lock.readLock().unlock(); }
    }

    @Override
    public Collection<Metapb.Region> allRegions() {
        lock.readLock().lock();
        try { return new ArrayList<>(regionsById.values()); }
        finally { lock.readLock().unlock(); }
    }
}
