package io.github.xinfra.lab.xkv.kv.store;

import io.github.xinfra.lab.xkv.kv.raft.RegionPeer;
import io.github.xinfra.lab.xkv.proto.Metapb;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Collection;
import java.util.Optional;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Default {@link Store} implementation.
 *
 * <p>Holds the local set of {@link RegionPeer}s plus a side index by
 * {@code start_key} so {@link #peerForKey} is O(log N). v1's equivalent
 * did a linear scan; the v2 indexing is the same trick we apply on the
 * PD side ({@link io.github.xinfra.lab.xkv.pd.state.InMemoryPdStateMachine}).
 *
 * <p>Add / remove operations grab the write lock; reads take the read lock.
 * Per-peer state machines are otherwise lock-free — region peer mutex
 * lives inside the peer.
 */
public final class StoreImpl implements Store {
    private static final Logger log = LoggerFactory.getLogger(StoreImpl.class);

    private final long storeId;
    private final Metapb.Store metadata;
    private final ConcurrentHashMap<Long, RegionPeer> byId = new ConcurrentHashMap<>();
    private final TreeMap<byte[], RegionPeer> byStartKey =
            new TreeMap<>(java.util.Arrays::compareUnsigned);
    private final ReentrantReadWriteLock indexLock = new ReentrantReadWriteLock();

    public StoreImpl(long storeId, Metapb.Store metadata) {
        this.storeId = storeId;
        this.metadata = metadata;
    }

    @Override public long storeId() { return storeId; }

    @Override public Metapb.Store metadata() { return metadata; }

    @Override
    public Optional<RegionPeer> peerForRegion(long regionId) {
        return Optional.ofNullable(byId.get(regionId));
    }

    @Override
    public Optional<RegionPeer> peerForKey(byte[] key) {
        indexLock.readLock().lock();
        try {
            var entry = byStartKey.floorEntry(key);
            if (entry == null) return Optional.empty();
            var peer = entry.getValue();
            byte[] end = peer.region().getEndKey().toByteArray();
            // Empty end_key encodes "+Inf".
            if (end.length > 0 && Arrays.compareUnsigned(end, key) <= 0) {
                return Optional.empty();
            }
            return Optional.of(peer);
        } finally {
            indexLock.readLock().unlock();
        }
    }

    @Override
    public Collection<RegionPeer> peers() { return byId.values(); }

    public int regionCount() { return byId.size(); }

    @Override
    public void registerPeer(RegionPeer peer) {
        indexLock.writeLock().lock();
        try {
            byId.put(peer.regionId(), peer);
            byStartKey.put(peer.region().getStartKey().toByteArray(), peer);
            log.info("store={} registered region={} range=[{}, {})",
                    storeId, peer.regionId(),
                    formatKey(peer.region().getStartKey().toByteArray()),
                    formatKey(peer.region().getEndKey().toByteArray()));
        } finally {
            indexLock.writeLock().unlock();
        }
    }

    @Override
    public void destroyPeer(long regionId) {
        indexLock.writeLock().lock();
        try {
            var peer = byId.remove(regionId);
            if (peer != null) {
                byStartKey.remove(peer.region().getStartKey().toByteArray());
                peer.shutdown();
                log.info("store={} destroyed region={}", storeId, regionId);
            }
        } finally {
            indexLock.writeLock().unlock();
        }
    }

    @Override
    public void runHeartbeatTick() {
        // Phase 4 will wire region heartbeat to PD here.
    }

    @Override
    public void shutdown() {
        indexLock.writeLock().lock();
        try {
            for (var p : byId.values()) {
                try { p.shutdown(); } catch (Throwable t) { log.warn("peer shutdown failed", t); }
            }
            byId.clear();
            byStartKey.clear();
        } finally {
            indexLock.writeLock().unlock();
        }
    }

    private static String formatKey(byte[] k) {
        if (k == null || k.length == 0) return "''";
        if (k.length > 16) return "0x" + java.util.HexFormat.of().formatHex(k, 0, 16) + "...";
        return new String(k);
    }
}
