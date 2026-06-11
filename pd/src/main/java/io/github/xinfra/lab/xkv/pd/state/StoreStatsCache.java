package io.github.xinfra.lab.xkv.pd.state;

import io.github.xinfra.lab.xkv.proto.Pdpb;

import java.util.Collection;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

/**
 * In-memory cache of the latest {@link Pdpb.StoreStats} reported by each
 * KV store via the {@code StoreHeartbeat} RPC.
 *
 * <p>Store stats are ephemeral, high-frequency data (capacity, region count,
 * busy flag, throughput counters). They are NOT replicated through raft —
 * only the PD leader needs them for scheduling decisions.
 *
 * <p>Thread-safe via {@link ConcurrentHashMap}.
 */
public final class StoreStatsCache {

    private final ConcurrentHashMap<Long, Pdpb.StoreStats> cache = new ConcurrentHashMap<>();

    public void update(Pdpb.StoreStats stats) {
        if (stats.getStoreId() == 0) return;
        cache.put(stats.getStoreId(), stats);
    }

    public Optional<Pdpb.StoreStats> get(long storeId) {
        return Optional.ofNullable(cache.get(storeId));
    }

    public Collection<Pdpb.StoreStats> all() {
        return cache.values();
    }

    public boolean isBusy(long storeId) {
        var s = cache.get(storeId);
        return s != null && s.getIsBusy();
    }

    public int slowScore(long storeId) {
        var s = cache.get(storeId);
        return s != null ? s.getSlowScore() : 0;
    }

    /**
     * Returns the available/capacity ratio for a store, or 1.0 if no stats
     * have been reported yet (optimistic default).
     */
    public double availableRatio(long storeId) {
        var s = cache.get(storeId);
        if (s == null || s.getCapacity() == 0) return 1.0;
        return (double) s.getAvailable() / s.getCapacity();
    }

    public void remove(long storeId) {
        cache.remove(storeId);
    }

    public int size() {
        return cache.size();
    }
}
