package io.github.xinfra.lab.xkv.kv.cdc;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListSet;

/**
 * Tracks in-flight lock timestamps per region to compute per-region resolved TS.
 *
 * <p>Resolved TS for a region = {@code min(in-flight lock startTs) - 1}. If no
 * locks are in-flight, the fallback timestamp (typically the current max TS) is
 * used. This is tighter than using a global max TS because it accounts for
 * transactions that have acquired locks but not yet committed.
 */
public final class RegionResolvedTsTracker {

    private final ConcurrentHashMap<Long, ConcurrentSkipListSet<Long>> inFlight =
            new ConcurrentHashMap<>();

    public void trackLock(long regionId, long startTs) {
        inFlight.computeIfAbsent(regionId, k -> new ConcurrentSkipListSet<>())
                .add(startTs);
    }

    public void untrackLock(long regionId, long startTs) {
        var set = inFlight.get(regionId);
        if (set != null) {
            set.remove(startTs);
        }
    }

    public long resolvedTs(long regionId, long fallbackTs) {
        var set = inFlight.get(regionId);
        if (set == null || set.isEmpty()) return fallbackTs;
        return set.first() - 1;
    }
}
