package io.github.xinfra.lab.xkv.pd.state;

import io.github.xinfra.lab.xkv.proto.Metapb;

import java.util.Collection;
import java.util.List;
import java.util.Optional;

/**
 * Read-only snapshot of cluster topology + statistics, exposed to schedulers.
 *
 * <p>Backed by {@link PdStateMachine}. Every method is read-only and
 * O(log N) at worst — implementations index regions by start_key in a
 * {@code TreeMap} so {@link #getRegionByKey} is fast (the v1 implementation's
 * linear scan was a 10k-region scaling cliff).
 */
public interface ClusterView {

    Collection<Metapb.Store> stores();

    Optional<Metapb.Store> store(long storeId);

    /** Estimated region count on this store; updated incrementally on apply. */
    int regionCountOnStore(long storeId);

    /** Estimated leader count on this store. */
    int leaderCountOnStore(long storeId);

    Optional<Metapb.Region> region(long regionId);

    /** Find the region whose [start, end) contains key. */
    Optional<Metapb.Region> getRegionByKey(byte[] key);

    /** Scan up to {@code limit} regions starting at {@code startKey}. */
    List<Metapb.Region> scanRegions(byte[] startKey, byte[] endKey, int limit);

    /** Total region count cluster-wide. */
    int totalRegions();

    /** Total store count cluster-wide. */
    int totalStores();

    /** Hot regions by recent QPS (top-N). */
    List<Long> hotRegionIds(int topN);
}
