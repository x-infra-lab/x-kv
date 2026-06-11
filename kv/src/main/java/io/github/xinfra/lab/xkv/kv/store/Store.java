package io.github.xinfra.lab.xkv.kv.store;

import io.github.xinfra.lab.xkv.kv.raft.RegionPeer;
import io.github.xinfra.lab.xkv.proto.Metapb;

import java.util.Collection;
import java.util.Optional;

/**
 * One physical KV node. Hosts many {@link RegionPeer}s, one per locally-
 * resident region. Owns store-wide background work (split / merge / GC
 * tick, region heartbeat to PD, store heartbeat to PD).
 */
public interface Store {

    long storeId();

    Optional<RegionPeer> peerForRegion(long regionId);

    /** All locally-resident peers. */
    Collection<RegionPeer> peers();

    /** Find the peer that owns a key. */
    Optional<RegionPeer> peerForKey(byte[] key);

    /** Register a freshly-created peer (split right-side, conf-change AddPeer). */
    void registerPeer(RegionPeer peer);

    /** Remove a peer (region merged away, conf-change RemovePeer). */
    void destroyPeer(long regionId);

    /**
     * Re-index a peer whose region descriptor changed (split / merge /
     * conf-change). Called after the apply path's hook has refreshed the
     * peer's in-memory region. Updates the {@code peerForKey} index so
     * subsequent routing uses the new range.
     *
     * <p>Default implementation: re-register the peer to refresh the
     * by-start-key map. Implementations may override to handle the
     * change-of-start-key case (rare — split keeps parent's start_key).
     */
    default void onRegionUpdated(long regionId) {
        peerForRegion(regionId).ifPresent(this::registerPeer);
    }

    /** Store-side metadata (address, labels, version). */
    Metapb.Store metadata();

    /** Trigger one round of region heartbeat to PD. */
    void runHeartbeatTick();

    void shutdown();
}
