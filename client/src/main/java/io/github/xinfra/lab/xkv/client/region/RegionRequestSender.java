package io.github.xinfra.lab.xkv.client.region;

import io.github.xinfra.lab.xkv.client.backoff.Backoffer;
import io.github.xinfra.lab.xkv.proto.Errorpb;

import java.util.function.Function;

/**
 * Routes one logical KV-side RPC to the right (store, region, leader).
 *
 * <p>Centralizes ALL retry / backoff / region-cache invalidation logic so
 * {@link io.github.xinfra.lab.xkv.client.txn.TwoPhaseCommitter} and the raw
 * KV layer don't each reimplement it (the v1 design rot).
 *
 * <h3>Lifecycle of one logical request</h3>
 * <ol>
 *   <li>Locate the region for the target key (RegionCache).</li>
 *   <li>Locate the leader peer of that region.</li>
 *   <li>Send the underlying gRPC.</li>
 *   <li>Inspect {@link Errorpb.Error} response field. Each error class
 *       routes to a specific {@link Backoffer.Reason} and remediation:
 *       <ul>
 *         <li>NotLeader      → updateLeader, retry</li>
 *         <li>EpochNotMatch  → invalidateRange, refetch from PD, retry</li>
 *         <li>ServerIsBusy   → backoff(SERVER_BUSY), retry</li>
 *         <li>StaleCommand   → updateLeader, retry</li>
 *         <li>StoreNotMatch  → invalidate region, retry</li>
 *         <li>MaxTsNotSynced → backoff(MAX_TS_NOT_SYNCED), retry</li>
 *         <li>DataIsNotReady → fall back to leader read, retry</li>
 *         <li>...</li>
 *       </ul></li>
 *   <li>Inspect gRPC status: UNAVAILABLE / DEADLINE_EXCEEDED →
 *       backoff(NETWORK), retry. Other statuses are fatal.</li>
 * </ol>
 *
 * <p>The retry loop terminates when the {@link Backoffer} is exhausted.
 */
public interface RegionRequestSender {

    /**
     * Send one RPC keyed at {@code key}. The caller supplies a function
     * mapping a (region, leader) pair to the actual gRPC call; the sender
     * provides routing, retry, and error mapping.
     *
     * @param key       the key whose region owns the request
     * @param backoffer per-request retry budget
     * @param call      transport-level call factory: given a target peer,
     *                  perform the gRPC and return the response (which the
     *                  sender will inspect for region_error)
     * @param <T> response type; must expose a {@code region_error} via
     *            {@code regionErrorOf}
     */
    <T> T send(byte[] key,
               Backoffer backoffer,
               Function<RegionCache.RegionInfo, T> call,
               Function<T, Errorpb.Error> regionErrorOf);

    /**
     * Send to a region addressed by id (e.g. CheckSecondaryLocks where the
     * caller already knows the region from a prior lookup).
     */
    <T> T sendToRegion(long regionId,
                       Backoffer backoffer,
                       Function<RegionCache.RegionInfo, T> call,
                       Function<T, Errorpb.Error> regionErrorOf);
}
