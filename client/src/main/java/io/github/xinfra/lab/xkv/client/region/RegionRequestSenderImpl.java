package io.github.xinfra.lab.xkv.client.region;

import io.github.xinfra.lab.xkv.client.backoff.Backoffer;
import io.github.xinfra.lab.xkv.proto.Errorpb;
import io.github.xinfra.lab.xkv.proto.TikvGrpc;
import io.github.xinfra.lab.xkv.proto.Tikvpb;
import io.grpc.StatusRuntimeException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CompletionException;
import java.util.function.BiFunction;
import java.util.function.Function;

/**
 * Default {@link RegionRequestSender} — the central retry / region-error
 * handling pipeline.
 *
 * <h3>Retry loop</h3>
 *
 * <ol>
 *   <li>Locate the region for {@code key} (cache → PD on miss).</li>
 *   <li>Resolve the leader peer's store, fetch its {@code TikvBlockingStub}.</li>
 *   <li>Invoke the caller-supplied {@code call(stub, info)} lambda.</li>
 *   <li>Inspect the response's {@code region_error}:
 *       <ul>
 *         <li>{@code NotLeader} → {@code updateLeader} + retry on backoff(NOT_LEADER)</li>
 *         <li>{@code EpochNotMatch} → invalidate region + retry on backoff(EPOCH_NOT_MATCH)</li>
 *         <li>{@code RegionNotFound / KeyNotInRegion} → invalidate + retry on backoff(REGION_MISS)</li>
 *         <li>{@code ServerIsBusy} → backoff(SERVER_BUSY) without invalidating</li>
 *         <li>{@code StaleCommand / MaxTimestampNotSynced / DataIsNotReady} → tiny backoff, retry</li>
 *         <li>{@code StoreNotMatch} → invalidate region + retry</li>
 *       </ul></li>
 *   <li>Inspect the gRPC status: {@code UNAVAILABLE / DEADLINE_EXCEEDED} →
 *       backoff(NETWORK), retry. Other statuses propagate.</li>
 * </ol>
 *
 * <p>The retry loop terminates when the {@link Backoffer} is exhausted.
 *
 * <p>v1 had this logic scattered across raw-KV / txn / scan paths with
 * subtle differences (one path checked NotLeader but not EpochNotMatch,
 * another swallowed StaleCommand, etc). Centralizing it here is the
 * reason "every region error has exactly one definition" became feasible.
 */
public final class RegionRequestSenderImpl implements RegionRequestSender {
    private static final Logger log = LoggerFactory.getLogger(RegionRequestSenderImpl.class);

    private final RegionCache cache;
    private final StoreChannelCache stores;
    private final BatchCommandsClient batchClient;

    public RegionRequestSenderImpl(RegionCache cache, StoreChannelCache stores) {
        this(cache, stores, null);
    }

    public RegionRequestSenderImpl(RegionCache cache, StoreChannelCache stores,
                                   BatchCommandsClient batchClient) {
        this.cache = cache;
        this.stores = stores;
        this.batchClient = batchClient;
    }

    public BatchCommandsClient batchClient() { return batchClient; }

    /**
     * The actual call you'd want most of the time: takes the locator key,
     * the per-stub call factory, and the per-response error extractor.
     * Returns the successful response or throws after exhausting backoff.
     */
    public <T> T sendKeyed(byte[] key,
                            Backoffer backoffer,
                            BiFunction<TikvGrpc.TikvBlockingStub, RegionCache.RegionInfo, T> call,
                            Function<T, Errorpb.Error> regionErrorOf) {
        while (true) {
            var info = cache.locateKey(key).orElse(null);
            if (info == null) {
                backoffer.backoff(Backoffer.Reason.REGION_MISS, "no region for key");
                continue;
            }
            long leaderStore = leaderStoreOf(info);
            if (leaderStore == 0) {
                backoffer.backoff(Backoffer.Reason.NOT_LEADER, "leader unknown");
                continue;
            }
            var stub = stores.stubFor(leaderStore);
            if (stub == null) {
                backoffer.backoff(Backoffer.Reason.NETWORK, "store " + leaderStore + " unreachable");
                continue;
            }
            T resp;
            try {
                resp = call.apply(stub, info);
            } catch (StatusRuntimeException e) {
                if (isNetworkError(e)) {
                    stores.closeStore(leaderStore);
                    backoffer.backoff(Backoffer.Reason.NETWORK, e.getStatus().toString());
                    continue;
                }
                throw e;
            }
            var err = regionErrorOf.apply(resp);
            if (err == null || isEmptyError(err)) {
                return resp;
            }
            handleRegionError(info, err, backoffer);
        }
    }

    /**
     * Send a request through the {@link BatchCommandsClient} bidi stream.
     *
     * @param wrap          builds the {@code BatchCommandsRequest.Request} from region info
     * @param unwrap        extracts the typed response from the batch response
     * @param regionErrorOf extracts region error from the typed response
     */
    public <T> T sendKeyedBatched(byte[] key,
                                   Backoffer backoffer,
                                   Function<RegionCache.RegionInfo, Tikvpb.BatchCommandsRequest.Request> wrap,
                                   Function<Tikvpb.BatchCommandsResponse.Response, T> unwrap,
                                   Function<T, Errorpb.Error> regionErrorOf) {
        if (batchClient == null) {
            throw new IllegalStateException("BatchCommandsClient not configured");
        }
        while (true) {
            var info = cache.locateKey(key).orElse(null);
            if (info == null) {
                backoffer.backoff(Backoffer.Reason.REGION_MISS, "no region for key");
                continue;
            }
            long leaderStore = leaderStoreOf(info);
            if (leaderStore == 0) {
                backoffer.backoff(Backoffer.Reason.NOT_LEADER, "leader unknown");
                continue;
            }
            Tikvpb.BatchCommandsResponse.Response batchResp;
            try {
                var req = wrap.apply(info);
                batchResp = batchClient.send(leaderStore, req).join();
            } catch (CompletionException e) {
                Throwable cause = e.getCause();
                if (cause instanceof StatusRuntimeException sre && isNetworkError(sre)) {
                    stores.closeStore(leaderStore);
                    batchClient.closeStore(leaderStore);
                    backoffer.backoff(Backoffer.Reason.NETWORK, sre.getStatus().toString());
                    continue;
                }
                stores.closeStore(leaderStore);
                batchClient.closeStore(leaderStore);
                backoffer.backoff(Backoffer.Reason.NETWORK, cause != null ? cause.getMessage() : e.getMessage());
                continue;
            } catch (Throwable t) {
                stores.closeStore(leaderStore);
                batchClient.closeStore(leaderStore);
                backoffer.backoff(Backoffer.Reason.NETWORK, t.getMessage());
                continue;
            }
            T resp = unwrap.apply(batchResp);
            if (resp == null) {
                backoffer.backoff(Backoffer.Reason.OTHER, "empty batch response");
                continue;
            }
            var err = regionErrorOf.apply(resp);
            if (err == null || isEmptyError(err)) {
                return resp;
            }
            handleRegionError(info, err, backoffer);
        }
    }

    @Override
    public <T> T send(byte[] key,
                      Backoffer backoffer,
                      Function<RegionCache.RegionInfo, T> call,
                      Function<T, Errorpb.Error> regionErrorOf) {
        // Adapter for the SPI's narrower signature: ignores the stub and
        // expects the caller to wire it themselves. Less convenient than
        // sendKeyed but matches the interface contract.
        while (true) {
            var info = cache.locateKey(key).orElse(null);
            if (info == null) {
                backoffer.backoff(Backoffer.Reason.REGION_MISS, "no region for key");
                continue;
            }
            T resp;
            try {
                resp = call.apply(info);
            } catch (StatusRuntimeException e) {
                if (isNetworkError(e)) {
                    backoffer.backoff(Backoffer.Reason.NETWORK, e.getStatus().toString());
                    continue;
                }
                throw e;
            }
            var err = regionErrorOf.apply(resp);
            if (err == null || isEmptyError(err)) return resp;
            handleRegionError(info, err, backoffer);
        }
    }

    @Override
    public <T> T sendToRegion(long regionId,
                              Backoffer backoffer,
                              Function<RegionCache.RegionInfo, T> call,
                              Function<T, Errorpb.Error> regionErrorOf) {
        while (true) {
            var info = cache.locateRegion(regionId).orElse(null);
            if (info == null) {
                backoffer.backoff(Backoffer.Reason.REGION_MISS, "regionId " + regionId + " unknown");
                continue;
            }
            T resp;
            try {
                resp = call.apply(info);
            } catch (StatusRuntimeException e) {
                if (isNetworkError(e)) {
                    backoffer.backoff(Backoffer.Reason.NETWORK, e.getStatus().toString());
                    continue;
                }
                throw e;
            }
            var err = regionErrorOf.apply(resp);
            if (err == null || isEmptyError(err)) return resp;
            handleRegionError(info, err, backoffer);
        }
    }

    // =====================================================================

    private void handleRegionError(RegionCache.RegionInfo info, Errorpb.Error err, Backoffer bo) {
        if (err.hasNotLeader()) {
            var nl = err.getNotLeader();
            if (nl.hasLeader()) {
                cache.updateLeader(info.region().getId(), nl.getLeader());
            } else {
                cache.invalidate(info.region().getId());
            }
            bo.backoff(Backoffer.Reason.NOT_LEADER, "not leader");
            return;
        }
        if (err.hasEpochNotMatch()) {
            // PD has fresher region info; drop and re-fetch.
            cache.invalidate(info.region().getId());
            bo.backoff(Backoffer.Reason.EPOCH_NOT_MATCH, "epoch not match");
            return;
        }
        if (err.hasRegionNotFound() || err.hasKeyNotInRegion() || err.hasRegionNotInitialized()) {
            cache.invalidate(info.region().getId());
            bo.backoff(Backoffer.Reason.REGION_MISS, "region missing/uninit");
            return;
        }
        if (err.hasStoreNotMatch() || err.hasMismatchPeerId()) {
            cache.invalidate(info.region().getId());
            bo.backoff(Backoffer.Reason.REGION_MISS, "store/peer mismatch");
            return;
        }
        if (err.hasServerIsBusy() || err.hasDiskFull()) {
            bo.backoff(Backoffer.Reason.SERVER_BUSY, err.getMessage());
            return;
        }
        if (err.hasStaleCommand()) {
            bo.backoff(Backoffer.Reason.STALE_COMMAND, "stale command");
            return;
        }
        if (err.hasMaxTimestampNotSynced() || err.hasReadIndexNotReady()) {
            bo.backoff(Backoffer.Reason.MAX_TS_NOT_SYNCED, "max-ts not synced");
            return;
        }
        if (err.hasDataIsNotReady()) {
            bo.backoff(Backoffer.Reason.DATA_NOT_READY, "data not ready");
            return;
        }
        if (err.hasRecoveryInProgress() || err.hasFlashbackInProgress() || err.hasFlashbackNotPrepared()) {
            bo.backoff(Backoffer.Reason.SERVER_BUSY, "recovery in progress");
            return;
        }
        if (err.hasRaftEntryTooLarge()) {
            // Don't retry — caller's payload exceeds limit.
            throw new RuntimeException("raft entry too large: " + err.getMessage());
        }
        // Unknown — backoff with the OTHER reason.
        log.debug("unknown region error: {}", err);
        bo.backoff(Backoffer.Reason.OTHER, err.getMessage());
    }

    private static boolean isEmptyError(Errorpb.Error err) {
        // Empty proto with no oneof set; treat as no error.
        return err.equals(Errorpb.Error.getDefaultInstance());
    }

    private static boolean isNetworkError(StatusRuntimeException e) {
        var c = e.getStatus().getCode();
        return c == io.grpc.Status.Code.UNAVAILABLE
                || c == io.grpc.Status.Code.DEADLINE_EXCEEDED
                || c == io.grpc.Status.Code.CANCELLED;
    }

    private static long leaderStoreOf(RegionCache.RegionInfo info) {
        var leader = info.leader();
        if (leader == null || leader.getId() == 0) {
            // No known leader; fall back to first peer.
            if (info.region().getPeersCount() == 0) return 0;
            return info.region().getPeers(0).getStoreId();
        }
        return leader.getStoreId() == 0
                ? findStoreIdInRegion(info, leader.getId())
                : leader.getStoreId();
    }

    private static long findStoreIdInRegion(RegionCache.RegionInfo info, long peerId) {
        for (var p : info.region().getPeersList()) {
            if (p.getId() == peerId) return p.getStoreId();
        }
        return 0;
    }
}
