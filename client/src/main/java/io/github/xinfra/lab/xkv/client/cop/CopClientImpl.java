package io.github.xinfra.lab.xkv.client.cop;

import com.google.protobuf.ByteString;
import io.github.xinfra.lab.xkv.client.backoff.Backoffer;
import io.github.xinfra.lab.xkv.client.backoff.BackofferImpl;
import io.github.xinfra.lab.xkv.client.config.ClientConfig;
import io.github.xinfra.lab.xkv.client.error.KvClientException;
import io.github.xinfra.lab.xkv.client.region.RegionCache;
import io.github.xinfra.lab.xkv.client.region.StoreChannelCache;
import io.github.xinfra.lab.xkv.proto.Coprocessor;
import io.github.xinfra.lab.xkv.proto.Errorpb;
import io.github.xinfra.lab.xkv.proto.Metapb;
import io.github.xinfra.lab.xkv.proto.TikvGrpc;
import io.grpc.StatusRuntimeException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;

public final class CopClientImpl implements CopClient {
    private static final Logger log = LoggerFactory.getLogger(CopClientImpl.class);

    private final RegionCache regionCache;
    private final StoreChannelCache storeCache;
    private final ExecutorService pool;
    private final ClientConfig.BackoffConfig backoffCfg;

    public CopClientImpl(RegionCache regionCache, StoreChannelCache storeCache,
                         int poolSize, ClientConfig.BackoffConfig backoffCfg) {
        this.regionCache = regionCache;
        this.storeCache = storeCache;
        this.backoffCfg = backoffCfg;
        this.pool = Executors.newFixedThreadPool(poolSize, r -> {
            Thread t = new Thread(r, "cop-client");
            t.setDaemon(true);
            return t;
        });
    }

    @Override
    public Coprocessor.Response send(Coprocessor.Request request, long regionId) {
        var regionInfo = regionCache.locateRegion(regionId)
                .orElseThrow(() -> new KvClientException(
                        KvClientException.Category.OTHER,
                        "region not found: " + regionId));
        return sendWithRetry(request, regionInfo);
    }

    @Override
    public Iterator<RegionCopResult> sendToRangeParallel(
            int tp, byte[] data, long startTs,
            byte[] startKey, byte[] endKey, int concurrency) {

        List<RegionCache.RegionInfo> regions = regionCache.scan(startKey, endKey);
        if (regions.isEmpty()) {
            return Collections.emptyIterator();
        }

        Semaphore semaphore = new Semaphore(concurrency);
        CompletionService<RegionCopResult> cs = new ExecutorCompletionService<>(pool);
        int submitted = 0;

        for (RegionCache.RegionInfo regionInfo : regions) {
            Metapb.Region region = regionInfo.region();
            byte[] regionStart = region.getStartKey().toByteArray();
            byte[] regionEnd = region.getEndKey().toByteArray();

            byte[] rangeStart = compare(startKey, regionStart) > 0 ? startKey : regionStart;
            byte[] rangeEnd;
            if (regionEnd.length == 0) {
                rangeEnd = endKey;
            } else {
                rangeEnd = compare(endKey, regionEnd) < 0 ? endKey : regionEnd;
            }

            Coprocessor.Request request = Coprocessor.Request.newBuilder()
                    .setTp(tp)
                    .setData(ByteString.copyFrom(data))
                    .setStartTs(startTs)
                    .addRanges(Coprocessor.KeyRange.newBuilder()
                            .setStart(ByteString.copyFrom(rangeStart))
                            .setEnd(ByteString.copyFrom(rangeEnd)))
                    .build();

            final RegionCache.RegionInfo ri = regionInfo;
            semaphore.acquireUninterruptibly();
            cs.submit(() -> {
                try {
                    Coprocessor.Response resp = sendWithRetry(request, ri);
                    return new RegionCopResult(ri.region().getId(), resp);
                } finally {
                    semaphore.release();
                }
            });
            submitted++;
        }

        return new CopResultIterator(cs, submitted);
    }

    private Coprocessor.Response sendWithRetry(Coprocessor.Request request,
                                                RegionCache.RegionInfo regionInfo) {
        Backoffer bo = new BackofferImpl(backoffCfg);
        RegionCache.RegionInfo current = regionInfo;
        while (true) {
            Coprocessor.Response resp;
            try {
                resp = sendToRegion(request, current);
            } catch (StatusRuntimeException e) {
                if (isNetworkError(e)) {
                    bo.backoff(Backoffer.Reason.NETWORK, e.getStatus().toString());
                    current = refreshOrBackoff(current, bo);
                    continue;
                }
                throw e;
            }
            if (!resp.hasRegionError()) {
                return resp;
            }
            handleRegionError(current, resp.getRegionError(), bo);
            current = refreshOrBackoff(current, bo);
        }
    }

    private RegionCache.RegionInfo refreshOrBackoff(RegionCache.RegionInfo current,
                                                     Backoffer bo) {
        var refreshed = regionCache.locateRegion(current.region().getId());
        if (refreshed.isPresent()) {
            return refreshed.get();
        }
        bo.backoff(Backoffer.Reason.REGION_MISS, "region gone: " + current.region().getId());
        return current;
    }

    private void handleRegionError(RegionCache.RegionInfo info, Errorpb.Error err,
                                    Backoffer bo) {
        if (err.hasNotLeader()) {
            var nl = err.getNotLeader();
            if (nl.hasLeader()) {
                regionCache.updateLeader(info.region().getId(), nl.getLeader());
            } else {
                regionCache.invalidate(info.region().getId());
            }
            bo.backoff(Backoffer.Reason.NOT_LEADER, "not leader");
            return;
        }
        if (err.hasEpochNotMatch()) {
            regionCache.invalidate(info.region().getId());
            bo.backoff(Backoffer.Reason.EPOCH_NOT_MATCH, "epoch not match");
            return;
        }
        if (err.hasRegionNotFound() || err.hasKeyNotInRegion() || err.hasRegionNotInitialized()) {
            regionCache.invalidate(info.region().getId());
            bo.backoff(Backoffer.Reason.REGION_MISS, "region missing/uninit");
            return;
        }
        if (err.hasStoreNotMatch() || err.hasMismatchPeerId()) {
            regionCache.invalidate(info.region().getId());
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
        if (err.hasRaftEntryTooLarge()) {
            throw new KvClientException(KvClientException.Category.OTHER,
                    "raft entry too large: " + err.getMessage());
        }
        log.debug("unknown coprocessor region error: {}", err);
        bo.backoff(Backoffer.Reason.OTHER, err.getMessage());
    }

    private Coprocessor.Response sendToRegion(Coprocessor.Request request,
                                               RegionCache.RegionInfo regionInfo) {
        var leader = regionInfo.leader();
        if (leader == null || leader.getStoreId() == 0) {
            return Coprocessor.Response.newBuilder()
                    .setOtherError("no leader for region " + regionInfo.region().getId())
                    .build();
        }
        TikvGrpc.TikvBlockingStub stub = storeCache.stubFor(leader.getStoreId());
        if (stub == null) {
            return Coprocessor.Response.newBuilder()
                    .setOtherError("cannot connect to store " + leader.getStoreId())
                    .build();
        }
        return stub.coprocessor(request);
    }

    private static boolean isNetworkError(StatusRuntimeException e) {
        var c = e.getStatus().getCode();
        return c == io.grpc.Status.Code.UNAVAILABLE
                || c == io.grpc.Status.Code.DEADLINE_EXCEEDED
                || c == io.grpc.Status.Code.CANCELLED;
    }

    private static int compare(byte[] a, byte[] b) {
        int len = Math.min(a.length, b.length);
        for (int i = 0; i < len; i++) {
            int cmp = (a[i] & 0xFF) - (b[i] & 0xFF);
            if (cmp != 0) return cmp;
        }
        return a.length - b.length;
    }

    @Override
    public void close() {
        pool.shutdown();
    }
}
