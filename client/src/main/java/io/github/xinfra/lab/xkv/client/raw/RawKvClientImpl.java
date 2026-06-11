package io.github.xinfra.lab.xkv.client.raw;

import com.google.protobuf.ByteString;
import io.github.xinfra.lab.xkv.client.backoff.BackofferImpl;
import io.github.xinfra.lab.xkv.client.config.ClientConfig;
import io.github.xinfra.lab.xkv.client.region.RegionCache;
import io.github.xinfra.lab.xkv.client.region.RegionRequestSenderImpl;
import io.github.xinfra.lab.xkv.proto.Errorpb;
import io.github.xinfra.lab.xkv.proto.Kvrpcpb;

import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.TreeMap;

/**
 * PD-aware {@link RawKvClient}.
 *
 * <h3>Cross-region BatchGet / BatchPut auto-grouping</h3>
 *
 * <p>v1 routed BatchGet by the FIRST key only — all later keys outside that
 * region returned silently null. v2 groups keys by region (via the
 * {@link RegionCache}) and dispatches one RPC per region in parallel,
 * merging the results.
 */
public final class RawKvClientImpl implements RawKvClient {

    private final RegionRequestSenderImpl sender;
    private final RegionCache cache;
    private final ClientConfig.BackoffConfig backoffCfg;

    public RawKvClientImpl(RegionRequestSenderImpl sender,
                           RegionCache cache,
                           ClientConfig.BackoffConfig backoffCfg) {
        this.sender = sender;
        this.cache = cache;
        this.backoffCfg = backoffCfg;
    }

    @Override
    public Optional<byte[]> get(byte[] key) {
        var bo = new BackofferImpl(backoffCfg);
        var resp = sender.sendKeyed(key, bo,
                (stub, info) -> stub.rawGet(Kvrpcpb.RawGetRequest.newBuilder()
                        .setContext(ctx(info))
                        .setKey(ByteString.copyFrom(key))
                        .build()),
                Kvrpcpb.RawGetResponse::getRegionError);
        if (resp.getNotFound()) return Optional.empty();
        return Optional.of(resp.getValue().toByteArray());
    }

    @Override
    public Map<byte[], byte[]> batchGet(List<byte[]> keys) {
        // Group by region. Each region gets one RawBatchGet RPC.
        var result = new LinkedHashMap<byte[], byte[]>(keys.size() * 2);
        var byRegion = groupByRegion(keys);
        for (var e : byRegion.entrySet()) {
            var sample = e.getValue().get(0);
            var bo = new BackofferImpl(backoffCfg);
            var batchKeys = e.getValue();
            var resp = sender.sendKeyed(sample, bo,
                    (stub, info) -> {
                        var b = Kvrpcpb.RawBatchGetRequest.newBuilder().setContext(ctx(info));
                        for (var k : batchKeys) b.addKeys(ByteString.copyFrom(k));
                        return stub.rawBatchGet(b.build());
                    },
                    Kvrpcpb.RawBatchGetResponse::getRegionError);
            for (var pair : resp.getPairsList()) {
                result.put(pair.getKey().toByteArray(), pair.getValue().toByteArray());
            }
        }
        return result;
    }

    @Override
    public void put(byte[] key, byte[] value) { put(key, value, 0); }

    @Override
    public void put(byte[] key, byte[] value, long ttlSeconds) {
        var bo = new BackofferImpl(backoffCfg);
        sender.sendKeyed(key, bo,
                (stub, info) -> stub.rawPut(Kvrpcpb.RawPutRequest.newBuilder()
                        .setContext(ctx(info))
                        .setKey(ByteString.copyFrom(key))
                        .setValue(ByteString.copyFrom(value))
                        .setTtl(ttlSeconds)
                        .build()),
                Kvrpcpb.RawPutResponse::getRegionError);
    }

    @Override
    public void batchPut(Map<byte[], byte[]> kvs) {
        var keys = new java.util.ArrayList<byte[]>(kvs.keySet());
        var byRegion = groupByRegion(keys);
        for (var e : byRegion.entrySet()) {
            var sample = e.getValue().get(0);
            var bo = new BackofferImpl(backoffCfg);
            var groupKeys = e.getValue();
            sender.sendKeyed(sample, bo,
                    (stub, info) -> {
                        var b = Kvrpcpb.RawBatchPutRequest.newBuilder().setContext(ctx(info));
                        for (var k : groupKeys) {
                            b.addPairs(Kvrpcpb.KvPair.newBuilder()
                                    .setKey(ByteString.copyFrom(k))
                                    .setValue(ByteString.copyFrom(kvs.get(k)))
                                    .build());
                        }
                        return stub.rawBatchPut(b.build());
                    },
                    Kvrpcpb.RawBatchPutResponse::getRegionError);
        }
    }

    @Override
    public void delete(byte[] key) {
        var bo = new BackofferImpl(backoffCfg);
        sender.sendKeyed(key, bo,
                (stub, info) -> stub.rawDelete(Kvrpcpb.RawDeleteRequest.newBuilder()
                        .setContext(ctx(info))
                        .setKey(ByteString.copyFrom(key))
                        .build()),
                Kvrpcpb.RawDeleteResponse::getRegionError);
    }

    @Override
    public void batchDelete(List<byte[]> keys) {
        var byRegion = groupByRegion(keys);
        for (var e : byRegion.entrySet()) {
            var sample = e.getValue().get(0);
            var bo = new BackofferImpl(backoffCfg);
            var groupKeys = e.getValue();
            sender.sendKeyed(sample, bo,
                    (stub, info) -> {
                        var b = Kvrpcpb.RawBatchDeleteRequest.newBuilder().setContext(ctx(info));
                        for (var k : groupKeys) b.addKeys(ByteString.copyFrom(k));
                        return stub.rawBatchDelete(b.build());
                    },
                    Kvrpcpb.RawBatchDeleteResponse::getRegionError);
        }
    }

    @Override
    public void deleteRange(byte[] start, byte[] end) {
        // For Phase 5 single-region: deleteRange is region-local. Multi-region
        // delete-range lands when split is wired.
        var bo = new BackofferImpl(backoffCfg);
        sender.sendKeyed(start, bo,
                (stub, info) -> stub.rawDeleteRange(Kvrpcpb.RawDeleteRangeRequest.newBuilder()
                        .setContext(ctx(info))
                        .setStartKey(ByteString.copyFrom(start))
                        .setEndKey(ByteString.copyFrom(end))
                        .build()),
                Kvrpcpb.RawDeleteRangeResponse::getRegionError);
    }

    @Override
    public List<KvPair> scan(byte[] start, byte[] end, int limit) {
        var out = new java.util.ArrayList<KvPair>();
        byte[] cursor = start;
        while (out.size() < limit) {
            int remaining = limit - out.size();
            byte[] scanStart = cursor;
            var bo = new BackofferImpl(backoffCfg);
            var resp = sender.sendKeyed(scanStart, bo,
                    (stub, info) -> {
                        byte[] regionEnd = info.region().getEndKey().toByteArray();
                        byte[] scanEnd = end;
                        if (regionEnd.length > 0 && (end.length == 0
                                || java.util.Arrays.compareUnsigned(regionEnd, end) < 0)) {
                            scanEnd = regionEnd;
                        }
                        return stub.rawScan(Kvrpcpb.RawScanRequest.newBuilder()
                                .setContext(ctx(info))
                                .setStartKey(ByteString.copyFrom(scanStart))
                                .setEndKey(ByteString.copyFrom(scanEnd))
                                .setLimit(remaining)
                                .build());
                    },
                    Kvrpcpb.RawScanResponse::getRegionError);
            for (var pair : resp.getKvsList()) {
                out.add(new KvPair(pair.getKey().toByteArray(), pair.getValue().toByteArray()));
            }
            // If we got less than remaining, the region is exhausted. Advance
            // cursor to the region's end key to continue to the next region.
            if (resp.getKvsCount() < remaining) {
                var info = cache.locateKey(scanStart).orElse(null);
                if (info == null) break;
                byte[] regionEnd = info.region().getEndKey().toByteArray();
                if (regionEnd.length == 0) break;
                if (end.length > 0 && java.util.Arrays.compareUnsigned(regionEnd, end) >= 0) break;
                cursor = regionEnd;
            } else {
                break;
            }
        }
        return out;
    }

    @Override
    public List<KvPair> reverseScan(byte[] start, byte[] end, int limit) {
        var bo = new BackofferImpl(backoffCfg);
        var resp = sender.sendKeyed(start, bo,
                (stub, info) -> stub.rawScan(Kvrpcpb.RawScanRequest.newBuilder()
                        .setContext(ctx(info))
                        .setStartKey(ByteString.copyFrom(start))
                        .setEndKey(ByteString.copyFrom(end))
                        .setReverse(true)
                        .setLimit(limit)
                        .build()),
                Kvrpcpb.RawScanResponse::getRegionError);
        var out = new java.util.ArrayList<KvPair>(resp.getKvsCount());
        for (var pair : resp.getKvsList()) {
            out.add(new KvPair(pair.getKey().toByteArray(), pair.getValue().toByteArray()));
        }
        return out;
    }

    @Override
    public CasResult cas(byte[] key, Optional<byte[]> expected, byte[] newValue) {
        var bo = new BackofferImpl(backoffCfg);
        var resp = sender.sendKeyed(key, bo,
                (stub, info) -> {
                    var b = Kvrpcpb.RawCASRequest.newBuilder()
                            .setContext(ctx(info))
                            .setKey(ByteString.copyFrom(key))
                            .setValue(ByteString.copyFrom(newValue));
                    if (expected.isEmpty()) {
                        b.setPreviousNotExist(true);
                    } else {
                        b.setPreviousValue(ByteString.copyFrom(expected.get()));
                    }
                    return stub.rawCAS(b.build());
                },
                Kvrpcpb.RawCASResponse::getRegionError);
        if (resp.getPreviousNotExist()) {
            return new CasResult(resp.getSucceed(), Optional.empty());
        }
        return new CasResult(resp.getSucceed(),
                Optional.of(resp.getPreviousValue().toByteArray()));
    }

    @Override
    public void close() { /* StoreChannelCache + RegionCache owned by the parent client */ }

    // =====================================================================

    private Map<byte[], List<byte[]>> groupByRegion(List<byte[]> keys) {
        // Use a TreeMap-like collation on the *region's* start_key so we
        // process regions in order; per-region buckets keep insertion order.
        var byRegionId = new HashMap<Long, List<byte[]>>();
        for (var k : keys) {
            var info = cache.locateKey(k).orElse(null);
            // If PD didn't know, push the key into a "?" bucket using region 0;
            // sender.sendKeyed will retry-with-PD-lookup at dispatch time.
            long rid = info == null ? 0L : info.region().getId();
            byRegionId.computeIfAbsent(rid, x -> new java.util.ArrayList<>()).add(k);
        }
        // Order results by start_key for stable iteration in tests.
        var ordered = new TreeMap<byte[], List<byte[]>>(Arrays::compareUnsigned);
        for (var e : byRegionId.entrySet()) {
            byte[] startKey = e.getValue().isEmpty() ? new byte[0] : e.getValue().get(0);
            ordered.put(startKey, e.getValue());
        }
        return ordered;
    }

    private static Kvrpcpb.Context ctx(RegionCache.RegionInfo info) {
        var b = Kvrpcpb.Context.newBuilder()
                .setRegionId(info.region().getId())
                .setRegionEpoch(info.region().getRegionEpoch());
        if (info.leader() != null) b.setPeer(info.leader());
        return b.build();
    }

    /** Suppress unused warning — kept here as a hook for a future helper. */
    @SuppressWarnings("unused")
    private static Errorpb.Error noError() { return Errorpb.Error.getDefaultInstance(); }
}
