package io.github.xinfra.lab.xkv.kv.server;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.google.protobuf.ByteString;
import io.github.xinfra.lab.xkv.kv.coprocessor.Coprocessor;
import io.github.xinfra.lab.xkv.kv.coprocessor.dag.VecDeadlineOp;
import io.github.xinfra.lab.xkv.proto.Coprocessor.BatchRequest;
import io.github.xinfra.lab.xkv.proto.Coprocessor.BatchResponse;
import io.github.xinfra.lab.xkv.proto.Coprocessor.KeyRange;
import io.github.xinfra.lab.xkv.proto.Coprocessor.Request;
import io.github.xinfra.lab.xkv.proto.Coprocessor.Response;
import io.github.xinfra.lab.xkv.proto.Coprocessor.StreamResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

public final class CoprocessorService {
    private static final Logger log = LoggerFactory.getLogger(CoprocessorService.class);

    private static final int CACHE_MAX_ENTRIES = 256;
    private static final long CACHE_TTL_SECONDS = 30;
    private static final long COP_SLOW_LOG_THRESHOLD_NS = 100_000_000L; // 100ms

    private final Map<Integer, Coprocessor> handlers = new ConcurrentHashMap<>();
    private final AtomicLong cacheVersion = new AtomicLong(1);

    private final Cache<CacheKey, CacheEntry> cache = Caffeine.newBuilder()
            .maximumSize(CACHE_MAX_ENTRIES)
            .expireAfterWrite(CACHE_TTL_SECONDS, TimeUnit.SECONDS)
            .build();

    // --- Metrics counters (atomic, lock-free) ---
    private final AtomicLong requestCount = new AtomicLong();
    private final AtomicLong requestErrorCount = new AtomicLong();
    private final AtomicLong cacheHitCount = new AtomicLong();
    private final AtomicLong cacheMissCount = new AtomicLong();
    private final AtomicLong deadlineExceededCount = new AtomicLong();
    private final AtomicLong totalExecNanos = new AtomicLong();

    public void register(Coprocessor handler) {
        handlers.put(handler.requestType(), handler);
    }

    public void invalidateCache() {
        cacheVersion.incrementAndGet();
        cache.invalidateAll();
    }

    public Response handle(Request req) {
        requestCount.incrementAndGet();
        var handler = handlers.get((int) req.getTp());
        if (handler == null) {
            requestErrorCount.incrementAndGet();
            return Response.newBuilder()
                    .setOtherError("unsupported coprocessor request type: " + req.getTp())
                    .build();
        }

        if (req.getIsCacheEnabled()) {
            long currentVersion = cacheVersion.get();
            CacheKey ck = CacheKey.of(req);

            if (req.getCacheIfMatchVersion() > 0
                    && req.getCacheIfMatchVersion() == currentVersion) {
                CacheEntry entry = cache.getIfPresent(ck);
                if (entry != null && entry.version == currentVersion) {
                    cacheHitCount.incrementAndGet();
                    return Response.newBuilder()
                            .setIsCacheHit(true)
                            .setCanBeCached(true)
                            .setCacheLastVersion(currentVersion)
                            .setData(entry.data)
                            .setExecDetailsMs(0)
                            .build();
                }
            }

            cacheMissCount.incrementAndGet();
            long t0 = System.nanoTime();
            Response resp;
            try {
                resp = handler.handle(req);
            } catch (VecDeadlineOp.DeadlineExceededException e) {
                deadlineExceededCount.incrementAndGet();
                long elapsed = System.nanoTime() - t0;
                totalExecNanos.addAndGet(elapsed);
                logSlowCop(req, elapsed, true);
                return Response.newBuilder().setOtherError(e.getMessage()).build();
            }
            long elapsed = System.nanoTime() - t0;
            totalExecNanos.addAndGet(elapsed);

            if (!resp.getOtherError().isEmpty()) requestErrorCount.incrementAndGet();
            logSlowCop(req, elapsed, false);

            if (resp.getOtherError().isEmpty() && !resp.hasLocked()) {
                cache.put(ck, new CacheEntry(resp.getData(), currentVersion));
                return resp.toBuilder()
                        .setCanBeCached(true)
                        .setCacheLastVersion(currentVersion)
                        .build();
            }
            return resp;
        }

        long t0 = System.nanoTime();
        Response resp;
        try {
            resp = handler.handle(req);
        } catch (VecDeadlineOp.DeadlineExceededException e) {
            deadlineExceededCount.incrementAndGet();
            long elapsed = System.nanoTime() - t0;
            totalExecNanos.addAndGet(elapsed);
            logSlowCop(req, elapsed, true);
            return Response.newBuilder().setOtherError(e.getMessage()).build();
        }
        long elapsed = System.nanoTime() - t0;
        totalExecNanos.addAndGet(elapsed);
        if (!resp.getOtherError().isEmpty()) requestErrorCount.incrementAndGet();
        logSlowCop(req, elapsed, false);
        return resp;
    }

    public void handleStream(Request req, Consumer<StreamResponse> sink) {
        requestCount.incrementAndGet();
        var handler = handlers.get((int) req.getTp());
        if (handler == null) {
            requestErrorCount.incrementAndGet();
            sink.accept(StreamResponse.newBuilder()
                    .setOtherError("unsupported coprocessor request type: " + req.getTp())
                    .build());
            return;
        }
        long t0 = System.nanoTime();
        handler.handleStream(req, sink);
        logSlowCop(req, System.nanoTime() - t0, false);
    }

    public void handleBatch(BatchRequest req, Consumer<BatchResponse> sink) {
        handleBatch(req, sink, null);
    }

    public void handleBatch(BatchRequest req, Consumer<BatchResponse> sink,
                             java.util.function.Function<Request, RegionCheckResult> regionValidator) {
        for (int i = 0; i < req.getRegionsCount(); i++) {
            var region = req.getRegions(i);
            var singleReq = Request.newBuilder()
                    .setTp(req.getTp())
                    .setData(req.getData())
                    .setStartTs(req.getStartTs())
                    .setPagingSize(req.getPagingSize())
                    .addAllRanges(region.getRangesList())
                    .build();
            try {
                if (regionValidator != null) {
                    var check = regionValidator.apply(singleReq);
                    if (check != null && check.error() != null) {
                        sink.accept(BatchResponse.newBuilder()
                                .setRegionError(check.error())
                                .setRegionId(region.getRegionId())
                                .build());
                        continue;
                    }
                    if (check != null && check.request() != null) {
                        singleReq = check.request();
                    }
                }

                var resp = handle(singleReq);
                sink.accept(BatchResponse.newBuilder()
                        .setData(resp.getData())
                        .setRegionId(region.getRegionId())
                        .build());
            } catch (Throwable t) {
                log.warn("batch coprocessor region={} failed", region.getRegionId(), t);
                sink.accept(BatchResponse.newBuilder()
                        .setOtherError(t.getMessage())
                        .setRegionId(region.getRegionId())
                        .build());
            }
        }
    }

    public record RegionCheckResult(io.github.xinfra.lab.xkv.proto.Errorpb.Error error,
                                     Request request) {}

    // --- Slow log ---

    private void logSlowCop(Request req, long elapsedNs, boolean deadlineExceeded) {
        if (elapsedNs < COP_SLOW_LOG_THRESHOLD_NS) return;
        long durationMs = elapsedNs / 1_000_000;
        log.warn("[COP-SLOW] tp={} duration_ms={} ranges={} cache_enabled={} deadline_exceeded={}",
                req.getTp(), durationMs, req.getRangesCount(),
                req.getIsCacheEnabled(), deadlineExceeded);
    }

    // --- Metrics accessors ---

    public long requestCount() { return requestCount.get(); }
    public long requestErrorCount() { return requestErrorCount.get(); }
    public long cacheHitCount() { return cacheHitCount.get(); }
    public long cacheMissCount() { return cacheMissCount.get(); }
    public long deadlineExceededCount() { return deadlineExceededCount.get(); }
    public long totalExecNanos() { return totalExecNanos.get(); }

    public void recordDeadlineExceeded() {
        deadlineExceededCount.incrementAndGet();
    }

    // --- Cache key using full content (no hash collisions) ---

    static final class CacheKey {
        final long tp;
        final ByteString data;
        final long startTs;
        final List<KeyRange> ranges;

        CacheKey(long tp, ByteString data, long startTs, List<KeyRange> ranges) {
            this.tp = tp;
            this.data = data;
            this.startTs = startTs;
            this.ranges = ranges;
        }

        static CacheKey of(Request req) {
            return new CacheKey(req.getTp(), req.getData(), req.getStartTs(),
                    req.getRangesList());
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (!(o instanceof CacheKey other)) return false;
            return tp == other.tp && startTs == other.startTs
                    && data.equals(other.data) && ranges.equals(other.ranges);
        }

        @Override
        public int hashCode() {
            return Objects.hash(tp, data, startTs, ranges);
        }
    }

    record CacheEntry(ByteString data, long version) {}
}
