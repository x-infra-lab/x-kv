package io.github.xinfra.lab.xkv.test;

import java.util.concurrent.atomic.LongAdder;

/**
 * Lock-free latency histogram for benchmark reporting.
 *
 * <p>Buckets: [0, 1ms), [1ms, 2ms), ..., [99ms, 100ms), [100ms+).
 * Each bucket uses a {@link LongAdder} for contention-free recording
 * from multiple benchmark threads.
 */
final class LatencyHistogram {

    private static final int BUCKET_COUNT = 101;
    private static final long BUCKET_WIDTH_NANOS = 1_000_000L; // 1ms
    private final LongAdder[] buckets;
    private final LongAdder totalCount = new LongAdder();
    private final LongAdder totalNanos = new LongAdder();
    private volatile long maxNanos = 0;

    LatencyHistogram() {
        buckets = new LongAdder[BUCKET_COUNT];
        for (int i = 0; i < BUCKET_COUNT; i++) buckets[i] = new LongAdder();
    }

    void record(long durationNanos) {
        int idx = (int) Math.min(durationNanos / BUCKET_WIDTH_NANOS, BUCKET_COUNT - 1);
        buckets[idx].increment();
        totalCount.increment();
        totalNanos.add(durationNanos);
        long cur = maxNanos;
        while (durationNanos > cur) {
            maxNanos = durationNanos;
            cur = maxNanos;
        }
    }

    long count() { return totalCount.sum(); }

    double percentileMs(double p) {
        long total = totalCount.sum();
        if (total == 0) return 0;
        long target = (long) Math.ceil(total * p);
        long running = 0;
        for (int i = 0; i < BUCKET_COUNT; i++) {
            running += buckets[i].sum();
            if (running >= target) {
                return i < BUCKET_COUNT - 1 ? (i + 0.5) : 100.0;
            }
        }
        return 100.0;
    }

    double maxMs() { return maxNanos / 1_000_000.0; }

    double avgMs() {
        long c = totalCount.sum();
        return c == 0 ? 0 : (totalNanos.sum() / 1_000_000.0) / c;
    }

    String summary(String label, double elapsedSeconds) {
        long c = count();
        double opsPerSec = elapsedSeconds > 0 ? c / elapsedSeconds : 0;
        return String.format("[Benchmark] %s: %d ops, avg=%.2fms, p50=%.2fms, p95=%.2fms, p99=%.2fms, max=%.2fms, throughput=%.0f ops/sec",
                label, c, avgMs(), percentileMs(0.50), percentileMs(0.95), percentileMs(0.99), maxMs(), opsPerSec);
    }

    String toJson(String label, double elapsedSeconds) {
        long c = count();
        double opsPerSec = elapsedSeconds > 0 ? c / elapsedSeconds : 0;
        return String.format(
                "{\"name\":\"%s\",\"ops\":%d,\"avg_ms\":%.3f,\"p50_ms\":%.3f,"
                + "\"p95_ms\":%.3f,\"p99_ms\":%.3f,\"max_ms\":%.3f,"
                + "\"throughput_ops_sec\":%.1f,\"elapsed_sec\":%.3f}",
                label, c, avgMs(), percentileMs(0.50), percentileMs(0.95),
                percentileMs(0.99), maxMs(), opsPerSec, elapsedSeconds);
    }
}
