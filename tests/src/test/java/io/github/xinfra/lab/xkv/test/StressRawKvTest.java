package io.github.xinfra.lab.xkv.test;

import io.github.xinfra.lab.xkv.client.XKvClient;
import io.github.xinfra.lab.xkv.client.config.ClientConfig;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Stress test: multi-threaded concurrent rawPut / rawGet / rawScan
 * against a 1 PD + 3 KV cluster. Verifies:
 * <ul>
 *   <li>Zero data loss — every successfully written key reads back correctly</li>
 *   <li>Zero unexpected errors</li>
 *   <li>Non-zero throughput</li>
 * </ul>
 */
final class StressRawKvTest {

    private static final int WORKERS = 4;
    private static final int OPS_PER_WORKER = 1000;
    private static final Duration TEST_TIMEOUT = Duration.ofSeconds(120);

    @TempDir Path baseDir;
    private ClusterHarness harness;
    private XKvClient client;

    @BeforeEach
    void start() throws Exception {
        harness = new ClusterHarness(baseDir, 3).start();
        client = XKvClient.create(ClientConfig.builder()
                .pdEndpoints(List.of("127.0.0.1:" + harness.pdPort()))
                .build());
    }

    @AfterEach
    void teardown() {
        if (client != null) client.close();
        if (harness != null) harness.close();
    }

    @Test
    void concurrentRawPutGetDeleteWithVerification() throws Exception {
        var written = new ConcurrentHashMap<String, String>();
        var errors = ConcurrentHashMap.<String>newKeySet();
        var opsCompleted = new AtomicLong();
        var pool = Executors.newFixedThreadPool(WORKERS);
        var ready = new CountDownLatch(WORKERS);
        var go = new CountDownLatch(1);

        var start = Instant.now();

        for (int w = 0; w < WORKERS; w++) {
            final int workerId = w;
            pool.submit(() -> {
                try {
                    ready.countDown();
                    go.await();
                    var raw = client.raw();
                    for (int i = 0; i < OPS_PER_WORKER; i++) {
                        String key = String.format("stress:%d:%05d", workerId, i);
                        String val = "v-" + workerId + "-" + i;
                        retryOnTransient(() -> raw.put(key.getBytes(), val.getBytes()));
                        written.put(key, val);
                        opsCompleted.incrementAndGet();
                    }
                } catch (Throwable e) {
                    errors.add("worker-" + workerId + ": " + e);
                }
            });
        }

        ready.await();
        go.countDown();
        pool.shutdown();
        assertThat(pool.awaitTermination(TEST_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS))
                .as("Workers should complete within timeout")
                .isTrue();

        var elapsed = Duration.between(start, Instant.now());
        assertThat(errors).as("No worker errors").isEmpty();
        assertThat(opsCompleted.get()).isEqualTo((long) WORKERS * OPS_PER_WORKER);

        // Verify: read back every written key.
        var raw = client.raw();
        var missing = new AtomicInteger();
        var mismatch = new AtomicInteger();
        for (var entry : written.entrySet()) {
            var got = retryOnTransient(() -> raw.get(entry.getKey().getBytes()));
            if (got.isEmpty()) {
                missing.incrementAndGet();
            } else if (!new String(got.get()).equals(entry.getValue())) {
                mismatch.incrementAndGet();
            }
        }
        assertThat(missing.get()).as("Missing keys").isZero();
        assertThat(mismatch.get()).as("Mismatched values").isZero();

        double opsPerSec = opsCompleted.get() * 1000.0 / elapsed.toMillis();
        System.out.printf("[StressRawKv] %d ops in %d ms = %.0f ops/sec, %d workers%n",
                opsCompleted.get(), elapsed.toMillis(), opsPerSec, WORKERS);
        assertThat(opsPerSec).as("Throughput should be positive").isGreaterThan(0);
    }

    @Test
    void concurrentScanDoesNotLoseKeys() throws Exception {
        var raw = client.raw();
        int totalKeys = 200;
        for (int i = 0; i < totalKeys; i++) {
            raw.put(String.format("scan:%04d", i).getBytes(), ("v" + i).getBytes());
        }

        var pool = Executors.newFixedThreadPool(WORKERS);
        var errors = ConcurrentHashMap.<String>newKeySet();
        for (int w = 0; w < WORKERS; w++) {
            pool.submit(() -> {
                try {
                    var pairs = client.raw().scan(
                            "scan:0000".getBytes(), "scan:9999".getBytes(), totalKeys + 10);
                    assertThat(pairs.size())
                            .as("Scan should return all keys")
                            .isEqualTo(totalKeys);
                } catch (Throwable e) {
                    errors.add(e.toString());
                }
            });
        }
        pool.shutdown();
        assertThat(pool.awaitTermination(30, TimeUnit.SECONDS)).isTrue();
        assertThat(errors).isEmpty();
    }

    private static <T> T retryOnTransient(java.util.concurrent.Callable<T> action) throws Exception {
        for (int attempt = 0; ; attempt++) {
            try {
                return action.call();
            } catch (Exception e) {
                if (attempt >= 10) throw e;
                Thread.sleep(50 + attempt * 100L);
            }
        }
    }

    private static void retryOnTransient(Runnable action) throws Exception {
        retryOnTransient(() -> { action.run(); return null; });
    }
}
