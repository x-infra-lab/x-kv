package io.github.xinfra.lab.xkv.test;

import io.github.xinfra.lab.xkv.client.TxnClient;
import io.github.xinfra.lab.xkv.client.XKvClient;
import io.github.xinfra.lab.xkv.client.config.ClientConfig;
import io.github.xinfra.lab.xkv.client.config.ClientConfig.RetryConfig;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Performance benchmark tests that output standardized latency reports.
 * Not JMH — lightweight enough to run in CI, detailed enough to track
 * regressions.
 */
@Timeout(value = 120, unit = TimeUnit.SECONDS)
final class BenchmarkE2ETest {

    @TempDir Path baseDir;
    private ClusterHarness harness;
    private XKvClient rawClient;
    private TxnClient txnClient;

    @BeforeEach
    void start() throws Exception {
        harness = new ClusterHarness(baseDir, 3).start();
        String pdAddr = "127.0.0.1:" + harness.pdPort();
        rawClient = XKvClient.create(ClientConfig.builder()
                .pdEndpoints(List.of(pdAddr))
                .build());
        txnClient = TxnClient.create(ClientConfig.builder()
                .pdEndpoints(List.of(pdAddr))
                .build());
    }

    @AfterEach
    void teardown() {
        if (rawClient != null) rawClient.close();
        if (txnClient != null) txnClient.close();
        if (harness != null) harness.close();
    }

    @Test
    void benchmarkRawPut() throws Exception {
        int workers = 4;
        int opsPerWorker = 1000;
        var hist = new LatencyHistogram();
        var barrier = new CyclicBarrier(workers);
        var done = new CountDownLatch(workers);
        var errors = new AtomicInteger();

        long startTime = System.nanoTime();
        for (int w = 0; w < workers; w++) {
            final int workerId = w;
            new Thread(() -> {
                try {
                    barrier.await(10, TimeUnit.SECONDS);
                    for (int i = 0; i < opsPerWorker; i++) {
                        byte[] key = String.format("bench-put-w%d-k%d", workerId, i).getBytes();
                        byte[] value = ("val-" + i).getBytes();
                        long t0 = System.nanoTime();
                        rawClient.raw().put(key, value);
                        hist.record(System.nanoTime() - t0);
                    }
                } catch (Throwable t) {
                    t.printStackTrace();
                    errors.incrementAndGet();
                } finally {
                    done.countDown();
                }
            }, "bench-put-" + w).start();
        }
        assertThat(done.await(90, TimeUnit.SECONDS)).isTrue();
        double elapsed = (System.nanoTime() - startTime) / 1e9;
        assertThat(errors.get()).isZero();
        System.out.println(hist.summary("rawPut", elapsed));
        writeJsonReport("rawPut", hist, elapsed);
        assertThat(hist.count()).isEqualTo((long) workers * opsPerWorker);
    }

    @Test
    void benchmarkRawGet() throws Exception {
        int totalKeys = 1000;
        for (int i = 0; i < totalKeys; i++) {
            rawClient.raw().put(
                    String.format("bench-get-k%04d", i).getBytes(),
                    ("value-" + i).getBytes());
        }

        int workers = 4;
        int opsPerWorker = 1000;
        var hist = new LatencyHistogram();
        var barrier = new CyclicBarrier(workers);
        var done = new CountDownLatch(workers);
        var errors = new AtomicInteger();

        long startTime = System.nanoTime();
        for (int w = 0; w < workers; w++) {
            final int workerId = w;
            new Thread(() -> {
                try {
                    barrier.await(10, TimeUnit.SECONDS);
                    var rnd = new Random(workerId);
                    for (int i = 0; i < opsPerWorker; i++) {
                        byte[] key = String.format("bench-get-k%04d", rnd.nextInt(totalKeys)).getBytes();
                        long t0 = System.nanoTime();
                        rawClient.raw().get(key);
                        hist.record(System.nanoTime() - t0);
                    }
                } catch (Throwable t) {
                    t.printStackTrace();
                    errors.incrementAndGet();
                } finally {
                    done.countDown();
                }
            }, "bench-get-" + w).start();
        }
        assertThat(done.await(90, TimeUnit.SECONDS)).isTrue();
        double elapsed = (System.nanoTime() - startTime) / 1e9;
        assertThat(errors.get()).isZero();
        System.out.println(hist.summary("rawGet", elapsed));
        writeJsonReport("rawGet", hist, elapsed);
        assertThat(hist.count()).isEqualTo((long) workers * opsPerWorker);
    }

    @Test
    void benchmarkTxnCommit() throws Exception {
        int workers = 4;
        int txnsPerWorker = 500;
        var hist = new LatencyHistogram();
        var barrier = new CyclicBarrier(workers);
        var done = new CountDownLatch(workers);
        var errors = new AtomicInteger();

        long startTime = System.nanoTime();
        for (int w = 0; w < workers; w++) {
            final int workerId = w;
            new Thread(() -> {
                try {
                    barrier.await(10, TimeUnit.SECONDS);
                    for (int i = 0; i < txnsPerWorker; i++) {
                        byte[] key = String.format("bench-txn-w%d-k%d", workerId, i).getBytes();
                        long t0 = System.nanoTime();
                        txnClient.executeWithRetry(txn -> {
                            txn.put(key, ("txn-val-" + workerId).getBytes());
                            return null;
                        });
                        hist.record(System.nanoTime() - t0);
                    }
                } catch (Throwable t) {
                    t.printStackTrace();
                    errors.incrementAndGet();
                } finally {
                    done.countDown();
                }
            }, "bench-txn-" + w).start();
        }
        assertThat(done.await(90, TimeUnit.SECONDS)).isTrue();
        double elapsed = (System.nanoTime() - startTime) / 1e9;
        assertThat(errors.get()).isZero();
        System.out.println(hist.summary("txnCommit", elapsed));
        writeJsonReport("txnCommit", hist, elapsed);
        assertThat(hist.count()).isEqualTo((long) workers * txnsPerWorker);
    }

    @Test
    void benchmarkTxnConflictRetry() throws Exception {
        int numKeys = 10;
        for (int i = 0; i < numKeys; i++) {
            txnClient.executeWithRetry(txn -> {
                txn.put(("bench-conflict-" + 0).getBytes(), "1000".getBytes());
                return null;
            });
        }
        for (int i = 0; i < numKeys; i++) {
            byte[] key = ("bench-conflict-" + i).getBytes();
            txnClient.executeWithRetry(txn -> {
                txn.put(key, "1000".getBytes());
                return null;
            });
        }

        int workers = 4;
        int transfersPerWorker = 50;
        var hist = new LatencyHistogram();
        var barrier = new CyclicBarrier(workers);
        var done = new CountDownLatch(workers);
        var errors = new AtomicInteger();
        var retryConfig = new RetryConfig(50, 2, 500);

        long startTime = System.nanoTime();
        for (int w = 0; w < workers; w++) {
            final int workerId = w;
            new Thread(() -> {
                try {
                    barrier.await(10, TimeUnit.SECONDS);
                    var rnd = new Random(workerId);
                    for (int i = 0; i < transfersPerWorker; i++) {
                        int from = rnd.nextInt(numKeys);
                        int to = (from + 1 + rnd.nextInt(numKeys - 1)) % numKeys;
                        byte[] fromKey = ("bench-conflict-" + from).getBytes();
                        byte[] toKey = ("bench-conflict-" + to).getBytes();
                        long t0 = System.nanoTime();
                        txnClient.executeWithRetry(txn -> {
                            int fromBal = Integer.parseInt(new String(txn.get(fromKey).orElse("1000".getBytes())));
                            int toBal = Integer.parseInt(new String(txn.get(toKey).orElse("1000".getBytes())));
                            txn.put(fromKey, Integer.toString(fromBal - 1).getBytes());
                            txn.put(toKey, Integer.toString(toBal + 1).getBytes());
                            return null;
                        }, retryConfig);
                        hist.record(System.nanoTime() - t0);
                    }
                } catch (Throwable t) {
                    t.printStackTrace();
                    errors.incrementAndGet();
                } finally {
                    done.countDown();
                }
            }, "bench-conflict-" + w).start();
        }
        assertThat(done.await(90, TimeUnit.SECONDS)).isTrue();
        double elapsed = (System.nanoTime() - startTime) / 1e9;
        assertThat(errors.get()).isZero();
        System.out.println(hist.summary("txnConflictRetry", elapsed));
        writeJsonReport("txnConflictRetry", hist, elapsed);
        assertThat(hist.count()).isEqualTo((long) workers * transfersPerWorker);

        // Verify conservation: sum of all balances should be numKeys * 1000.
        int totalBalance = 0;
        for (int i = 0; i < numKeys; i++) {
            byte[] key = ("bench-conflict-" + i).getBytes();
            final int idx = i;
            totalBalance += txnClient.executeWithRetry(txn -> {
                var v = txn.get(key);
                return Integer.parseInt(new String(v.orElse("1000".getBytes())));
            });
        }
        assertThat(totalBalance)
                .as("total balance should be conserved across conflicting transfers")
                .isEqualTo(numKeys * 1000);
    }

    private static final Path BENCHMARK_DIR = Path.of("target", "benchmark-results");

    private void writeJsonReport(String name, LatencyHistogram hist, double elapsed) {
        try {
            Files.createDirectories(BENCHMARK_DIR);
            Files.writeString(BENCHMARK_DIR.resolve(name + ".json"),
                    hist.toJson(name, elapsed) + "\n");
        } catch (IOException e) {
            System.err.println("Failed to write benchmark report for " + name + ": " + e.getMessage());
        }
    }
}
