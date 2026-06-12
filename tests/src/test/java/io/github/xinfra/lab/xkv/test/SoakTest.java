package io.github.xinfra.lab.xkv.test;

import io.github.xinfra.lab.xkv.client.TxnClient;
import io.github.xinfra.lab.xkv.client.XKvClient;
import io.github.xinfra.lab.xkv.client.config.ClientConfig;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Long-running stability ("soak") test.
 *
 * <p>Disabled by default — run manually with:
 * <pre>
 * mvn test -pl tests -Dtest=SoakTest -Dsoak.duration.minutes=1440
 * </pre>
 *
 * <p>Default duration: 5 minutes (set via {@code soak.duration.minutes}).
 *
 * <p>Exercises raw KV and transactional workloads concurrently against a
 * 3-node cluster with periodic chaos (random follower kill/restart) and
 * invariant checks (bank-transfer balance conservation).
 */
@Disabled("soak test — run manually via -Dtest=SoakTest")
final class SoakTest {

    private static final Logger log = LoggerFactory.getLogger(SoakTest.class);

    private static final int KV_NODES = 3;
    private static final int RAW_WORKERS = 4;
    private static final int TXN_WORKERS = 2;
    private static final int BANK_ACCOUNTS = 10;
    private static final int INITIAL_BALANCE = 1000;
    private static final long INVARIANT_CHECK_INTERVAL_MS = 10_000;
    private static final long CHAOS_MIN_INTERVAL_MS = 5_000;
    private static final long CHAOS_MAX_JITTER_MS = 3_000;

    @TempDir Path baseDir;
    private ClusterHarness harness;
    private XKvClient rawClient;
    private TxnClient txnClient;

    @BeforeEach
    void start() throws Exception {
        harness = new ClusterHarness(baseDir, KV_NODES).start();
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
    void soak() throws Exception {
        long durationMinutes = Long.parseLong(
                System.getProperty("soak.duration.minutes", "5"));
        long durationMs = Duration.ofMinutes(durationMinutes).toMillis();
        log.info("SOAK: starting {}min run ({} raw workers, {} txn workers, chaos enabled)",
                durationMinutes, RAW_WORKERS, TXN_WORKERS);

        seedAccounts();

        var stop = new AtomicBoolean(false);
        var rawOps = new AtomicLong();
        var rawErrors = new AtomicLong();
        var txnOps = new AtomicLong();
        var txnErrors = new AtomicLong();
        var txnConflictRetries = new AtomicLong();
        var invariantChecks = new AtomicLong();
        var invariantFailures = new AtomicLong();
        var chaosKills = new AtomicLong();
        var rawHist = new LatencyHistogram();
        var txnHist = new LatencyHistogram();

        int totalWorkers = RAW_WORKERS + TXN_WORKERS + 2;
        var done = new CountDownLatch(RAW_WORKERS + TXN_WORKERS);

        for (int w = 0; w < RAW_WORKERS; w++) {
            final int id = w;
            startDaemon("soak-raw-" + id, () ->
                    rawWorker(id, stop, rawOps, rawErrors, rawHist, done));
        }

        for (int w = 0; w < TXN_WORKERS; w++) {
            final int id = w;
            startDaemon("soak-txn-" + id, () ->
                    txnWorker(id, stop, txnOps, txnErrors, txnConflictRetries, txnHist, done));
        }

        var stopChaos = new AtomicBoolean(false);
        var chaosThread = startDaemon("soak-chaos", () ->
                chaosLoop(stopChaos, chaosKills));

        var stopInvariant = new AtomicBoolean(false);
        var invariantThread = startDaemon("soak-invariant", () ->
                invariantLoop(stopInvariant, invariantChecks, invariantFailures));

        Instant startTime = Instant.now();
        long reportIntervalMs = 60_000;
        long nextReport = System.currentTimeMillis() + reportIntervalMs;

        while (System.currentTimeMillis() < startTime.toEpochMilli() + durationMs) {
            Thread.sleep(1_000);
            if (System.currentTimeMillis() >= nextReport) {
                long elapsed = Duration.between(startTime, Instant.now()).toSeconds();
                log.info("SOAK [{}s]: raw_ops={} raw_err={} txn_ops={} txn_err={} "
                                + "txn_retries={} chaos_kills={} inv_checks={} inv_fail={}",
                        elapsed, rawOps.get(), rawErrors.get(),
                        txnOps.get(), txnErrors.get(), txnConflictRetries.get(),
                        chaosKills.get(), invariantChecks.get(), invariantFailures.get());
                nextReport += reportIntervalMs;
            }
        }

        stopChaos.set(true);
        chaosThread.join(15_000);
        Thread.sleep(3_000);

        stopInvariant.set(true);
        invariantThread.join(5_000);

        stop.set(true);
        done.await(30, TimeUnit.SECONDS);

        long totalElapsed = Duration.between(startTime, Instant.now()).toSeconds();
        log.info("SOAK COMPLETE [{}s]: raw_ops={} raw_err={} txn_ops={} txn_err={} "
                        + "chaos_kills={} inv_checks={} inv_fail={}",
                totalElapsed, rawOps.get(), rawErrors.get(),
                txnOps.get(), txnErrors.get(), chaosKills.get(),
                invariantChecks.get(), invariantFailures.get());
        log.info("RAW latency: {}", rawHist.summary("rawKv", totalElapsed));
        log.info("TXN latency: {}", txnHist.summary("txnKv", totalElapsed));

        assertThat(rawOps.get()).as("raw ops completed").isPositive();
        assertThat(txnOps.get()).as("txn ops completed").isPositive();
        assertThat(chaosKills.get()).as("chaos events fired").isPositive();
        assertThat(invariantChecks.get()).as("invariant checks ran").isPositive();
        assertThat(invariantFailures.get()).as("invariant failures").isZero();
    }

    private void seedAccounts() {
        for (int i = 0; i < BANK_ACCOUNTS; i++) {
            byte[] key = accountKey(i);
            txnClient.executeWithRetry(txn -> {
                txn.put(key, Integer.toString(INITIAL_BALANCE).getBytes());
                return null;
            });
        }
        log.info("SOAK: seeded {} accounts with balance={}", BANK_ACCOUNTS, INITIAL_BALANCE);
    }

    private void rawWorker(int id, AtomicBoolean stop, AtomicLong ops,
                           AtomicLong errors, LatencyHistogram hist,
                           CountDownLatch done) {
        var rnd = new Random(id * 7919L);
        int counter = 0;
        try {
            while (!stop.get()) {
                String key = "soak-raw-" + id + "-" + (counter++ % 500);
                long t0 = System.nanoTime();
                try {
                    if (rnd.nextBoolean()) {
                        rawClient.raw().put(key.getBytes(), ("v" + counter).getBytes());
                    } else {
                        rawClient.raw().get(key.getBytes());
                    }
                    hist.record(System.nanoTime() - t0);
                    ops.incrementAndGet();
                } catch (Exception e) {
                    errors.incrementAndGet();
                    Thread.sleep(50);
                }
            }
        } catch (InterruptedException ignored) {
            Thread.currentThread().interrupt();
        } finally {
            done.countDown();
        }
    }

    private void txnWorker(int id, AtomicBoolean stop, AtomicLong ops,
                           AtomicLong errors, AtomicLong retries,
                           LatencyHistogram hist, CountDownLatch done) {
        var rnd = new Random(id * 104729L);
        try {
            while (!stop.get()) {
                int from = rnd.nextInt(BANK_ACCOUNTS);
                int to;
                do { to = rnd.nextInt(BANK_ACCOUNTS); } while (to == from);
                int amount = 1 + rnd.nextInt(10);
                final int fTo = to;
                final int fAmount = amount;
                long t0 = System.nanoTime();
                try {
                    txnClient.executeWithRetry(txn -> {
                        byte[] fromKey = accountKey(from);
                        byte[] toKey = accountKey(fTo);
                        int balFrom = Integer.parseInt(
                                new String(txn.get(fromKey).orElse("0".getBytes())));
                        int balTo = Integer.parseInt(
                                new String(txn.get(toKey).orElse("0".getBytes())));
                        if (balFrom >= fAmount) {
                            txn.put(fromKey, Integer.toString(balFrom - fAmount).getBytes());
                            txn.put(toKey, Integer.toString(balTo + fAmount).getBytes());
                        }
                        return null;
                    });
                    hist.record(System.nanoTime() - t0);
                    ops.incrementAndGet();
                } catch (Exception e) {
                    errors.incrementAndGet();
                    Thread.sleep(50);
                }
            }
        } catch (InterruptedException ignored) {
            Thread.currentThread().interrupt();
        } finally {
            done.countDown();
        }
    }

    private void chaosLoop(AtomicBoolean stop, AtomicLong kills) {
        var rnd = new Random();
        while (!stop.get()) {
            try {
                Thread.sleep(CHAOS_MIN_INTERVAL_MS + rnd.nextLong(CHAOS_MAX_JITTER_MS));
            } catch (InterruptedException e) { return; }
            if (stop.get()) return;

            var followers = harness.kvNodes().stream()
                    .filter(n -> !n.peer.isLeader()).toList();
            if (followers.isEmpty()) continue;
            var victim = followers.get(rnd.nextInt(followers.size()));

            try {
                log.info("SOAK CHAOS: killing follower peer={}", victim.peerId);
                Map<Long, String> peerAddrs = victim.peerAddrs;
                victim.shutdown();
                harness.kvNodes().remove(victim);
                kills.incrementAndGet();
                Thread.sleep(2000 + rnd.nextInt(2000));
                log.info("SOAK CHAOS: restarting peer={}", victim.peerId);
                harness.restartNode(victim.peerId, peerAddrs);
            } catch (Exception e) {
                log.warn("SOAK CHAOS error: {}", e.getMessage());
            }
        }
    }

    private void invariantLoop(AtomicBoolean stop, AtomicLong checks,
                               AtomicLong failures) {
        while (!stop.get()) {
            try {
                Thread.sleep(INVARIANT_CHECK_INTERVAL_MS);
            } catch (InterruptedException e) { return; }
            if (stop.get()) return;
            try {
                int total = txnClient.executeWithRetry(txn -> {
                    int sum = 0;
                    for (int i = 0; i < BANK_ACCOUNTS; i++) {
                        var val = txn.get(accountKey(i));
                        sum += Integer.parseInt(new String(val.orElse("0".getBytes())));
                    }
                    return sum;
                });
                checks.incrementAndGet();
                if (total != BANK_ACCOUNTS * INITIAL_BALANCE) {
                    log.error("SOAK INVARIANT VIOLATION: expected={} actual={}",
                            BANK_ACCOUNTS * INITIAL_BALANCE, total);
                    failures.incrementAndGet();
                }
            } catch (Exception e) {
                log.warn("SOAK invariant check failed: {}", e.getMessage());
            }
        }
    }

    private static byte[] accountKey(int i) {
        return String.format("soak-acct:%03d", i).getBytes();
    }

    private static Thread startDaemon(String name, Runnable task) {
        var t = new Thread(task, name);
        t.setDaemon(true);
        t.start();
        return t;
    }
}
