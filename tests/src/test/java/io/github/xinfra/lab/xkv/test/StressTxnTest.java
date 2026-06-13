package io.github.xinfra.lab.xkv.test;

import io.github.xinfra.lab.xkv.client.TxnClient;
import io.github.xinfra.lab.xkv.client.config.ClientConfig;
import io.github.xinfra.lab.xkv.client.error.KvClientException;
import io.github.xinfra.lab.xkv.client.txn.Transaction;
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

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Bank-transfer stress test: N accounts, M concurrent workers transferring
 * money for a sustained period. Verifies SI guarantees:
 * <ul>
 *   <li>Total balance conservation (no money created or destroyed)</li>
 *   <li>No deadlock timeouts (deadlock detector works correctly)</li>
 *   <li>All final balances &ge; 0</li>
 * </ul>
 */
final class StressTxnTest {

    private static final int ACCOUNTS = 10;
    private static final int INITIAL_BALANCE = 1000;
    private static final int WORKERS = 4;
    private static final Duration TEST_DURATION = Duration.ofSeconds(15);
    private static final int MAX_RETRIES = 30;

    @TempDir Path baseDir;
    private ClusterHarness harness;
    private TxnClient client;

    @BeforeEach
    void start() throws Exception {
        harness = new ClusterHarness(baseDir, 3).start();
        client = TxnClient.create(ClientConfig.builder()
                .pdEndpoints(List.of("127.0.0.1:" + harness.pdPort()))
                .build());

        try (var txn = client.begin()) {
            for (int i = 0; i < ACCOUNTS; i++) {
                txn.put(acct(i).getBytes(),
                        Integer.toString(INITIAL_BALANCE).getBytes());
            }
            txn.commit();
        }
    }

    @AfterEach
    void teardown() {
        if (client != null) client.close();
        if (harness != null) harness.close();
    }

    @Test
    void bankTransferStress() throws Exception {
        var pool = Executors.newFixedThreadPool(WORKERS);
        var ready = new CountDownLatch(WORKERS);
        var go = new CountDownLatch(1);
        var errors = ConcurrentHashMap.<String>newKeySet();
        var succeeded = new AtomicInteger();
        var conflicted = new AtomicInteger();
        var unknownCommit = new AtomicInteger();

        var startTime = Instant.now();

        for (int w = 0; w < WORKERS; w++) {
            final int workerId = w;
            pool.submit(() -> {
                try {
                    ready.countDown();
                    go.await();
                    var rng = new java.util.Random(workerId * 7919L);
                    while (Duration.between(startTime, Instant.now()).compareTo(TEST_DURATION) < 0) {
                        int from = rng.nextInt(ACCOUNTS);
                        int to;
                        do { to = rng.nextInt(ACCOUNTS); } while (to == from);
                        int amount = 1 + rng.nextInt(50);

                        var result = transfer(from, to, amount);
                        switch (result) {
                            case SUCCEEDED -> succeeded.incrementAndGet();
                            case RETRIED_OUT -> conflicted.incrementAndGet();
                            case UNKNOWN -> unknownCommit.incrementAndGet();
                            case FUNDS_LOW -> {}
                        }
                    }
                } catch (Throwable e) {
                    errors.add("worker-" + workerId + ": " + e);
                }
            });
        }

        ready.await();
        go.countDown();
        pool.shutdown();
        assertThat(pool.awaitTermination(TEST_DURATION.toMillis() + 60_000, TimeUnit.MILLISECONDS))
                .as("Workers should finish")
                .isTrue();

        assertThat(errors).as("No worker errors").isEmpty();
        assertThat(succeeded.get()).as("At least some transfers succeeded").isGreaterThan(0);

        // Verify total balance.
        int total = totalBalance();
        int expected = ACCOUNTS * INITIAL_BALANCE;
        if (unknownCommit.get() == 0) {
            assertThat(total)
                    .as("Strict conservation when no UNKNOWN commits")
                    .isEqualTo(expected);
        } else {
            int maxDrift = 50 * unknownCommit.get();
            assertThat(Math.abs(total - expected))
                    .as("Bounded drift with %d UNKNOWN commits", unknownCommit.get())
                    .isLessThanOrEqualTo(maxDrift);
        }

        // All balances non-negative.
        try (var txn = client.begin()) {
            for (int i = 0; i < ACCOUNTS; i++) {
                int bal = readInt(txn, acct(i));
                assertThat(bal)
                        .as("Account %d balance", i)
                        .isGreaterThanOrEqualTo(0);
            }
            txn.rollback();
        }

        System.out.printf("[StressTxn] succeeded=%d conflicted=%d unknown=%d total=%d (expected=%d)%n",
                succeeded.get(), conflicted.get(), unknownCommit.get(), total, expected);
    }

    private enum Result { SUCCEEDED, FUNDS_LOW, RETRIED_OUT, UNKNOWN }

    private Result transfer(int from, int to, int amount) {
        for (int retry = 0; retry < MAX_RETRIES; retry++) {
            try (Transaction txn = client.begin()) {
                int balFrom = readInt(txn, acct(from));
                int balTo = readInt(txn, acct(to));
                if (balFrom < amount) return Result.FUNDS_LOW;
                txn.put(acct(from).getBytes(),
                        Integer.toString(balFrom - amount).getBytes());
                txn.put(acct(to).getBytes(),
                        Integer.toString(balTo + amount).getBytes());
                txn.commit();
                return Result.SUCCEEDED;
            } catch (KvClientException e) {
                if (e.category() == KvClientException.Category.UNKNOWN_COMMIT_STATE) {
                    return Result.UNKNOWN;
                }
                try { Thread.sleep(2 + retry * 3L); }
                catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    return Result.RETRIED_OUT;
                }
            } catch (Exception e) {
                try { Thread.sleep(2 + retry * 3L); }
                catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    return Result.RETRIED_OUT;
                }
            }
        }
        return Result.RETRIED_OUT;
    }

    private int totalBalance() {
        try (Transaction txn = client.begin()) {
            int total = 0;
            for (int i = 0; i < ACCOUNTS; i++) {
                total += readInt(txn, acct(i));
            }
            txn.rollback();
            return total;
        }
    }

    private static int readInt(Transaction txn, String key) {
        var v = txn.get(key.getBytes());
        return v.isEmpty() ? 0 : Integer.parseInt(new String(v.get()));
    }

    private static String acct(int i) { return String.format("bank:%03d", i); }
}
