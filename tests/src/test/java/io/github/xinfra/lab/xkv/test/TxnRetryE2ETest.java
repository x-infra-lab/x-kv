package io.github.xinfra.lab.xkv.test;

import io.github.xinfra.lab.xkv.client.TxnClient;
import io.github.xinfra.lab.xkv.client.config.ClientConfig;
import io.github.xinfra.lab.xkv.client.config.ClientConfig.RetryConfig;
import io.github.xinfra.lab.xkv.client.error.KvClientException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@Timeout(value = 60, unit = TimeUnit.SECONDS)
final class TxnRetryE2ETest {

    @TempDir Path baseDir;
    private ClusterHarness harness;
    private TxnClient txnClient;

    @BeforeEach
    void start() throws Exception {
        harness = new ClusterHarness(baseDir, 3).start();
        String pdAddr = "127.0.0.1:" + harness.pdPort();
        txnClient = TxnClient.create(ClientConfig.builder()
                .pdEndpoints(List.of(pdAddr))
                .build());
    }

    @AfterEach
    void teardown() {
        if (txnClient != null) txnClient.close();
        if (harness != null) harness.close();
    }

    @Test
    void retryOnWriteConflict() throws Exception {
        byte[] key = "counter".getBytes();

        // Seed key.
        txnClient.executeWithRetry(txn -> {
            txn.put(key, "0".getBytes());
            return null;
        });

        int workers = 4;
        int incrementsPerWorker = 10;
        var barrier = new CyclicBarrier(workers);
        var done = new CountDownLatch(workers);
        var errors = new AtomicInteger();

        for (int w = 0; w < workers; w++) {
            new Thread(() -> {
                try {
                    barrier.await(10, TimeUnit.SECONDS);
                    for (int i = 0; i < incrementsPerWorker; i++) {
                        txnClient.executeWithRetry(txn -> {
                            var val = txn.get(key);
                            int cur = Integer.parseInt(new String(val.orElseThrow()));
                            txn.put(key, Integer.toString(cur + 1).getBytes());
                            return null;
                        }, new RetryConfig(50, 2, 500));
                    }
                } catch (Throwable t) {
                    t.printStackTrace();
                    errors.incrementAndGet();
                } finally {
                    done.countDown();
                }
            }, "retry-worker-" + w).start();
        }

        assertThat(done.await(30, TimeUnit.SECONDS)).isTrue();
        assertThat(errors.get()).isZero();

        // All increments should have been applied.
        int expected = workers * incrementsPerWorker;
        txnClient.executeWithRetry(txn -> {
            var val = txn.get(key);
            int finalVal = Integer.parseInt(new String(val.orElseThrow()));
            assertThat(finalVal)
                    .as("counter after %d concurrent increments", expected)
                    .isEqualTo(expected);
            return null;
        });
    }

    @Test
    void nonRetryableExceptionPropagates() {
        byte[] key = "deadlock-test".getBytes();

        // Seed.
        txnClient.executeWithRetry(txn -> {
            txn.put(key, "seed".getBytes());
            return null;
        });

        // An action that always throws a non-retryable exception.
        assertThatThrownBy(() ->
                txnClient.executeWithRetry(txn -> {
                    txn.get(key);
                    throw new KvClientException(
                            KvClientException.Category.DEADLOCK, "simulated deadlock");
                }, new RetryConfig(3, 1, 10))
        ).isInstanceOf(KvClientException.class)
                .satisfies(e -> assertThat(((KvClientException) e).category())
                        .isEqualTo(KvClientException.Category.DEADLOCK));
    }

    @Test
    void bankTransferWithRetry() throws Exception {
        byte[] aliceKey = "alice-retry".getBytes();
        byte[] bobKey = "bob-retry".getBytes();

        // Seed balances.
        txnClient.executeWithRetry(txn -> {
            txn.put(aliceKey, "1000".getBytes());
            txn.put(bobKey, "1000".getBytes());
            return null;
        });

        int workers = 4;
        int transfersPerWorker = 10;
        int amount = 10;
        var barrier = new CyclicBarrier(workers);
        var done = new CountDownLatch(workers);
        var errors = new AtomicInteger();

        for (int w = 0; w < workers; w++) {
            new Thread(() -> {
                try {
                    barrier.await(10, TimeUnit.SECONDS);
                    for (int i = 0; i < transfersPerWorker; i++) {
                        txnClient.executeWithRetry(txn -> {
                            int alice = Integer.parseInt(new String(txn.get(aliceKey).orElseThrow()));
                            int bob = Integer.parseInt(new String(txn.get(bobKey).orElseThrow()));
                            txn.put(aliceKey, Integer.toString(alice - amount).getBytes());
                            txn.put(bobKey, Integer.toString(bob + amount).getBytes());
                            return null;
                        }, new RetryConfig(50, 2, 500));
                    }
                } catch (Throwable t) {
                    t.printStackTrace();
                    errors.incrementAndGet();
                } finally {
                    done.countDown();
                }
            }, "bank-worker-" + w).start();
        }

        assertThat(done.await(30, TimeUnit.SECONDS)).isTrue();
        assertThat(errors.get()).isZero();

        // Total balance must be conserved: 2000.
        txnClient.executeWithRetry(txn -> {
            int alice = Integer.parseInt(new String(txn.get(aliceKey).orElseThrow()));
            int bob = Integer.parseInt(new String(txn.get(bobKey).orElseThrow()));
            assertThat(alice + bob)
                    .as("total balance conserved after concurrent transfers")
                    .isEqualTo(2000);
            // All transfers go alice→bob, so alice = 1000 - 4*10*10 = 600, bob = 1400.
            assertThat(alice).isEqualTo(1000 - workers * transfersPerWorker * amount);
            assertThat(bob).isEqualTo(1000 + workers * transfersPerWorker * amount);
            return null;
        });
    }
}
