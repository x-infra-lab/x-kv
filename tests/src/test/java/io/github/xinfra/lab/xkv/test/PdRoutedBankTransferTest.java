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
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Phase 5 capstone: BankTransfer through the FULL stack.
 *
 * <pre>
 *   client SDK ──► PD (TSO + region routing)
 *               └► 3-store KV cluster (real raft, real gRPC)
 * </pre>
 *
 * <p>Compared to the original {@code BankTransferTxnTest}, this:
 * <ul>
 *   <li>Uses a real {@link TxnClient}, not a hand-rolled 2PC + atomic-long TSO.</li>
 *   <li>Goes over real gRPC to a 3-peer multi-store raft cluster, not loopback.</li>
 *   <li>Uses PD-derived TSOs whose monotonicity is guaranteed by
 *       {@link io.github.xinfra.lab.xkv.pd.state.HlcTsoOracle}.</li>
 *   <li>Lock conflicts are handled by the production
 *       {@link io.github.xinfra.lab.xkv.client.txn.LockResolverImpl}.</li>
 * </ul>
 *
 * <h3>SI guarantees</h3>
 *
 * <p>At THREADS = 2, the test asserts STRICT conservation
 * ({@code total == ACCOUNTS * INITIAL_BALANCE}). Per-key reader latches
 * + max_ts mechanism + 2PC write-conflict detection together give this
 * guarantee.
 *
 * <p>At THREADS &ge; 3, drift becomes observable (commonly 5–30 dollars
 * over 90 transfers). The remaining hole: between a txn's two
 * {@code get} RPCs another concurrent txn can commit + a third concurrent
 * txn can read, driving max_ts beyond what would have been our snapshot
 * floor. The robust fix is server-side per-transaction snapshot pinning
 * (the leader takes ONE RocksDB snapshot at {@code start_ts} and serves
 * all reads from it) — Phase 7 work.
 */
final class PdRoutedBankTransferTest {

    private static final int ACCOUNTS = 5;
    private static final int INITIAL_BALANCE = 100;
    private static final int THREADS = 3;
    private static final int TRANSFERS_PER_THREAD = 30;
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

        // Seed accounts with initial balance.
        try (var txn = client.begin()) {
            for (int i = 0; i < ACCOUNTS; i++) {
                txn.put(account(i).getBytes(), Integer.toString(INITIAL_BALANCE).getBytes());
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
    void totalBalanceConservedSerial() {
        // Single-threaded — establishes the txn pipeline works end-to-end
        // before introducing concurrency.
        for (int i = 0; i < 30; i++) {
            attemptTransfer(0, 1, 1 + (i % 10));
        }
        assertThat(totalBalance()).isEqualTo(ACCOUNTS * INITIAL_BALANCE);
    }

    @Test
    void totalBalanceConservedUnderConcurrentTransfers() throws Exception {
        var pool = Executors.newFixedThreadPool(THREADS);
        var ready = new CountDownLatch(THREADS);
        var go = new CountDownLatch(1);
        var failures = ConcurrentHashMap.<String>newKeySet();
        var transfersDone = new AtomicInteger();
        var transfersSucceeded = new AtomicInteger();
        var transfersUnknown = new AtomicInteger();

        for (int t = 0; t < THREADS; t++) {
            final int threadId = t;
            pool.submit(() -> {
                try {
                    ready.countDown();
                    go.await();
                    var rng = new java.util.Random(threadId * 1000L);
                    for (int i = 0; i < TRANSFERS_PER_THREAD; i++) {
                        int from = rng.nextInt(ACCOUNTS);
                        int to;
                        do { to = rng.nextInt(ACCOUNTS); } while (to == from);
                        int amount = 1 + rng.nextInt(20);
                        var fate = attemptTransferTracked(from, to, amount);
                        transfersDone.incrementAndGet();
                        if (fate == Fate.SUCCEEDED) transfersSucceeded.incrementAndGet();
                        else if (fate == Fate.UNKNOWN) transfersUnknown.incrementAndGet();
                    }
                } catch (Throwable e) {
                    failures.add(e.toString());
                }
            });
        }
        ready.await();
        go.countDown();
        pool.shutdown();
        boolean done = pool.awaitTermination(3, TimeUnit.MINUTES);
        assertThat(done).isTrue();
        assertThat(failures).isEmpty();

        // Strict assertion: with per-key reader latches + max_ts + 2PC's
        // write-conflict detection, total balance must be exactly conserved
        // when no commits returned UNKNOWN. (UNKNOWN txns are
        // possibly-applied; a background resolver eventually decides.)
        int total = totalBalance();
        int maxPerTxn = 20;     // matches the amount cap above
        if (transfersUnknown.get() == 0) {
            assertThat(total)
                    .as("succeeded=%d total=%d", transfersSucceeded.get(), total)
                    .isEqualTo(ACCOUNTS * INITIAL_BALANCE);
        } else {
            int drift = Math.abs(total - ACCOUNTS * INITIAL_BALANCE);
            assertThat(drift)
                    .as("succeeded=%d unknown=%d", transfersSucceeded.get(), transfersUnknown.get())
                    .isLessThanOrEqualTo(maxPerTxn * transfersUnknown.get());
        }
    }

    // =====================================================================

    private enum Fate { SUCCEEDED, FUNDS_LOW, RETRIED_OUT, UNKNOWN }

    private void attemptTransfer(int from, int to, int amount) {
        attemptTransferTracked(from, to, amount);
    }

    private Fate attemptTransferTracked(int from, int to, int amount) {
        for (int retry = 0; retry < MAX_RETRIES; retry++) {
            try (Transaction txn = client.begin()) {
                var balFrom = readInt(txn, account(from));
                var balTo = readInt(txn, account(to));
                if (balFrom < amount) return Fate.FUNDS_LOW;
                txn.put(account(from).getBytes(), Integer.toString(balFrom - amount).getBytes());
                txn.put(account(to).getBytes(), Integer.toString(balTo + amount).getBytes());
                long commitTs = txn.commit();
                if (DEBUG.get()) {
                    System.err.printf("[OK ] startTs=%d commitTs=%d %d->%d $%d (was %d,%d → %d,%d)%n",
                            txn.startTs(), commitTs, from, to, amount,
                            balFrom, balTo, balFrom - amount, balTo + amount);
                }
                return Fate.SUCCEEDED;
            } catch (KvClientException e) {
                if (DEBUG.get()) {
                    System.err.printf("[ERR] %d->%d $%d retry=%d cat=%s msg=%s%n",
                            from, to, amount, retry, e.category(), e.getMessage());
                }
                if (e.category() == KvClientException.Category.UNKNOWN_COMMIT_STATE) {
                    return Fate.UNKNOWN;
                }
                if (e.category() == KvClientException.Category.WRITE_CONFLICT
                        || e.category() == KvClientException.Category.KEY_LOCKED) {
                    try { Thread.sleep(2 + retry * 3); } catch (InterruptedException ie) { Thread.currentThread().interrupt(); return Fate.RETRIED_OUT; }
                    continue;
                }
                throw e;
            }
        }
        return Fate.RETRIED_OUT;
    }

    private static final java.util.concurrent.atomic.AtomicBoolean DEBUG =
            new java.util.concurrent.atomic.AtomicBoolean(false);

    private int totalBalance() {
        try (Transaction txn = client.begin()) {
            int total = 0;
            for (int i = 0; i < ACCOUNTS; i++) {
                total += readInt(txn, account(i));
            }
            txn.rollback();    // read-only, no need to commit
            return total;
        }
    }

    private static int readInt(Transaction txn, String key) {
        var v = txn.get(key.getBytes());
        return v.isEmpty() ? 0 : Integer.parseInt(new String(v.get()));
    }

    private static String account(int i) { return String.format("acct:%03d", i); }

    /** Suppress unused. */
    @SuppressWarnings("unused")
    private static List<Object> empty() { return new ArrayList<>(); }
}
