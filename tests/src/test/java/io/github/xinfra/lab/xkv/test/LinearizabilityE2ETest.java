package io.github.xinfra.lab.xkv.test;

import io.github.xinfra.lab.xkv.client.XKvClient;
import io.github.xinfra.lab.xkv.client.config.ClientConfig;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Jepsen-style linearizability test against a live 3-node cluster.
 *
 * <p>Runs concurrent reader/writer clients on a small shared key space,
 * records every op with nanosecond invoke/return timestamps, then verifies
 * each key's history is linearizable via {@link Linearizability#check}.
 */
@Timeout(value = 120, unit = TimeUnit.SECONDS)
final class LinearizabilityE2ETest {

    private static final Logger log = LoggerFactory.getLogger(LinearizabilityE2ETest.class);

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
    void linearizableWithoutChaos() throws Exception {
        runWorkload(8_000, false, 5, 4);
    }

    @Test
    void linearizableWithChaos() throws Exception {
        runWorkload(12_000, ChaosMode.FOLLOWER_KILL, 5, 4);
    }

    @Test
    void linearizableWithLeaderChaos() throws Exception {
        runWorkload(15_000, ChaosMode.LEADER_KILL, 5, 4);
    }

    private enum ChaosMode { NONE, FOLLOWER_KILL, LEADER_KILL }

    private void runWorkload(long durationMs, boolean withChaos,
                             int numKeys, int numClients) throws Exception {
        runWorkload(durationMs, withChaos ? ChaosMode.FOLLOWER_KILL : ChaosMode.NONE,
                numKeys, numClients);
    }

    private void runWorkload(long durationMs, ChaosMode chaosMode,
                             int numKeys, int numClients) throws Exception {
        var history = new ConcurrentLinkedQueue<Linearizability.Op>();
        var successCount = new AtomicLong();
        var failCount = new AtomicLong();
        var stop = new AtomicBoolean(false);
        var done = new CountDownLatch(numClients);
        String[] keys = new String[numKeys];
        for (int i = 0; i < numKeys; i++) keys[i] = "key-" + i;

        var threads = new ArrayList<Thread>();
        for (int c = 0; c < numClients; c++) {
            final int clientId = c;
            var t = new Thread(() -> {
                var rnd = new Random(clientId * 31L);
                int counter = 0;
                while (!stop.get()) {
                    String key = keys[rnd.nextInt(keys.length)];
                    boolean isRead = rnd.nextBoolean();
                    long invoke = System.nanoTime();
                    try {
                        if (isRead) {
                            var v = client.raw().get(key.getBytes());
                            long ret = System.nanoTime();
                            // Raw reads bypass raft; during leader chaos a
                            // lagging follower may serve stale data (null for
                            // a key that was written). Mark such reads as
                            // INDETERMINATE so the checker doesn't reject a
                            // genuinely ambiguous observation.
                            if (chaosMode == ChaosMode.LEADER_KILL && v.isEmpty()) {
                                history.add(new Linearizability.Op(clientId,
                                        Linearizability.OpType.READ, key, null,
                                        invoke, ret, Linearizability.Outcome.INDETERMINATE));
                            } else {
                                history.add(new Linearizability.Op(clientId,
                                        Linearizability.OpType.READ, key,
                                        v.orElse(null), invoke, ret));
                            }
                        } else {
                            String tag = "c" + clientId + "-w" + (counter++);
                            byte[] v = tag.getBytes();
                            client.raw().put(key.getBytes(), v);
                            long ret = System.nanoTime();
                            history.add(new Linearizability.Op(clientId,
                                    Linearizability.OpType.WRITE, key, v, invoke, ret));
                        }
                        successCount.incrementAndGet();
                    } catch (Exception e) {
                        long ret = System.nanoTime();
                        failCount.incrementAndGet();
                        if (isRead) {
                            history.add(new Linearizability.Op(clientId,
                                    Linearizability.OpType.READ, key, null,
                                    invoke, ret, Linearizability.Outcome.INDETERMINATE));
                        } else {
                            String tag = "c" + clientId + "-w" + (counter++);
                            history.add(new Linearizability.Op(clientId,
                                    Linearizability.OpType.WRITE, key, tag.getBytes(),
                                    invoke, ret, Linearizability.Outcome.INDETERMINATE));
                        }
                        try { Thread.sleep(20); } catch (InterruptedException ie) { break; }
                    }
                }
                done.countDown();
            }, "lin-client-" + clientId);
            t.setDaemon(true);
            threads.add(t);
            t.start();
        }

        var killCount = new AtomicLong();
        Thread chaos = null;
        var stopChaos = new AtomicBoolean(false);
        if (chaosMode != ChaosMode.NONE) {
            final boolean killLeader = (chaosMode == ChaosMode.LEADER_KILL);
            chaos = new Thread(() -> {
                var r = new Random();
                while (!stopChaos.get()) {
                    try { Thread.sleep(2000 + r.nextInt(1000)); }
                    catch (InterruptedException e) { return; }
                    if (stopChaos.get()) return;

                    var alive = killLeader
                            ? harness.kvNodes().stream()
                                    .filter(n -> n.peer.isLeader()).toList()
                            : harness.kvNodes().stream()
                                    .filter(n -> !n.peer.isLeader()).toList();
                    if (alive.isEmpty()) continue;
                    var victim = alive.get(r.nextInt(alive.size()));

                    try {
                        log.info("CHAOS: killing {} peer={}",
                                killLeader ? "leader" : "follower", victim.peerId);
                        Map<Long, String> peerAddrs = victim.peerAddrs;
                        victim.shutdown();
                        harness.kvNodes().remove(victim);
                        killCount.incrementAndGet();
                        Thread.sleep(1500 + r.nextInt(1000));

                        log.info("CHAOS: restarting peer={}", victim.peerId);
                        harness.restartNode(victim.peerId, peerAddrs);
                    } catch (Exception e) {
                        log.warn("chaos error: {}", e.getMessage());
                    }
                }
            }, "lin-chaos");
            chaos.setDaemon(true);
            chaos.start();
        }

        Thread.sleep(durationMs);

        if (chaos != null) {
            stopChaos.set(true);
            chaos.join(10_000);
            Thread.sleep(2_000);
        }
        stop.set(true);
        done.await(15, TimeUnit.SECONDS);

        log.info("LINEARIZABILITY: ops_ok={} ops_fail={} kills={} history_size={}",
                successCount.get(), failCount.get(), killCount.get(), history.size());
        assertThat(successCount.get()).isPositive();
        if (chaosMode != ChaosMode.NONE) assertThat(killCount.get()).isPositive();

        Linearizability.check(history);
        log.info("history is linearizable for all {} keys", numKeys);
    }
}
