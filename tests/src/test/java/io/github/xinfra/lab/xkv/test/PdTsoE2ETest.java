package io.github.xinfra.lab.xkv.test;

import io.github.xinfra.lab.xkv.pd.config.PdConfig;
import io.github.xinfra.lab.xkv.pd.config.PdConfig.PeerAddress;
import io.github.xinfra.lab.xkv.pd.server.PdServer;
import io.github.xinfra.lab.xkv.proto.PDGrpc;
import io.github.xinfra.lab.xkv.proto.Pdpb;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.stub.StreamObserver;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * Validates TSO (Timestamp Oracle) monotonicity across PD leader changes.
 *
 * <p>Covers:
 * <ul>
 *   <li>Timestamps are strictly monotonically increasing within a leader's tenure</li>
 *   <li>After leader failover, new leader's timestamps are strictly greater than
 *       all timestamps issued by the old leader</li>
 *   <li>Followers reject TSO requests with "not PD leader" error</li>
 * </ul>
 */
@Timeout(value = 90, unit = TimeUnit.SECONDS)
final class PdTsoE2ETest {

    @TempDir Path tempDir;
    private final List<PdServer> servers = new ArrayList<>();
    private final List<ManagedChannel> channels = new ArrayList<>();

    @AfterEach
    void teardown() {
        for (var ch : channels) {
            try { ch.shutdownNow().awaitTermination(2, TimeUnit.SECONDS); } catch (Exception e) { e.printStackTrace(); }
        }
        channels.clear();
        for (var s : servers) {
            try { s.stop(); } catch (Exception e) { e.printStackTrace(); }
        }
        servers.clear();
        TestCluster.releaseAllPorts();
    }

    @Test
    void tsoIsMonotonicAcrossLeaderChanges() throws Exception {
        int[] clientPorts = new int[3];
        int[] raftPorts = new int[3];
        for (int i = 0; i < 3; i++) {
            clientPorts[i] = TestCluster.freePort();
            raftPorts[i] = TestCluster.freePort();
        }

        var peers = List.of(
                new PeerAddress(1, "127.0.0.1:" + raftPorts[0], "127.0.0.1:" + clientPorts[0]),
                new PeerAddress(2, "127.0.0.1:" + raftPorts[1], "127.0.0.1:" + clientPorts[1]),
                new PeerAddress(3, "127.0.0.1:" + raftPorts[2], "127.0.0.1:" + clientPorts[2])
        );

        for (int i = 0; i < 3; i++) {
            var cfg = PdConfig.builder()
                    .nodeId(i + 1)
                    .clusterId(1)
                    .clientAddress("127.0.0.1:" + clientPorts[i])
                    .raftAddress("127.0.0.1:" + raftPorts[i])
                    .dataDir(tempDir.resolve("pd-" + (i + 1)))
                    .peers(peers)
                    .build();
            TestCluster.releasePort(clientPorts[i]);
            TestCluster.releasePort(raftPorts[i]);
            var srv = new PdServer(cfg);
            srv.start();
            servers.add(srv);
        }

        // Wait for leader.
        await().atMost(15, TimeUnit.SECONDS).pollInterval(200, TimeUnit.MILLISECONDS)
                .until(() -> servers.stream().anyMatch(s ->
                        s.raftNode() != null && s.raftNode().isLeader()));

        int leaderIdx = findLeaderIdx();

        // ====== Phase 1: Allocate timestamps on initial leader ======

        List<Long> timestampsPhase1 = allocTimestamps(clientPorts[leaderIdx], 200);
        assertThat(timestampsPhase1).hasSize(200);

        // Verify strict monotonicity within phase 1.
        for (int i = 1; i < timestampsPhase1.size(); i++) {
            assertThat(timestampsPhase1.get(i))
                    .as("TSO must be strictly increasing: ts[%d]=%d vs ts[%d]=%d",
                            i - 1, timestampsPhase1.get(i - 1), i, timestampsPhase1.get(i))
                    .isGreaterThan(timestampsPhase1.get(i - 1));
        }

        long maxTsBeforeFailover = timestampsPhase1.get(timestampsPhase1.size() - 1);

        // ====== Phase 2: Kill leader, wait for new leader ======

        int stoppedIdx = leaderIdx;
        servers.get(stoppedIdx).stop();

        await().atMost(15, TimeUnit.SECONDS).pollInterval(300, TimeUnit.MILLISECONDS)
                .until(() -> {
                    for (int i = 0; i < 3; i++) {
                        if (i == stoppedIdx) continue;
                        var rn = servers.get(i).raftNode();
                        if (rn != null && rn.isLeader()) return true;
                    }
                    return false;
                });

        int newLeaderIdx = -1;
        for (int i = 0; i < 3; i++) {
            if (i == stoppedIdx) continue;
            var rn = servers.get(i).raftNode();
            if (rn != null && rn.isLeader()) { newLeaderIdx = i; break; }
        }
        assertThat(newLeaderIdx).isGreaterThanOrEqualTo(0);

        // ====== Phase 3: Allocate timestamps on new leader ======

        List<Long> timestampsPhase2 = allocTimestamps(clientPorts[newLeaderIdx], 200);
        assertThat(timestampsPhase2).hasSize(200);

        // Cross-leader monotonicity: every new timestamp > max old timestamp.
        assertThat(timestampsPhase2.get(0))
                .as("First TSO from new leader (%d) must be > max from old leader (%d)",
                        timestampsPhase2.get(0), maxTsBeforeFailover)
                .isGreaterThan(maxTsBeforeFailover);

        // Strict monotonicity within phase 2.
        for (int i = 1; i < timestampsPhase2.size(); i++) {
            assertThat(timestampsPhase2.get(i))
                    .as("TSO must be strictly increasing after failover: ts[%d]=%d vs ts[%d]=%d",
                            i - 1, timestampsPhase2.get(i - 1), i, timestampsPhase2.get(i))
                    .isGreaterThan(timestampsPhase2.get(i - 1));
        }

        // ====== Phase 4: Follower rejection ======

        int followerIdx = -1;
        for (int i = 0; i < 3; i++) {
            if (i == stoppedIdx || i == newLeaderIdx) continue;
            followerIdx = i;
            break;
        }
        assertThat(followerIdx).isGreaterThanOrEqualTo(0);

        // Allocate on follower — should get error responses.
        var followerResponses = allocTimestampsRaw(clientPorts[followerIdx], 5);
        for (var resp : followerResponses) {
            assertThat(resp.getHeader().hasError())
                    .as("Follower should return error for TSO request")
                    .isTrue();
            assertThat(resp.getHeader().getError().getMessage())
                    .contains("not PD leader");
        }
    }

    private List<Long> allocTimestamps(int port, int count) throws InterruptedException {
        var ch = channel(port);
        var stub = PDGrpc.newStub(ch);

        var timestamps = new CopyOnWriteArrayList<Long>();
        var latch = new CountDownLatch(count);
        var errors = new CopyOnWriteArrayList<Throwable>();

        var reqObserver = stub.getTimestamp(new StreamObserver<>() {
            @Override public void onNext(Pdpb.TsoResponse resp) {
                if (resp.getHeader().hasError()) {
                    errors.add(new RuntimeException(resp.getHeader().getError().getMessage()));
                    latch.countDown();
                    return;
                }
                long physical = resp.getTimestamp().getPhysical();
                long logical = resp.getTimestamp().getLogical();
                long ts = (physical << 18) | logical;
                timestamps.add(ts);
                latch.countDown();
            }
            @Override public void onError(Throwable t) {
                errors.add(t);
                while (latch.getCount() > 0) latch.countDown();
            }
            @Override public void onCompleted() {
                while (latch.getCount() > 0) latch.countDown();
            }
        });

        for (int i = 0; i < count; i++) {
            reqObserver.onNext(Pdpb.TsoRequest.newBuilder().setCount(1).build());
            Thread.sleep(1);
        }
        reqObserver.onCompleted();

        assertThat(latch.await(30, TimeUnit.SECONDS)).isTrue();
        assertThat(errors)
                .as("TSO allocation should not produce errors on leader")
                .isEmpty();
        return timestamps;
    }

    private List<Pdpb.TsoResponse> allocTimestampsRaw(int port, int count) throws InterruptedException {
        var ch = channel(port);
        var stub = PDGrpc.newStub(ch);

        var responses = new CopyOnWriteArrayList<Pdpb.TsoResponse>();
        var latch = new CountDownLatch(count);

        var reqObserver = stub.getTimestamp(new StreamObserver<>() {
            @Override public void onNext(Pdpb.TsoResponse resp) {
                responses.add(resp);
                latch.countDown();
            }
            @Override public void onError(Throwable t) {
                while (latch.getCount() > 0) latch.countDown();
            }
            @Override public void onCompleted() {
                while (latch.getCount() > 0) latch.countDown();
            }
        });

        for (int i = 0; i < count; i++) {
            reqObserver.onNext(Pdpb.TsoRequest.newBuilder().setCount(1).build());
            Thread.sleep(1);
        }
        reqObserver.onCompleted();

        latch.await(10, TimeUnit.SECONDS);
        return responses;
    }

    private int findLeaderIdx() {
        for (int i = 0; i < servers.size(); i++) {
            var rn = servers.get(i).raftNode();
            if (rn != null && rn.isLeader()) return i;
        }
        throw new IllegalStateException("no leader found");
    }

    private ManagedChannel channel(int port) {
        var ch = ManagedChannelBuilder.forAddress("127.0.0.1", port)
                .usePlaintext().build();
        channels.add(ch);
        return ch;
    }
}
