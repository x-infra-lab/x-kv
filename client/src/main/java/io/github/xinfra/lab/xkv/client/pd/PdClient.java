package io.github.xinfra.lab.xkv.client.pd;

import io.github.xinfra.lab.xkv.common.auth.AuthClientInterceptor;
import io.github.xinfra.lab.xkv.common.tls.GrpcChannelFactory;
import io.github.xinfra.lab.xkv.common.tls.TlsConfig;
import io.github.xinfra.lab.xkv.proto.PDGrpc;
import io.github.xinfra.lab.xkv.proto.Pdpb;
import io.grpc.ClientInterceptor;
import io.grpc.ManagedChannel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;

/**
 * PD leader discovery and failover client.
 *
 * <p>Maintains a gRPC channel to the current PD leader. On construction,
 * probes all configured endpoints via {@code GetMembers} to discover the
 * leader and connects directly to it.
 *
 * <p>When the leader changes (PD node goes down, leadership transfer),
 * callers invoke {@link #switchLeader()} to re-probe and reconnect.
 *
 * <p>Thread-safe: multiple callers can read stubs concurrently; leader
 * switches are serialized under an internal lock.
 */
public final class PdClient implements AutoCloseable {
    private static final Logger log = LoggerFactory.getLogger(PdClient.class);

    private final List<String> endpoints;
    private final TlsConfig tls;
    private final String authToken;
    private final ReentrantLock switchLock = new ReentrantLock();

    private volatile ManagedChannel leaderChannel;
    private volatile PDGrpc.PDBlockingStub blockingStub;
    private volatile PDGrpc.PDStub asyncStub;
    private volatile long leaderMemberId;
    private volatile String leaderAddress;
    private volatile boolean closed;

    public PdClient(List<String> endpoints) {
        this(endpoints, null, null);
    }

    public PdClient(List<String> endpoints, TlsConfig tls, String authToken) {
        if (endpoints == null || endpoints.isEmpty()) {
            throw new IllegalArgumentException("at least one PD endpoint required");
        }
        this.endpoints = List.copyOf(endpoints);
        this.tls = tls;
        this.authToken = authToken;
        switchLeader();
        if (leaderChannel == null) {
            connectTo(this.endpoints.get(0));
        }
    }

    public PDGrpc.PDBlockingStub blockingStub() { return blockingStub; }

    public PDGrpc.PDStub asyncStub() { return asyncStub; }

    public ManagedChannel leaderChannel() { return leaderChannel; }

    public long leaderMemberId() { return leaderMemberId; }

    public String leaderAddress() { return leaderAddress; }

    /**
     * Re-probe all endpoints to discover the current PD leader and
     * reconnect. Called when the current leader becomes unreachable
     * or returns NOT_LEADER.
     */
    public void switchLeader() {
        if (closed) return;
        switchLock.lock();
        try {
            String bestLeaderUrl = null;
            long bestLeaderId = 0;
            String fallbackEndpoint = null;

            for (var ep : endpoints) {
                ManagedChannel probeCh = null;
                try {
                    probeCh = GrpcChannelFactory.build(ep, tls, clientInterceptors());
                    var probeStub = PDGrpc.newBlockingStub(probeCh)
                            .withDeadlineAfter(3, TimeUnit.SECONDS);
                    var resp = probeStub.getMembers(
                            Pdpb.GetMembersRequest.newBuilder().build());
                    if (resp.hasLeader() && resp.getLeader().getClientUrlsCount() > 0) {
                        String leaderUrl = resp.getLeader().getClientUrls(0);
                        long leaderId = resp.getLeader().getMemberId();
                        if (leaderUrl.equals(ep)) {
                            bestLeaderUrl = leaderUrl;
                            bestLeaderId = leaderId;
                            break;
                        }
                        if (bestLeaderUrl == null) {
                            bestLeaderUrl = leaderUrl;
                            bestLeaderId = leaderId;
                        }
                    } else if (fallbackEndpoint == null) {
                        fallbackEndpoint = ep;
                    }
                } catch (Exception e) {
                    log.debug("PD probe {} failed: {}", ep, e.getMessage());
                } finally {
                    if (probeCh != null) probeCh.shutdownNow();
                }
            }

            if (bestLeaderUrl != null) {
                if (bestLeaderUrl.equals(leaderAddress) && leaderChannel != null) {
                    return;
                }
                connectTo(bestLeaderUrl);
                leaderMemberId = bestLeaderId;
                log.info("PD leader discovered: {} (memberId={})",
                        bestLeaderUrl, bestLeaderId);
            } else if (fallbackEndpoint != null) {
                connectTo(fallbackEndpoint);
                log.info("PD connected to {} (no leader info)", fallbackEndpoint);
            } else {
                log.warn("PD leader discovery failed on all {} endpoints", endpoints.size());
            }
        } finally {
            switchLock.unlock();
        }
    }

    private void connectTo(String address) {
        var old = leaderChannel;
        leaderChannel = GrpcChannelFactory.build(address, tls, clientInterceptors());
        blockingStub = PDGrpc.newBlockingStub(leaderChannel);
        asyncStub = PDGrpc.newStub(leaderChannel);
        leaderAddress = address;
        if (old != null) {
            try { old.shutdown().awaitTermination(1, TimeUnit.SECONDS); }
            catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                old.shutdownNow();
            }
        }
    }

    private List<ClientInterceptor> clientInterceptors() {
        if (authToken == null) return null;
        var list = new ArrayList<ClientInterceptor>(1);
        list.add(new AuthClientInterceptor(authToken));
        return list;
    }

    @Override
    public void close() {
        closed = true;
        if (leaderChannel != null) {
            try { leaderChannel.shutdown().awaitTermination(2, TimeUnit.SECONDS); }
            catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                leaderChannel.shutdownNow();
            }
        }
    }
}
