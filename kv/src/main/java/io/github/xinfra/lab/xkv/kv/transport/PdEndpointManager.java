package io.github.xinfra.lab.xkv.kv.transport;

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
 * Manages a gRPC connection to the current PD leader, probing all
 * configured endpoints via {@code GetMembers} to discover it.
 *
 * <p>Equivalent to the client module's {@code PdClient} but lives in the
 * kv module to avoid a circular Maven dependency. KvServer uses this to
 * connect to PD for bootstrap, heartbeat, split, and on-demand spawn.
 *
 * <p>Thread-safe: stubs are read concurrently; leader switches are
 * serialized under an internal lock.
 */
public final class PdEndpointManager implements AutoCloseable {
    private static final Logger log = LoggerFactory.getLogger(PdEndpointManager.class);

    private final List<String> endpoints;
    private final TlsConfig tls;
    private final String authToken;
    private final ReentrantLock switchLock = new ReentrantLock();

    private volatile ManagedChannel leaderChannel;
    private volatile PDGrpc.PDBlockingStub blockingStub;
    private volatile PDGrpc.PDStub asyncStub;
    private volatile String leaderAddress;
    private volatile boolean closed;

    public PdEndpointManager(List<String> endpoints) {
        this(endpoints, null, null);
    }

    public PdEndpointManager(List<String> endpoints, TlsConfig tls, String authToken) {
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

    public String leaderAddress() { return leaderAddress; }

    public void switchLeader() {
        if (closed) return;
        switchLock.lock();
        try {
            for (var ep : endpoints) {
                try {
                    var probeCh = GrpcChannelFactory.build(ep, tls, clientInterceptors());
                    try {
                        var probeStub = PDGrpc.newBlockingStub(probeCh)
                                .withDeadlineAfter(3, TimeUnit.SECONDS);
                        var resp = probeStub.getMembers(
                                Pdpb.GetMembersRequest.newBuilder().build());
                        if (resp.hasLeader() && resp.getLeader().getClientUrlsCount() > 0) {
                            String leaderUrl = resp.getLeader().getClientUrls(0);
                            if (leaderUrl.equals(leaderAddress) && leaderChannel != null) {
                                probeCh.shutdownNow();
                                return;
                            }
                            probeCh.shutdownNow();
                            connectTo(leaderUrl);
                            log.info("PD leader discovered: {}", leaderUrl);
                            return;
                        }
                        probeCh.shutdownNow();
                        connectTo(ep);
                        log.info("PD connected to {} (no leader info)", ep);
                        return;
                    } finally {
                        if (!probeCh.isShutdown()) probeCh.shutdownNow();
                    }
                } catch (Exception e) {
                    log.debug("PD probe {} failed: {}", ep, e.getMessage());
                }
            }
            log.warn("PD leader discovery failed on all {} endpoints", endpoints.size());
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
