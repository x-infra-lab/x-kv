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

    /**
     * Fetch a single fresh timestamp from the current PD leader, encoded as
     * {@code (physical << 18) | logical} — the same layout the client uses for
     * {@code start_ts}/{@code commit_ts}. Returns {@code -1} on failure.
     *
     * <p>Used to re-establish a region leader's {@code max_ts} on failover so
     * Snapshot Isolation holds across leadership changes. {@code GetTimestamp}
     * is a bidi stream; we open a short-lived one, ask for a single ts, and
     * close it. Bounded by a short deadline so a slow/unavailable PD never
     * stalls the caller indefinitely.
     */
    public long fetchTimestamp() {
        var stub = asyncStub;
        if (closed || stub == null) return -1L;
        var result = new java.util.concurrent.CompletableFuture<Long>();
        io.grpc.stub.StreamObserver<Pdpb.TsoRequest> outbound;
        try {
            outbound = stub.getTimestamp(new io.grpc.stub.StreamObserver<>() {
                @Override public void onNext(Pdpb.TsoResponse resp) {
                    if (resp.getHeader().hasError()) {
                        result.completeExceptionally(new RuntimeException(
                                resp.getHeader().getError().getMessage()));
                        return;
                    }
                    long physical = resp.getTimestamp().getPhysical();
                    long logical = resp.getTimestamp().getLogical();
                    // Same encoding as the client TSO batcher (LOGICAL_BITS=18).
                    result.complete((physical << 18) | logical);
                }
                @Override public void onError(Throwable t) { result.completeExceptionally(t); }
                @Override public void onCompleted() {
                    if (!result.isDone()) {
                        result.completeExceptionally(new RuntimeException("tso stream closed"));
                    }
                }
            });
        } catch (Throwable t) {
            log.warn("fetchTimestamp: open stream failed: {}", t.getMessage());
            return -1L;
        }
        try {
            outbound.onNext(Pdpb.TsoRequest.newBuilder().setCount(1).build());
            long ts = result.get(3, TimeUnit.SECONDS);
            try { outbound.onCompleted(); } catch (Throwable ignore) { /* best-effort */ }
            return ts;
        } catch (Exception e) {
            log.warn("fetchTimestamp failed: {}", e.getMessage());
            try { outbound.onError(e); } catch (Throwable ignore) { /* best-effort */ }
            return -1L;
        }
    }

    public ManagedChannel leaderChannel() { return leaderChannel; }

    public String leaderAddress() { return leaderAddress; }

    public void switchLeader() {
        if (closed) return;
        switchLock.lock();
        try {
            String firstReachable = null;
            for (var ep : endpoints) {
                try {
                    var probeCh = GrpcChannelFactory.build(ep, tls, clientInterceptors());
                    try {
                        var probeStub = PDGrpc.newBlockingStub(probeCh)
                                .withDeadlineAfter(3, TimeUnit.SECONDS);
                        var resp = probeStub.getMembers(
                                Pdpb.GetMembersRequest.newBuilder().build());
                        if (firstReachable == null) firstReachable = ep;
                        if (resp.hasLeader() && resp.getLeader().getClientUrlsCount() > 0) {
                            String leaderUrl = resp.getLeader().getClientUrls(0);
                            if (leaderUrl.equals(leaderAddress) && leaderChannel != null) {
                                return;
                            }
                            connectTo(leaderUrl);
                            log.info("PD leader discovered: {}", leaderUrl);
                            return;
                        }
                        // Reachable but leader unknown (mid-election, or a
                        // follower that declines to answer). Keep probing the
                        // remaining endpoints instead of pinning to a
                        // non-leader — pinning there would silently drop
                        // operator dispatch on region heartbeats.
                    } finally {
                        if (!probeCh.isShutdown()) probeCh.shutdownNow();
                    }
                } catch (Exception e) {
                    log.debug("PD probe {} failed: {}", ep, e.getMessage());
                }
            }
            // No endpoint reported a leader. Fall back to the first reachable
            // one (if any) so we still hold a channel; the next switchLeader()
            // retry re-discovers once an election settles.
            if (firstReachable != null && !firstReachable.equals(leaderAddress)) {
                connectTo(firstReachable);
                log.info("PD connected to {} (no leader info)", firstReachable);
            } else if (firstReachable == null) {
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
