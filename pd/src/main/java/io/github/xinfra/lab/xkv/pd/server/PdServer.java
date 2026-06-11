package io.github.xinfra.lab.xkv.pd.server;

import io.github.xinfra.lab.raft.Peer;
import io.github.xinfra.lab.xkv.pd.config.PdConfig;
import io.github.xinfra.lab.xkv.pd.raft.PdRaftNode;
import io.github.xinfra.lab.xkv.pd.state.HlcTsoOracle;
import io.github.xinfra.lab.xkv.pd.state.InMemorySafePointService;
import io.github.xinfra.lab.xkv.pd.state.PdStateMachine;
import io.github.xinfra.lab.xkv.pd.state.RocksDbPdStateMachine;
import io.github.xinfra.lab.xkv.pd.state.StoreStatsCache;
import io.github.xinfra.lab.xkv.common.auth.AuthServerInterceptor;
import io.github.xinfra.lab.xkv.common.logging.MdcServerInterceptor;
import io.github.xinfra.lab.xkv.common.metrics.GrpcServerMetricsInterceptor;
import io.github.xinfra.lab.xkv.common.metrics.MetricsHttpServer;
import io.github.xinfra.lab.xkv.common.metrics.XKvMetrics;
import io.github.xinfra.lab.xkv.common.ratelimit.ConcurrencyLimitInterceptor;
import io.github.xinfra.lab.xkv.common.tls.GrpcChannelFactory;
import io.github.xinfra.lab.xkv.pd.transport.PdRaftServiceImpl;
import io.github.xinfra.lab.xkv.pd.transport.PdRaftTransport;
import io.grpc.Server;
import io.grpc.netty.shaded.io.grpc.netty.NettyServerBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * PD node entrypoint.
 *
 * <p>Supports two modes:
 * <ul>
 *   <li><b>Single-node</b> (no peers configured): state is in-memory only.
 *       Adequate for dev/test. This is the default when no peers are set.</li>
 *   <li><b>Multi-node HA</b> (peers configured): state mutations are
 *       replicated through an internal raft group. TSO and schedulers run
 *       only on the PD leader. Followers redirect or reject mutating RPCs.</li>
 * </ul>
 */
public final class PdServer {
    private static final Logger log = LoggerFactory.getLogger(PdServer.class);

    static final long SAFE_POINT_ADVANCE_INTERVAL_MS = 1_000L;
    static final long LEADER_BALANCE_INTERVAL_MS = 5_000L;
    static final long REGION_BALANCE_INTERVAL_MS = 10_000L;
    static final long DEADLOCK_CLEANUP_INTERVAL_MS = 10_000L;

    private final PdConfig config;
    private Server grpcServer;
    private Server raftGrpcServer;
    private RocksDbPdStateMachine state;
    private HlcTsoOracle tso;
    private InMemorySafePointService safePoint;
    private io.github.xinfra.lab.xkv.pd.state.OperatorQueue operators;
    private io.github.xinfra.lab.xkv.pd.state.DeadlockDetector deadlock;
    private io.github.xinfra.lab.xkv.pd.state.LeaderBalanceScheduler leaderBalance;
    private io.github.xinfra.lab.xkv.pd.state.RegionBalanceScheduler regionBalance;
    private io.github.xinfra.lab.xkv.pd.state.SplitCheckerScheduler splitChecker;
    private StoreStatsCache storeStatsCache;
    private PdServiceImpl service;
    private PdRaftNode raftNode;
    private PdRaftTransport raftTransport;
    private ScheduledExecutorService scheduler;
    private MetricsHttpServer metricsHttpServer;

    public PdServer(PdConfig config) {
        this.config = config;
    }

    public void start() throws IOException {
        state = new RocksDbPdStateMachine(config.dataDir().resolve("pd-state"));
        tso = new HlcTsoOracle(0, target -> CompletableFuture.completedFuture(target),
                System::currentTimeMillis,
                config.tso().savedIntervalMs());
        safePoint = new InMemorySafePointService();
        operators = new io.github.xinfra.lab.xkv.pd.state.OperatorQueue();
        deadlock = new io.github.xinfra.lab.xkv.pd.state.DeadlockDetector();
        storeStatsCache = new StoreStatsCache();

        var addr = GrpcChannelFactory.parseHostPort(config.clientAddress());
        var memberInfos = buildMemberInfos();
        service = new PdServiceImpl(state, tso, safePoint, operators, deadlock,
                config.clusterId(), config.nodeId(), config.clientAddress(), memberInfos,
                storeStatsCache);

        // Multi-PD raft: if peers are configured, start the raft group.
        if (!config.peers().isEmpty()) {
            startRaftGroup();
        }

        var metricsRegistry = XKvMetrics.init("pd");
        var grpcBuilder = GrpcChannelFactory.serverBuilder(
                        new InetSocketAddress(addr.host(), addr.port()), config.clientTls())
                .addService(service);
        grpcBuilder.intercept(new GrpcServerMetricsInterceptor(metricsRegistry, config.slowLogThresholdMs()));
        grpcBuilder.intercept(MdcServerInterceptor.forPd(config.nodeId()));
        if (config.maxConcurrentRequests() > 0) {
            grpcBuilder.intercept(new ConcurrencyLimitInterceptor(config.maxConcurrentRequests()));
        }
        if (config.authToken() != null) {
            grpcBuilder.intercept(new AuthServerInterceptor(config.authToken()));
        }
        grpcServer = grpcBuilder.build().start();
        log.info("PD node {} listening on {}", config.nodeId(), config.clientAddress());

        if (config.metricsPort() > 0) {
            metricsHttpServer = new MetricsHttpServer(config.metricsPort(), metricsRegistry,
                    () -> grpcServer != null && !grpcServer.isShutdown());
        }

        scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            var t = new Thread(r, "pd-safepoint-advance");
            t.setDaemon(true);
            return t;
        });
        scheduler.scheduleAtFixedRate(this::advanceSafePointSafely,
                SAFE_POINT_ADVANCE_INTERVAL_MS, SAFE_POINT_ADVANCE_INTERVAL_MS, TimeUnit.MILLISECONDS);
        scheduler.scheduleAtFixedRate(this::cleanupDeadlockSafely,
                DEADLOCK_CLEANUP_INTERVAL_MS, DEADLOCK_CLEANUP_INTERVAL_MS, TimeUnit.MILLISECONDS);

        leaderBalance = new io.github.xinfra.lab.xkv.pd.state.LeaderBalanceScheduler(
                state, operators, storeStatsCache, LEADER_BALANCE_INTERVAL_MS);
        leaderBalance.start();

        regionBalance = new io.github.xinfra.lab.xkv.pd.state.RegionBalanceScheduler(
                state, operators, storeStatsCache, REGION_BALANCE_INTERVAL_MS);
        regionBalance.start();

        splitChecker = new io.github.xinfra.lab.xkv.pd.state.SplitCheckerScheduler(
                state, operators, config.scheduler().regionSplitBytes(),
                config.scheduler().heartbeatIntervalMs());
        splitChecker.start();
    }

    /**
     * Boot the PD-internal raft group for HA. Each PD node gets:
     * <ol>
     *   <li>A {@link PdRaftTransport} for inter-PD raft messaging</li>
     *   <li>A gRPC server on the raft address to receive inbound messages</li>
     *   <li>A {@link PdRaftNode} wrapping x-raft-lib's Node</li>
     * </ol>
     */
    private void startRaftGroup() throws IOException {
        raftTransport = new PdRaftTransport(config.nodeId(), config.raftTls());
        for (var peer : config.peers()) {
            if (peer.id() != config.nodeId()) {
                raftTransport.addPeer(peer.id(), peer.raftAddress());
            }
        }

        // Start the raft gRPC server on the raft address.
        var raftAddr = GrpcChannelFactory.parseHostPort(config.raftAddress());
        raftGrpcServer = GrpcChannelFactory.serverBuilder(
                        new InetSocketAddress(raftAddr.host(), raftAddr.port()), config.raftTls())
                .addService(new PdRaftServiceImpl(raftTransport))
                .build()
                .start();

        // Build the peer list for raft initialization.
        var raftPeers = new ArrayList<Peer>();
        for (var p : config.peers()) raftPeers.add(new Peer(p.id()));
        // Include self if not already in the list.
        if (config.peers().stream().noneMatch(p -> p.id() == config.nodeId())) {
            raftPeers.add(new Peer(config.nodeId()));
        }

        raftNode = new PdRaftNode(config.nodeId(), raftPeers, state, raftTransport, 100,
                config.dataDir());
        service.setRaftNode(raftNode);

        log.info("PD raft group started: node={} peers={}", config.nodeId(),
                config.peers().stream().map(p -> p.id() + "@" + p.raftAddress()).toList());
    }

    private final io.micrometer.core.instrument.Counter bgErrorCounter =
            XKvMetrics.errorCounter("pd_server", "background_task");

    private void advanceSafePointSafely() {
        try { if (safePoint != null) safePoint.advance(); }
        catch (Throwable t) {
            bgErrorCounter.increment();
            log.warn("safe-point advance failed: {}", t.getMessage());
        }
    }

    private void cleanupDeadlockSafely() {
        try { if (deadlock != null) deadlock.cleanupExpired(); }
        catch (Throwable t) {
            bgErrorCounter.increment();
            log.warn("deadlock cleanup failed: {}", t.getMessage());
        }
    }

    public RocksDbPdStateMachine state() { return state; }
    public HlcTsoOracle tso() { return tso; }
    public InMemorySafePointService safePointService() { return safePoint; }
    public io.github.xinfra.lab.xkv.pd.state.OperatorQueue operators() { return operators; }
    public io.github.xinfra.lab.xkv.pd.state.DeadlockDetector deadlockDetector() { return deadlock; }
    public PdRaftNode raftNode() { return raftNode; }

    public void awaitTermination() throws InterruptedException {
        if (grpcServer != null) grpcServer.awaitTermination();
    }

    public void stop() {
        if (metricsHttpServer != null) {
            try { metricsHttpServer.close(); } catch (Exception ignored) {}
        }
        if (splitChecker != null) splitChecker.close();
        if (regionBalance != null) regionBalance.close();
        if (leaderBalance != null) leaderBalance.close();
        if (scheduler != null) scheduler.shutdownNow();
        if (grpcServer != null) {
            grpcServer.shutdown();
            try {
                if (!grpcServer.awaitTermination(5, TimeUnit.SECONDS)) {
                    grpcServer.shutdownNow();
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
        if (raftNode != null) {
            try { raftNode.close(); } catch (Throwable ignored) {}
        }
        if (raftGrpcServer != null) {
            raftGrpcServer.shutdown();
            try {
                if (!raftGrpcServer.awaitTermination(5, TimeUnit.SECONDS)) {
                    raftGrpcServer.shutdownNow();
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
        if (raftTransport != null) {
            try { raftTransport.close(); } catch (Throwable ignored) {}
        }
        if (state != null) {
            try { state.close(); } catch (Throwable ignored) {}
        }
    }

    public static void main(String[] args) throws Exception {
        var cfg = io.github.xinfra.lab.xkv.pd.config.PdConfigLoader.load(args);
        var srv = new PdServer(cfg);
        srv.start();
        Runtime.getRuntime().addShutdownHook(new Thread(srv::stop, "pd-shutdown"));
        srv.awaitTermination();
    }

    private List<PdServiceImpl.MemberInfo> buildMemberInfos() {
        var infos = new ArrayList<PdServiceImpl.MemberInfo>();
        boolean selfIncluded = false;
        for (var peer : config.peers()) {
            String clientAddr = peer.clientAddress().isEmpty()
                    ? "" : peer.clientAddress();
            if (peer.id() == config.nodeId()) {
                clientAddr = config.clientAddress();
                selfIncluded = true;
            }
            infos.add(new PdServiceImpl.MemberInfo(peer.id(), "pd-" + peer.id(), clientAddr));
        }
        if (!selfIncluded) {
            infos.add(new PdServiceImpl.MemberInfo(config.nodeId(),
                    "pd-" + config.nodeId(), config.clientAddress()));
        }
        return infos;
    }

}
