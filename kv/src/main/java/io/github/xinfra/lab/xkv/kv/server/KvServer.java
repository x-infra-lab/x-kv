package io.github.xinfra.lab.xkv.kv.server;

import io.github.xinfra.lab.raft.Peer;
import io.github.xinfra.lab.xkv.kv.cdc.CdcEventBus;
import io.github.xinfra.lab.xkv.kv.coprocessor.TableScanCoprocessor;
import io.github.xinfra.lab.xkv.kv.config.KvConfig;
import io.github.xinfra.lab.xkv.kv.engine.PerRegionRaftEngine;
import io.github.xinfra.lab.xkv.kv.engine.RaftCfKeys;
import io.github.xinfra.lab.xkv.kv.engine.RocksStorageEngine;
import io.github.xinfra.lab.xkv.kv.engine.SnapshotEngineImpl;
import io.github.xinfra.lab.xkv.kv.engine.StorageEngine;
import io.github.xinfra.lab.xkv.kv.mvcc.ConcurrencyManager;
import io.github.xinfra.lab.xkv.kv.mvcc.MaxTsTracker;
import io.github.xinfra.lab.xkv.kv.raft.BatchRegionPeer;
import io.github.xinfra.lab.xkv.kv.raft.CompositeApplyHandler;
import io.github.xinfra.lab.xkv.kv.raft.AdminApplyHandler;
import io.github.xinfra.lab.xkv.kv.raft.RaftPoller;
import io.github.xinfra.lab.xkv.kv.raft.RegionPeer;
import io.github.xinfra.lab.xkv.kv.raft.RegionPeerImpl;
import io.github.xinfra.lab.xkv.kv.raft.TickDriver;
import io.github.xinfra.lab.xkv.kv.store.GcWorker;
import io.github.xinfra.lab.xkv.kv.store.LogCompactionWorker;
import io.github.xinfra.lab.xkv.kv.store.RegionHeartbeater;
import io.github.xinfra.lab.xkv.kv.store.SplitDriver;
import io.github.xinfra.lab.xkv.kv.store.StoreHeartbeater;
import io.github.xinfra.lab.xkv.kv.store.StoreImpl;
import io.github.xinfra.lab.xkv.kv.transport.DeadlockClient;
import io.github.xinfra.lab.xkv.kv.transport.GrpcRaftTransport;
import io.github.xinfra.lab.xkv.kv.transport.PdEndpointManager;
import io.github.xinfra.lab.xkv.kv.transport.RaftMessageDispatcher;
import io.github.xinfra.lab.xkv.common.auth.AuthServerInterceptor;
import io.github.xinfra.lab.xkv.common.logging.MdcServerInterceptor;
import io.github.xinfra.lab.xkv.common.metrics.GrpcServerMetricsInterceptor;
import io.github.xinfra.lab.xkv.common.metrics.MetricsHttpServer;
import io.github.xinfra.lab.xkv.common.metrics.XKvMetrics;
import io.github.xinfra.lab.xkv.common.ratelimit.ConcurrencyLimitInterceptor;
import io.github.xinfra.lab.xkv.common.ratelimit.DrainingInterceptor;
import io.github.xinfra.lab.xkv.common.util.CloseUtils;
import io.github.xinfra.lab.xkv.common.tls.GrpcChannelFactory;
import io.github.xinfra.lab.xkv.proto.Metapb;
import io.github.xinfra.lab.xkv.proto.PDGrpc;
import io.github.xinfra.lab.xkv.proto.Pdpb;
import io.grpc.Server;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

/**
 * KV store node entrypoint.
 *
 * <p>Boots a fully-functional KV node following TiKV's startup protocol:
 * <ol>
 *   <li>Opens the local storage engine.</li>
 *   <li>Connects to PD: bootstraps the cluster (first store) or registers
 *       via {@code PutStore} (subsequent stores).</li>
 *   <li>Recovers all regions from the local RAFT CF (primary recovery path).</li>
 *   <li>If this is the bootstrap store and no regions were recovered, creates
 *       the initial Region 1 peer.</li>
 *   <li>Starts the raft transport, client-facing gRPC, and heartbeaters.</li>
 * </ol>
 *
 * <p>A store can start with zero regions — PD's schedulers will assign
 * regions via ConfChange, and {@code spawnOnDemand} creates peers when
 * raft messages arrive.
 *
 * <p>Shutdown order: heartbeaters → gRPC servers → peers → PD channels → engine.
 */
public final class KvServer {
    private static final Logger log = LoggerFactory.getLogger(KvServer.class);

    private static final long PROPOSE_TIMEOUT_MS = 10_000;
    private static final long HEARTBEAT_INTERVAL_MS = 500;
    private static final long STORE_HEARTBEAT_INTERVAL_MS = 10_000;
    private static final int BOOTSTRAP_CHECK_MAX_RETRIES = 3;

    private final KvConfig config;

    private RocksStorageEngine engine;
    private StoreImpl store;
    private ConcurrencyManager storeCm;
    private RaftMessageDispatcher dispatcher;
    private Server clientServer;
    private Server raftServer;
    private final List<RegionPeer> peers = new java.util.concurrent.CopyOnWriteArrayList<>();
    private RaftPoller raftPoller;
    private TickDriver tickDriver;
    private final List<RegionHeartbeater> heartbeaters = new java.util.concurrent.CopyOnWriteArrayList<>();
    private PdEndpointManager pdManager;
    private TransactionService txnService;
    private RawKvService rawKvService;
    private StoreHeartbeater storeHeartbeater;
    private LogCompactionWorker logCompactionWorker;
    private GcWorker gcWorker;
    private MetricsHttpServer metricsHttpServer;
    private DrainingInterceptor drainingInterceptor;
    private io.github.xinfra.lab.xkv.kv.ratelimit.ResourceGroupThrottler resourceGroupThrottler;
    private CdcEventBus cdcEventBus;
    private ChangeDataServiceImpl cdcService;
    private io.github.xinfra.lab.xkv.kv.raft.ApplyWorker applyWorker;
    private final io.github.xinfra.lab.xkv.kv.mvcc.InMemoryLockTable inMemoryLockTable =
            new io.github.xinfra.lab.xkv.kv.mvcc.InMemoryLockTable();
    private final io.github.xinfra.lab.xkv.kv.cdc.RegionResolvedTsTracker resolvedTsTracker =
            new io.github.xinfra.lab.xkv.kv.cdc.RegionResolvedTsTracker();
    private io.github.xinfra.lab.xkv.kv.config.ConfigManager configManager;
    private CoprocessorService copService;
    private TikvServiceImpl tikvService;

    public KvServer(KvConfig config) {
        this.config = config;
    }

    public void start() throws Exception {
        log.info("KV store {} starting: client={} raft={} pd={} data={}",
                config.storeId(), config.clientAddress(), config.raftAddress(),
                config.pdEndpoints(), config.dataDir());

        // 1) Open storage engine.
        Files.createDirectories(config.dataDir());
        engine = RocksStorageEngine.open(config.dataDir(), config.engine());

        // 2) Connect to PD (probes all endpoints, discovers leader).
        pdManager = new PdEndpointManager(config.pdEndpoints(), config.clientTls(), config.authToken());
        var pdStub = pdManager.blockingStub();

        var storeMetaBuilder = Metapb.Store.newBuilder()
                .setId(config.storeId())
                .setAddress(config.clientAddress())
                .setPeerAddress(config.raftAddress())
                .setState(Metapb.StoreState.Up);
        for (var entry : config.labels().entrySet()) {
            storeMetaBuilder.addLabels(Metapb.StoreLabel.newBuilder()
                    .setKey(entry.getKey())
                    .setValue(entry.getValue()));
        }
        var storeMeta = storeMetaBuilder.build();

        // 3) Bootstrap or join the cluster.
        Optional<Metapb.Region> bootstrapRegion = bootstrapOrJoin(pdStub, storeMeta);

        // 4) Build store container + CDC event bus + config manager.
        store = new StoreImpl(config.storeId(), storeMeta);
        dispatcher = new RaftMessageDispatcher();
        cdcEventBus = new CdcEventBus();
        configManager = new io.github.xinfra.lab.xkv.kv.config.ConfigManager(config);

        // 5) Boot the raft gRPC server so peers can receive messages immediately.
        var snapshotEngine = new SnapshotEngineImpl(engine, config.dataDir().resolve("snap"));
        var r = GrpcChannelFactory.parseHostPort(config.raftAddress());
        var raftBuilder = GrpcChannelFactory.serverBuilder(
                        new InetSocketAddress(r.host(), r.port()), config.raftTls())
                .addService(new KvRaftServiceImpl(dispatcher, snapshotEngine));
        if (config.maxConcurrentRequests() > 0) {
            raftBuilder.intercept(new ConcurrencyLimitInterceptor(config.maxConcurrentRequests()));
        }
        if (config.authToken() != null) {
            raftBuilder.intercept(new AuthServerInterceptor(config.authToken()));
        }
        raftServer = raftBuilder.build().start();

        // On-demand spawn handler for regions this store doesn't yet host.
        dispatcher.setMissingHandler((regionId, firstMsg) -> {
            var t = new Thread(() -> spawnOnDemand(regionId),
                    "spawn-on-demand-" + regionId);
            t.setDaemon(true);
            t.start();
        });

        // 5b) Initialize BatchSystem: shared poller + tick driver + apply pool.
        raftPoller = new RaftPoller(config.raft().pollerThreads());
        tickDriver = new TickDriver(config.raft().heartbeatTickMs());
        applyWorker = new io.github.xinfra.lab.xkv.kv.raft.ApplyWorker(
                config.worker().applyPoolThreads());

        // 6) Recover ALL persisted regions from RAFT CF (primary recovery path).
        recoverPersistedRegions(pdStub, snapshotEngine);

        // 6b) If this is the bootstrap store and no regions were recovered
        //     (first-ever boot), create the initial Region 1 peer.
        if (bootstrapRegion.isPresent() && peers.isEmpty()) {
            var region = bootstrapRegion.get();
            Map<Long, String> peerAddrs = resolvePeerAddresses(pdStub, region);
            var raftEngine = new PerRegionRaftEngine(engine, region.getId());
            var cm = new ConcurrencyManager(new MaxTsTracker(raftEngine.persistedMaxTs()));
            var regionPeer = createRegionPeer(region, peerAddrs, snapshotEngine, raftEngine, cm);
            store.registerPeer(regionPeer);
            peers.add(regionPeer);
        }

        // 7) Build store-level ConcurrencyManager from max across all regions.
        long globalMaxTs = 0;
        for (var peer : peers) {
            var re = new PerRegionRaftEngine(engine, peer.regionId());
            globalMaxTs = Math.max(globalMaxTs, re.persistedMaxTs());
        }
        storeCm = new ConcurrencyManager(new MaxTsTracker(globalMaxTs));

        // 8) Build RPC services backed by the store's peer locator.
        RawKvService.PeerLocator locator = key ->
                store.peerForKey(key).orElse(null);
        rawKvService = new RawKvService(engine, locator, PROPOSE_TIMEOUT_MS);
        txnService = new TransactionService(engine, locator, PROPOSE_TIMEOUT_MS, storeCm, inMemoryLockTable);

        // Deadlock detector via PD.
        var deadlockClient = new DeadlockClient(pdManager.blockingStub(), /* clusterId= */ 1L);
        txnService.setDeadlockClient(deadlockClient);

        // Split driver.
        var splitDriver = new SplitDriver(pdManager.blockingStub(), PROPOSE_TIMEOUT_MS);

        // 9) Build CoprocessorService and start client-facing gRPC server.
        copService = new CoprocessorService();
        copService.register(new TableScanCoprocessor(engine));
        copService.register(new io.github.xinfra.lab.xkv.kv.coprocessor.SQLScanCoprocessor(engine));
        copService.register(new io.github.xinfra.lab.xkv.kv.coprocessor.AnalyzeCoprocessor(engine));
        copService.register(new io.github.xinfra.lab.xkv.kv.coprocessor.IndexScanCoprocessor(engine));
        copService.register(new io.github.xinfra.lab.xkv.kv.coprocessor.SplitKeysCoprocessor(engine));
        copService.register(new io.github.xinfra.lab.xkv.kv.coprocessor.ChecksumCoprocessor(engine));

        // CDC service — resolved TS uses the store-level CM.
        cdcService = new ChangeDataServiceImpl(cdcEventBus, engine, () -> storeCm.maxTs().current(), resolvedTsTracker);

        var metricsRegistry = XKvMetrics.init("kv");
        var c = GrpcChannelFactory.parseHostPort(config.clientAddress());
        var clientServerBuilder = GrpcChannelFactory.serverBuilder(
                        new InetSocketAddress(c.host(), c.port()), config.clientTls())
                .addService(tikvService = buildTikvService(rawKvService, txnService, copService, splitDriver, locator))
                .addService(cdcService);
        if (config.enableDebugService()) {
            clientServerBuilder.addService(
                    new DebugServiceImpl(metricsRegistry, store, engine, storeMeta, config.dataDir(), configManager));
        }
        // Interceptor order: last .intercept() is outermost (executed first).
        clientServerBuilder.intercept(new GrpcServerMetricsInterceptor(metricsRegistry, config.slowLogThresholdMs()));
        clientServerBuilder.intercept(MdcServerInterceptor.forStore(config.storeId()));
        resourceGroupThrottler = new io.github.xinfra.lab.xkv.kv.ratelimit.ResourceGroupThrottler();
        clientServerBuilder.intercept(new io.github.xinfra.lab.xkv.kv.ratelimit.ResourceGroupInterceptor(resourceGroupThrottler));
        if (config.maxConcurrentRequests() > 0) {
            clientServerBuilder.intercept(new ConcurrencyLimitInterceptor(config.maxConcurrentRequests()));
        }
        if (config.authToken() != null) {
            clientServerBuilder.intercept(new AuthServerInterceptor(config.authToken()));
        }
        drainingInterceptor = new DrainingInterceptor();
        clientServerBuilder.intercept(drainingInterceptor);
        clientServer = clientServerBuilder.build().start();

        // 9b) Start metrics HTTP server if configured.
        if (config.metricsPort() > 0) {
            metricsHttpServer = new MetricsHttpServer(config.metricsPort(), metricsRegistry,
                    () -> store != null && !peers.isEmpty());
        }

        // 10) Start region heartbeater(s) for all loaded peers.
        for (var p : peers) {
            startHeartbeater(p);
        }

        // 11) Start store-level heartbeater.
        storeHeartbeater = new StoreHeartbeater(config.storeId(), pdManager,
                store, config.dataDir(), STORE_HEARTBEAT_INTERVAL_MS);

        // 12) Start background workers.
        var wc = config.worker();
        logCompactionWorker = new LogCompactionWorker(store,
                wc.logCompactionIntervalMs(),
                wc.logCompactionGapThreshold(),
                wc.logCompactionSafetyMargin(),
                PROPOSE_TIMEOUT_MS);
        logCompactionWorker.start();

        gcWorker = new GcWorker(store, pdManager.blockingStub(),
                wc.gcIntervalMs(), PROPOSE_TIMEOUT_MS);
        gcWorker.start();

        if (peers.isEmpty()) {
            log.info("KV store {} started with 0 regions, waiting for PD scheduling: client={} raft={}",
                    config.storeId(), config.clientAddress(), config.raftAddress());
        } else {
            log.info("KV store {} started: client={} raft={} regions={}",
                    config.storeId(), config.clientAddress(), config.raftAddress(), peers.size());
        }
    }

    // =====================================================================
    // Bootstrap / Join
    // =====================================================================

    /**
     * Bootstrap the cluster (if this is the first store) or register this
     * store as a member of an existing cluster.
     *
     * <p>Follows TiKV's bootstrap protocol:
     * <ol>
     *   <li>Call {@code IsBootstrapped} with retry on transient errors.</li>
     *   <li>If already bootstrapped: register via {@code PutStore}, return empty.</li>
     *   <li>If not bootstrapped: prepare bootstrap region locally (persist to
     *       RAFT CF), then call {@code PD.Bootstrap}. On {@code ALREADY_BOOTSTRAPPED}
     *       race, clean up local state and fall back to join.</li>
     * </ol>
     *
     * <p>Non-bootstrap stores start with zero regions — region assignment
     * happens via PD scheduling and {@code spawnOnDemand}.
     */
    private Optional<Metapb.Region> bootstrapOrJoin(PDGrpc.PDBlockingStub pdStub,
                                                     Metapb.Store storeMeta) {
        boolean bootstrapped = checkClusterBootstrapped(pdStub);

        if (bootstrapped) {
            log.info("Joining existing cluster, registering store {}", config.storeId());
            pdStub.putStore(Pdpb.PutStoreRequest.newBuilder()
                    .setStore(storeMeta).build());
            return Optional.empty();
        }

        // First store — prepare locally, then bootstrap via PD.
        log.info("Bootstrapping cluster with store {}", config.storeId());
        var region = prepareBootstrapRegion();

        try {
            pdStub.bootstrap(Pdpb.BootstrapRequest.newBuilder()
                    .setStore(storeMeta)
                    .setRegion(region)
                    .build());
        } catch (StatusRuntimeException e) {
            if (isAlreadyBootstrappedError(e)) {
                log.info("Another store bootstrapped first, switching to join path");
                clearPreparedBootstrap();
                pdStub.putStore(Pdpb.PutStoreRequest.newBuilder()
                        .setStore(storeMeta).build());
                return Optional.empty();
            }
            clearPreparedBootstrap();
            throw e;
        }

        return Optional.of(region);
    }

    /**
     * Persist bootstrap Region 1 to local RAFT CF before calling PD.Bootstrap().
     * Aligns with TiKV's {@code prepare_bootstrap_cluster}: ensures the region
     * is recoverable even if the store crashes between PD.Bootstrap() and peer creation.
     */
    private Metapb.Region prepareBootstrapRegion() {
        var region = Metapb.Region.newBuilder()
                .setId(1)
                .setRegionEpoch(Metapb.RegionEpoch.newBuilder()
                        .setConfVer(1).setVersion(1))
                .addPeers(Metapb.Peer.newBuilder()
                        .setId(config.storeId())
                        .setStoreId(config.storeId())
                        .setRole(Metapb.PeerRole.Voter))
                .build();
        try (var batch = engine.newWriteBatch()) {
            batch.put(StorageEngine.Cf.RAFT, RaftCfKeys.regionKey(1), region.toByteArray());
            engine.write(batch, true);
        }
        return region;
    }

    /**
     * Remove locally prepared bootstrap state. Called when another store won
     * the bootstrap race (ALREADY_BOOTSTRAPPED), aligning with TiKV's
     * {@code clear_prepare_bootstrap_state}.
     */
    private void clearPreparedBootstrap() {
        try (var batch = engine.newWriteBatch()) {
            batch.delete(StorageEngine.Cf.RAFT, RaftCfKeys.regionKey(1));
            engine.write(batch, true);
        }
    }

    /**
     * Check cluster bootstrap status with retry on transient errors.
     * Throws on persistent failure — never falls through to bootstrap on error.
     */
    private boolean checkClusterBootstrapped(PDGrpc.PDBlockingStub pdStub) {
        for (int attempt = 0; attempt < BOOTSTRAP_CHECK_MAX_RETRIES; attempt++) {
            try {
                var resp = pdStub.isBootstrapped(
                        Pdpb.IsBootstrappedRequest.newBuilder().build());
                return resp.getBootstrapped();
            } catch (StatusRuntimeException e) {
                if (isTransientError(e) && attempt < BOOTSTRAP_CHECK_MAX_RETRIES - 1) {
                    long backoffMs = 100L * (1L << attempt);
                    log.warn("PD unreachable (attempt {}/{}), retrying in {}ms: {}",
                            attempt + 1, BOOTSTRAP_CHECK_MAX_RETRIES, backoffMs, e.getMessage());
                    try { Thread.sleep(backoffMs); } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                        throw new IllegalStateException("Interrupted while checking PD", ie);
                    }
                    continue;
                }
                throw new IllegalStateException(
                        "Cannot determine cluster state after " + BOOTSTRAP_CHECK_MAX_RETRIES + " attempts", e);
            }
        }
        throw new IllegalStateException("Cannot reach PD");
    }

    private static boolean isTransientError(StatusRuntimeException e) {
        var code = e.getStatus().getCode();
        return code == Status.Code.UNAVAILABLE
                || code == Status.Code.DEADLINE_EXCEEDED
                || code == Status.Code.INTERNAL;
    }

    private static boolean isAlreadyBootstrappedError(StatusRuntimeException e) {
        // PD returns ALREADY_BOOTSTRAPPED in the response header, but may also
        // signal it via gRPC error status in some implementations.
        return e.getMessage() != null
                && e.getMessage().contains("already bootstrapped");
    }

    // =====================================================================
    // Peer address resolution
    // =====================================================================

    private Map<Long, String> resolvePeerAddresses(PDGrpc.PDBlockingStub pdStub,
                                                    Metapb.Region region) {
        var addrs = new HashMap<Long, String>();
        for (var peer : region.getPeersList()) {
            if (peer.getStoreId() == config.storeId()) {
                addrs.put(peer.getId(), config.raftAddress());
                continue;
            }
            try {
                var storeResp = pdStub.getStore(Pdpb.GetStoreRequest.newBuilder()
                        .setStoreId(peer.getStoreId()).build());
                if (storeResp.hasStore()) {
                    addrs.put(peer.getId(), storeResp.getStore().getPeerAddress());
                }
            } catch (Exception e) {
                log.warn("Failed to resolve peer address for store={}: {}",
                        peer.getStoreId(), e.getMessage());
            }
        }
        return addrs;
    }

    // =====================================================================
    // Region peer creation
    // =====================================================================

    private BatchRegionPeer createRegionPeer(Metapb.Region region,
                                              Map<Long, String> peerAddrs,
                                              SnapshotEngineImpl snapshotEngine,
                                              PerRegionRaftEngine raftEngine,
                                              ConcurrencyManager cm) {
        long peerId = findSelfPeerId(region);

        var transport = new GrpcRaftTransport(region.getId(), peerId, config.storeId(), config.raftTls());
        for (var peer : region.getPeersList()) {
            if (peer.getId() == peerId) continue;
            var addr = peerAddrs.get(peer.getId());
            if (addr != null) transport.addPeer(peer.getId(), addr, peer.getStoreId());
        }

        var raftPeers = new ArrayList<Peer>();
        for (var pe : region.getPeersList()) raftPeers.add(new Peer(pe.getId()));

        var self = region.getPeersList().stream()
                .filter(p -> p.getId() == peerId)
                .findFirst().orElseThrow();

        var peerHolder = new AtomicReference<BatchRegionPeer>();
        var splitObserver = (AdminApplyHandler.SplitObserver)
                (updatedParent, children) -> {
                    var p = peerHolder.get();
                    if (p != null) p.updateRegion(updatedParent);
                    for (var child : children) {
                        var childSelfOpt = child.getPeersList().stream()
                                .filter(pe -> pe.getStoreId() == config.storeId())
                                .findFirst();
                        if (childSelfOpt.isEmpty()) continue;
                        try {
                            spawnChildPeer(child, childSelfOpt.get(), peerAddrs, snapshotEngine);
                        } catch (Exception e) {
                            log.error("Failed to spawn child peer for region={}",
                                    child.getId(), e);
                        }
                    }
                };
        var mergeObserver = (AdminApplyHandler.MergeObserver)
                (mergedTarget, sourceRegion) -> {
                    var p = peerHolder.get();
                    if (p != null) p.updateRegion(mergedTarget);
                    store.destroyPeer(sourceRegion.getId());
                    peers.removeIf(rp -> rp.regionId() == sourceRegion.getId());
                };

        var settings = new RegionPeerImpl.Settings(
                config.raft().electionTickMs() > 0
                        ? (int) (config.raft().electionTickMs() / config.raft().heartbeatTickMs()) : 10,
                1,
                config.raft().heartbeatTickMs(),
                config.raft().leaseBasedRead());

        var peer = new BatchRegionPeer(
                engine, raftEngine, region, self, raftPeers,
                transport,
                CompositeApplyHandler.defaultFor(engine, cm, region.getId(), cdcEventBus,
                                inMemoryLockTable, resolvedTsTracker)
                        .withAdmin(raftEngine, engine, splitObserver, mergeObserver),
                settings, cm, snapshotEngine, raftPoller, tickDriver, applyWorker);
        peerHolder.set(peer);
        // Wire the raft transport for dynamically-added peers: when a
        // conf-change (AddPeer) applies, resolve the new peer's raft address
        // from PD so the leader can actually reach the new replica. Without
        // this the transport drops messages to unknown peers and the region
        // never replicates to newly-scheduled stores.
        peer.setChangePeerObserver((type, changedPeer, updatedRegion) ->
                onChangePeer(transport, type, changedPeer));
        dispatcher.register(region.getId(), transport);
        return peer;
    }

    private long findSelfPeerId(Metapb.Region region) {
        for (var peer : region.getPeersList()) {
            if (peer.getStoreId() == config.storeId()) return peer.getId();
        }
        throw new IllegalStateException(
                "No peer found for store " + config.storeId()
                        + " in region " + region.getId());
    }

    /**
     * Conf-change transport wiring. Runs after every applied conf-change on
     * every peer (leader and followers). On AddNode/AddLearnerNode we resolve
     * the added peer's raft address from PD and register it in this region's
     * transport so raft messages can flow to the new replica; on RemoveNode
     * we drop the peer's outbound link. Self is skipped (no loopback).
     */
    private void onChangePeer(GrpcRaftTransport transport,
                              io.github.xinfra.lab.raft.proto.Eraftpb.ConfChangeType type,
                              Metapb.Peer changedPeer) {
        try {
            switch (type) {
                case ConfChangeAddNode, ConfChangeAddLearnerNode -> {
                    if (changedPeer.getStoreId() == config.storeId()) return;
                    String addr = resolveStoreRaftAddr(changedPeer.getStoreId());
                    if (addr != null && !addr.isEmpty()) {
                        transport.addPeer(changedPeer.getId(), addr, changedPeer.getStoreId());
                        log.info("conf-change: wired peer {} on store {} at {}",
                                changedPeer.getId(), changedPeer.getStoreId(), addr);
                    } else {
                        log.warn("conf-change: could not resolve raft addr for store {}",
                                changedPeer.getStoreId());
                    }
                }
                case ConfChangeRemoveNode -> transport.removePeer(changedPeer.getId());
                default -> { }
            }
        } catch (Throwable t) {
            log.warn("conf-change transport wiring failed: {}", t.getMessage());
        }
    }

    private String resolveStoreRaftAddr(long storeId) {
        try {
            var resp = pdManager.blockingStub().getStore(
                    Pdpb.GetStoreRequest.newBuilder().setStoreId(storeId).build());
            if (resp.hasStore()) return resp.getStore().getPeerAddress();
        } catch (Exception e) {
            log.warn("resolve store {} raft addr failed: {}", storeId, e.getMessage());
        }
        return null;
    }

    /**
     * Spawn a freshly-split child region as a live RegionPeer.
     * Uses the store-level ConcurrencyManager for consistent max_ts tracking.
     */
    private void spawnChildPeer(Metapb.Region childRegion,
                                 Metapb.Peer childSelf,
                                 Map<Long, String> peerAddrs,
                                 SnapshotEngineImpl snapshotEngine) {
        long childPeerId = childSelf.getId();
        long childRegionId = childRegion.getId();
        var childRaftEngine = new PerRegionRaftEngine(engine, childRegionId);

        var childTransport = new GrpcRaftTransport(childRegionId, childPeerId, config.storeId(), config.raftTls());
        for (var cpe : childRegion.getPeersList()) {
            if (cpe.getId() == childPeerId) continue;
            var addr = peerAddrs.get(cpe.getStoreId());
            if (addr == null) addr = peerAddrs.get(cpe.getId());
            if (addr != null) childTransport.addPeer(cpe.getId(), addr, cpe.getStoreId());
        }

        var childPeers = new ArrayList<Peer>();
        for (var pe : childRegion.getPeersList()) childPeers.add(new Peer(pe.getId()));

        // Use store-level CM: consistent max_ts across all regions.
        var childCm = storeCm != null ? storeCm
                : new ConcurrencyManager(new MaxTsTracker(childRaftEngine.persistedMaxTs()));

        var childPeerHolder = new AtomicReference<BatchRegionPeer>();
        var childMergeObserver = (AdminApplyHandler.MergeObserver)
                (mergedTarget, sourceRegion) -> {
                    var cp = childPeerHolder.get();
                    if (cp != null) cp.updateRegion(mergedTarget);
                    store.destroyPeer(sourceRegion.getId());
                    peers.removeIf(rp -> rp.regionId() == sourceRegion.getId());
                };

        var childHandler = CompositeApplyHandler.defaultFor(engine, childCm, childRegionId, cdcEventBus,
                        inMemoryLockTable, resolvedTsTracker)
                .withAdmin(childRaftEngine, engine, (p, ch) -> {}, childMergeObserver);

        var childPeer = new BatchRegionPeer(
                engine, childRaftEngine, childRegion, childSelf, childPeers,
                childTransport, childHandler,
                new RegionPeerImpl.Settings(10, 1, config.raft().heartbeatTickMs(),
                        config.raft().leaseBasedRead()),
                childCm, snapshotEngine, raftPoller, tickDriver, applyWorker);
        childPeerHolder.set(childPeer);
        childPeer.setChangePeerObserver((type, changedPeer, updatedRegion) ->
                onChangePeer(childTransport, type, changedPeer));
        dispatcher.register(childRegionId, childTransport);
        store.registerPeer(childPeer);
        peers.add(childPeer);

        if (pdManager != null) {
            startHeartbeater(childPeer);
        }

        if (copService != null) copService.invalidateCache();
        log.info("Spawned child peer region={} peer={}", childRegionId, childPeerId);
    }

    /**
     * On-demand region spawn: a raft message arrived for a region this store
     * doesn't yet host. Query PD, find this store's peer slot, and create it.
     */
    private void spawnOnDemand(long regionId) {
        try {
            var resp = pdManager.blockingStub().getRegionByID(Pdpb.GetRegionByIDRequest.newBuilder()
                    .setRegionId(regionId).build());
            if (!resp.hasRegion()) {
                log.warn("on-demand spawn: PD has no region={}", regionId);
                return;
            }
            var regionDesc = resp.getRegion();
            var selfPeer = regionDesc.getPeersList().stream()
                    .filter(p -> p.getStoreId() == config.storeId())
                    .findFirst().orElse(null);
            if (selfPeer == null) {
                log.warn("on-demand spawn: region={} has no peer on store={}",
                        regionId, config.storeId());
                return;
            }
            if (store.peerForRegion(regionId).isPresent()) return;

            var peerAddrs = resolvePeerAddresses(pdManager.blockingStub(), regionDesc);
            var snapshotEngine = new SnapshotEngineImpl(engine, config.dataDir().resolve("snap"));
            spawnChildPeer(regionDesc, selfPeer, peerAddrs, snapshotEngine);
            log.info("on-demand spawn: region={} peer={} on store={} created",
                    regionId, selfPeer.getId(), config.storeId());
        } catch (Throwable err) {
            log.warn("on-demand spawn for region={} failed: {}", regionId, err.getMessage());
        } finally {
            dispatcher.onSpawnDone(regionId);
        }
    }

    // =====================================================================
    // Region recovery from local RAFT CF
    // =====================================================================

    /**
     * Recover all regions persisted in the local RAFT CF.
     * This is the primary recovery path on restart — we never query PD
     * for "which regions does this store host".
     */
    private void recoverPersistedRegions(PDGrpc.PDBlockingStub pdStub,
                                         SnapshotEngineImpl snapshotEngine) {
        byte[] prefix = RaftCfKeys.allRegionKeysPrefix();
        byte[] end = RaftCfKeys.allRegionKeysEnd();
        int recovered = 0;
        try (var ro = engine.newReadOptions()
                .iterateLowerBound(prefix)
                .iterateUpperBound(end);
             var it = engine.newIterator(StorageEngine.Cf.RAFT, ro)) {
            for (it.seek(prefix); it.isValid(); it.next()) {
                long regionId = RaftCfKeys.regionIdFromKey(it.key());
                if (store.peerForRegion(regionId).isPresent()) continue;

                Metapb.Region region;
                try {
                    region = Metapb.Region.parseFrom(it.value());
                } catch (Exception e) {
                    log.warn("skipping unreadable region descriptor for regionId={}: {}",
                            regionId, e.getMessage());
                    continue;
                }

                var selfPeer = region.getPeersList().stream()
                        .filter(p -> p.getStoreId() == config.storeId())
                        .findFirst().orElse(null);
                if (selfPeer == null) continue;

                var peerAddrs = resolvePeerAddresses(pdStub, region);
                var childRaftEngine = new PerRegionRaftEngine(engine, regionId);
                var childCm = new ConcurrencyManager(
                        new MaxTsTracker(childRaftEngine.persistedMaxTs()));
                var peer = createRegionPeer(region, peerAddrs, snapshotEngine,
                        childRaftEngine, childCm);
                store.registerPeer(peer);
                peers.add(peer);
                recovered++;

                log.debug("recovered persisted region={} range=[{}, {})",
                        regionId,
                        region.getStartKey().toStringUtf8(),
                        region.getEndKey().toStringUtf8());
            }
        }
        if (recovered > 0) {
            log.info("Recovered {} region(s) from RAFT CF", recovered);
        }
    }

    // =====================================================================
    // Heartbeaters
    // =====================================================================

    private void startHeartbeater(RegionPeer peer) {
        var pdAsyncStub = pdManager.asyncStub();
        var splitDriver = new SplitDriver(pdManager.blockingStub(), PROPOSE_TIMEOUT_MS);
        var hb = new RegionHeartbeater(pdAsyncStub, peer,
                HEARTBEAT_INTERVAL_MS, splitDriver::split);
        hb.start();
        heartbeaters.add(hb);
    }

    // =====================================================================
    // Lifecycle
    // =====================================================================

    public void awaitTermination() throws InterruptedException {
        if (clientServer != null) clientServer.awaitTermination();
    }

    /**
     * Drain in-flight traffic before shutdown:
     * 1. Reject new client requests (DrainingInterceptor).
     * 2. Notify PD that this store is going offline.
     * 3. Transfer leadership of all regions to surviving peers.
     * 4. Wait up to drainTimeoutMs for transfers to complete.
     */
    public void drain() {
        if (drainingInterceptor != null) {
            drainingInterceptor.startDraining();
        }
        log.info("KV store {} draining", config.storeId());

        // Notify PD: mark store as Offline.
        if (pdManager != null) {
            try {
                var offlineStore = Metapb.Store.newBuilder()
                        .setId(config.storeId())
                        .setAddress(config.clientAddress())
                        .setPeerAddress(config.raftAddress())
                        .setState(Metapb.StoreState.Offline)
                        .build();
                pdManager.blockingStub().putStore(
                        Pdpb.PutStoreRequest.newBuilder().setStore(offlineStore).build());
            } catch (Throwable t) {
                log.warn("drain: failed to notify PD of offline state: {}", t.getMessage());
            }
        }

        // Transfer leadership of all leader regions to another peer.
        for (var peer : peers) {
            if (!peer.isLeader()) continue;
            var otherPeer = peer.region().getPeersList().stream()
                    .filter(p -> p.getStoreId() != config.storeId())
                    .findFirst().orElse(null);
            if (otherPeer == null) continue;
            try {
                peer.transferLeader(otherPeer.getId());
            } catch (Throwable t) {
                log.warn("drain: leader transfer failed for region={}: {}",
                        peer.regionId(), t.getMessage());
            }
        }

        // Wait for leadership to migrate away.
        long deadline = System.currentTimeMillis() + config.drainTimeoutMs();
        while (System.currentTimeMillis() < deadline) {
            boolean anyLeader = peers.stream().anyMatch(RegionPeer::isLeader);
            if (!anyLeader) break;
            try { Thread.sleep(100); } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
        }

        boolean remaining = peers.stream().anyMatch(RegionPeer::isLeader);
        if (remaining) {
            log.warn("drain: timed out waiting for leader transfer ({}ms)", config.drainTimeoutMs());
        } else {
            log.info("drain: all leaders transferred successfully");
        }
    }

    /**
     * Graceful shutdown. Order: drain → heartbeaters → gRPC servers → peers
     * → PD channels → engine. This prevents RocksDB use-after-close.
     */
    public void stop() {
        log.info("KV store {} shutting down", config.storeId());

        // 0) Drain traffic and transfer leaders.
        drain();

        // 1) Stop heartbeaters.
        CloseUtils.closeQuietly(log, "storeHeartbeater", storeHeartbeater);
        for (var hb : heartbeaters) {
            CloseUtils.closeQuietly(log, "regionHeartbeater", hb);
        }
        heartbeaters.clear();

        // 1b) Stop CDC service.
        if (cdcService != null) {
            try { cdcService.close(); } catch (Exception e) {
                log.warn("failed to close cdcService: {}", e.getMessage(), e);
            }
        }

        // 1c) Stop background workers.
        CloseUtils.closeQuietly(log, "logCompactionWorker", logCompactionWorker);
        CloseUtils.closeQuietly(log, "gcWorker", gcWorker);

        // 2) Shut down gRPC servers.
        for (Server s : new Server[]{clientServer, raftServer}) {
            if (s == null) continue;
            s.shutdown();
            try {
                if (!s.awaitTermination(5, TimeUnit.SECONDS)) {
                    s.shutdownNow();
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }

        // 2b) Shut down coprocessor thread pool.
        if (tikvService != null) tikvService.shutdown();

        // 3) Shut down all peers.
        for (var peer : peers) {
            try { peer.shutdown(); } catch (Throwable t) {
                log.warn("peer shutdown failed: {}", t.getMessage(), t);
            }
        }

        // 3b) Shut down BatchSystem: apply pool → poller → tick driver.
        if (applyWorker != null) applyWorker.shutdown();
        if (tickDriver != null) tickDriver.shutdown();
        if (raftPoller != null) raftPoller.shutdown();
        peers.clear();

        // 4) Close PD connection.
        CloseUtils.closeQuietly(log, "pdManager", pdManager);

        // 5) Close metrics HTTP server.
        CloseUtils.closeQuietly(log, "metricsHttpServer", metricsHttpServer);

        // 6) Close storage engine.
        CloseUtils.closeQuietly(log, "storageEngine", engine);

        log.info("KV store {} stopped", config.storeId());
    }

    /** Visible for tests and metrics. */
    public StoreImpl store() { return store; }
    public RocksStorageEngine engine() { return engine; }

    private TikvServiceImpl buildTikvService(RawKvService rawKv, TransactionService txn,
                                              CoprocessorService cop,
                                              io.github.xinfra.lab.xkv.kv.store.SplitDriver splitDriver,
                                              RawKvService.PeerLocator locator) {
        var svc = new TikvServiceImpl(rawKv, txn, cop, splitDriver, locator);
        var backupTmp = config.dataDir().resolve("backup-tmp");
        var restoreTmp = config.dataDir().resolve("restore-tmp");
        svc.setBackupManager(new io.github.xinfra.lab.xkv.kv.backup.BackupManager(engine, backupTmp));
        svc.setRestoreManager(new io.github.xinfra.lab.xkv.kv.backup.RestoreManager(engine, restoreTmp));
        return svc;
    }

    // ---- helpers ----

    public static void main(String[] args) throws Exception {
        var cfg = io.github.xinfra.lab.xkv.kv.config.KvConfigLoader.load(args);
        var srv = new KvServer(cfg);
        srv.start();
        Runtime.getRuntime().addShutdownHook(new Thread(srv::stop, "kv-shutdown"));
        srv.awaitTermination();
    }
}
