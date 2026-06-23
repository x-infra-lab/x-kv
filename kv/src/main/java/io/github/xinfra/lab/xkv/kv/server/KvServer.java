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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

/**
 * KV store node entrypoint.
 *
 * <p>Boots a fully-functional KV node that:
 * <ol>
 *   <li>Connects to PD and registers this store (or bootstraps the cluster
 *       if it is the first store).</li>
 *   <li>Discovers the initial region assignment from PD.</li>
 *   <li>Opens a shared {@link RocksStorageEngine} at {@code dataDir}.</li>
 *   <li>Creates a {@link RegionPeerImpl} per locally-assigned region, wired
 *       to the raft transport and apply pipeline.</li>
 *   <li>Starts the client-facing gRPC server ({@code Tikv} service) and the
 *       raft transport gRPC server ({@code KvRaft} service).</li>
 *   <li>Starts a {@link RegionHeartbeater} per region to report leader +
 *       region state to PD.</li>
 * </ol>
 *
 * <p>Shutdown order: heartbeaters → gRPC servers → peers → PD channels → engine.
 * This prevents RocksDB use-after-close crashes.
 */
public final class KvServer {
    private static final Logger log = LoggerFactory.getLogger(KvServer.class);

    private static final long PROPOSE_TIMEOUT_MS = 10_000;
    private static final long HEARTBEAT_INTERVAL_MS = 500;

    private final KvConfig config;

    private RocksStorageEngine engine;
    private StoreImpl store;
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
    private CdcEventBus cdcEventBus;
    private ChangeDataServiceImpl cdcService;
    private final io.github.xinfra.lab.xkv.kv.mvcc.InMemoryLockTable inMemoryLockTable =
            new io.github.xinfra.lab.xkv.kv.mvcc.InMemoryLockTable();

    private static final long STORE_HEARTBEAT_INTERVAL_MS = 10_000;

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

        var storeMeta = Metapb.Store.newBuilder()
                .setId(config.storeId())
                .setAddress(config.clientAddress())
                .setPeerAddress(config.raftAddress())
                .setState(Metapb.StoreState.Up)
                .build();

        Metapb.Region initialRegion = bootstrapOrJoin(pdStub, storeMeta);

        // 3) Build store container + CDC event bus.
        store = new StoreImpl(config.storeId(), storeMeta);
        dispatcher = new RaftMessageDispatcher();
        cdcEventBus = new CdcEventBus();

        // 4) Resolve peer addresses from PD for raft transport.
        Map<Long, String> peerAddrs = resolvePeerAddresses(pdStub, initialRegion);

        // 5) Boot the raft gRPC server first so peers can receive messages
        //    as soon as they start.
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

        // 5b) Initialize BatchSystem: shared poller + tick driver.
        raftPoller = new RaftPoller(config.raft().pollerThreads());
        tickDriver = new TickDriver(config.raft().heartbeatTickMs());

        // 6) Create the initial region peer.
        long selfPeerId = findSelfPeerId(initialRegion);
        var raftEngine = new PerRegionRaftEngine(engine, initialRegion.getId());
        var cm = new ConcurrencyManager(
                new MaxTsTracker(raftEngine.persistedMaxTs()));
        var regionPeer = createRegionPeer(initialRegion, peerAddrs, snapshotEngine,
                raftEngine, cm);
        store.registerPeer(regionPeer);
        peers.add(regionPeer);

        // 6b) Recover any additional regions persisted by prior splits.
        recoverPersistedRegions(pdStub, snapshotEngine);

        // 7) Build RPC services backed by the store's peer locator.
        RawKvService.PeerLocator locator = key ->
                store.peerForKey(key).orElse(null);
        rawKvService = new RawKvService(engine, locator, PROPOSE_TIMEOUT_MS);
        txnService = new TransactionService(engine, locator, PROPOSE_TIMEOUT_MS, cm, inMemoryLockTable);

        // Deadlock detector via PD.
        var deadlockClient = new DeadlockClient(pdManager.blockingStub(), /* clusterId= */ 1L);
        txnService.setDeadlockClient(deadlockClient);

        // Split driver.
        var splitDriver = new SplitDriver(pdManager.blockingStub(), PROPOSE_TIMEOUT_MS);

        // 8) Build CoprocessorService and start client-facing gRPC server.
        var copService = new CoprocessorService();
        copService.register(new TableScanCoprocessor(engine));
        copService.register(new io.github.xinfra.lab.xkv.kv.coprocessor.SQLScanCoprocessor(engine));
        copService.register(new io.github.xinfra.lab.xkv.kv.coprocessor.AnalyzeCoprocessor(engine));

        // CDC service — resolved TS uses the initial region's CM.
        cdcService = new ChangeDataServiceImpl(cdcEventBus, () -> cm.maxTs().current());

        var metricsRegistry = XKvMetrics.init("kv");
        var c = GrpcChannelFactory.parseHostPort(config.clientAddress());
        var clientServerBuilder = GrpcChannelFactory.serverBuilder(
                        new InetSocketAddress(c.host(), c.port()), config.clientTls())
                .addService(new TikvServiceImpl(rawKvService, txnService, copService, splitDriver, locator))
                .addService(cdcService);
        if (config.enableDebugService()) {
            clientServerBuilder.addService(
                    new DebugServiceImpl(metricsRegistry, store, engine, storeMeta, config.dataDir()));
        }
        // Interceptor order: last .intercept() is outermost (executed first).
        // Desired: drain → auth → rateLimit → mdc → metrics (innermost)
        clientServerBuilder.intercept(new GrpcServerMetricsInterceptor(metricsRegistry, config.slowLogThresholdMs()));
        clientServerBuilder.intercept(MdcServerInterceptor.forStore(config.storeId()));
        if (config.maxConcurrentRequests() > 0) {
            clientServerBuilder.intercept(new ConcurrencyLimitInterceptor(config.maxConcurrentRequests()));
        }
        if (config.authToken() != null) {
            clientServerBuilder.intercept(new AuthServerInterceptor(config.authToken()));
        }
        drainingInterceptor = new DrainingInterceptor();
        clientServerBuilder.intercept(drainingInterceptor);
        clientServer = clientServerBuilder.build().start();

        // 8b) Start metrics HTTP server if configured.
        if (config.metricsPort() > 0) {
            metricsHttpServer = new MetricsHttpServer(config.metricsPort(), metricsRegistry,
                    () -> store != null && !peers.isEmpty());
        }

        // 9) Start region heartbeater(s) for all loaded peers.
        for (var p : peers) {
            startHeartbeater(p);
        }

        // 10) Start store-level heartbeater.
        storeHeartbeater = new StoreHeartbeater(config.storeId(), pdManager,
                store, config.dataDir(), STORE_HEARTBEAT_INTERVAL_MS);

        // 11) Start background workers.
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

        log.info("KV store {} started: client={} raft={} region={} peers={}",
                config.storeId(), config.clientAddress(), config.raftAddress(),
                initialRegion.getId(), initialRegion.getPeersList().size());
    }

    /**
     * Bootstrap the cluster (if this is the first store) or register this
     * store as a member of an existing cluster.
     *
     * <p>The protocol follows TiKV's bootstrap convention:
     * <ol>
     *   <li>Call {@code GetClusterInfo}. If the cluster is already bootstrapped
     *       (i.e. has a non-empty cluster config), register via {@code PutStore}
     *       and discover the initial region via {@code GetRegionByID(1)}.</li>
     *   <li>Otherwise, this is the first store: call {@code Bootstrap} with
     *       the store metadata and a single region spanning the entire key
     *       space.</li>
     * </ol>
     */
    private Metapb.Region bootstrapOrJoin(PDGrpc.PDBlockingStub pdStub,
                                           Metapb.Store storeMeta) {
        try {
            var clusterInfo = pdStub.getClusterInfo(
                    Pdpb.GetClusterInfoRequest.newBuilder().build());
            if (clusterInfo.hasCluster() && clusterInfo.getCluster().getId() > 0) {
                // Cluster already bootstrapped — register this store and fetch region.
                log.info("Joining existing cluster (id={})", clusterInfo.getCluster().getId());
                pdStub.putStore(Pdpb.PutStoreRequest.newBuilder()
                        .setStore(storeMeta).build());

                // Try to find a region assigned to this store. Start with
                // region 1 (the bootstrap region), then fall back to ScanRegions.
                var resp = pdStub.getRegionByID(Pdpb.GetRegionByIDRequest.newBuilder()
                        .setRegionId(1).build());
                if (resp.hasRegion()) return resp.getRegion();

                // Scan for any region with a peer on this store.
                var scanResp = pdStub.scanRegions(Pdpb.ScanRegionsRequest.newBuilder()
                        .setLimit(100).build());
                for (var region : scanResp.getRegionsList()) {
                    for (var peer : region.getPeersList()) {
                        if (peer.getStoreId() == config.storeId()) return region;
                    }
                }
                throw new IllegalStateException(
                        "No region found for store " + config.storeId() + " in PD");
            }
        } catch (io.grpc.StatusRuntimeException e) {
            if (e.getStatus().getCode() != io.grpc.Status.Code.UNIMPLEMENTED) {
                log.debug("getClusterInfo failed (expected on first boot): {}", e.getMessage());
            }
        }

        // First store — bootstrap the cluster.
        log.info("Bootstrapping cluster with store {}", config.storeId());
        var bootstrapRegion = Metapb.Region.newBuilder()
                .setId(1)
                .setRegionEpoch(Metapb.RegionEpoch.newBuilder()
                        .setConfVer(1).setVersion(1))
                .addPeers(Metapb.Peer.newBuilder()
                        .setId(config.storeId())
                        .setStoreId(config.storeId())
                        .setRole(Metapb.PeerRole.Voter))
                .build();

        pdStub.bootstrap(Pdpb.BootstrapRequest.newBuilder()
                .setStore(storeMeta)
                .setRegion(bootstrapRegion)
                .build());

        return bootstrapRegion;
    }

    /**
     * Resolve raft peer addresses by querying PD for each peer's store
     * metadata. Returns a map of peer_id → raft address.
     */
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

    /**
     * Create a fully-wired {@link BatchRegionPeer} for a region, backed by
     * the shared {@link RaftPoller} and {@link TickDriver}.
     */
    private BatchRegionPeer createRegionPeer(Metapb.Region region,
                                              Map<Long, String> peerAddrs,
                                              SnapshotEngineImpl snapshotEngine,
                                              PerRegionRaftEngine raftEngine,
                                              ConcurrencyManager cm) {
        long peerId = findSelfPeerId(region);

        var transport = new GrpcRaftTransport(region.getId(), peerId, config.raftTls());
        for (var peer : region.getPeersList()) {
            if (peer.getId() == peerId) continue;
            var addr = peerAddrs.get(peer.getId());
            if (addr != null) transport.addPeer(peer.getId(), addr);
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
                                inMemoryLockTable)
                        .withAdmin(raftEngine, engine, splitObserver, mergeObserver),
                settings, cm, snapshotEngine, raftPoller, tickDriver);
        peerHolder.set(peer);
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
     * Spawn a freshly-split child region as a live RegionPeer.
     */
    private void spawnChildPeer(Metapb.Region childRegion,
                                 Metapb.Peer childSelf,
                                 Map<Long, String> peerAddrs,
                                 SnapshotEngineImpl snapshotEngine) {
        long childPeerId = childSelf.getId();
        long childRegionId = childRegion.getId();
        var childRaftEngine = new PerRegionRaftEngine(engine, childRegionId);

        var childTransport = new GrpcRaftTransport(childRegionId, childPeerId, config.raftTls());
        for (var cpe : childRegion.getPeersList()) {
            if (cpe.getId() == childPeerId) continue;
            var addr = peerAddrs.get(cpe.getStoreId());
            if (addr == null) addr = peerAddrs.get(cpe.getId());
            if (addr != null) childTransport.addPeer(cpe.getId(), addr);
        }

        var childPeers = new ArrayList<Peer>();
        for (var pe : childRegion.getPeersList()) childPeers.add(new Peer(pe.getId()));

        var childCm = new ConcurrencyManager(
                new MaxTsTracker(childRaftEngine.persistedMaxTs()));

        var childPeerHolder = new AtomicReference<BatchRegionPeer>();
        var childMergeObserver = (AdminApplyHandler.MergeObserver)
                (mergedTarget, sourceRegion) -> {
                    var cp = childPeerHolder.get();
                    if (cp != null) cp.updateRegion(mergedTarget);
                    store.destroyPeer(sourceRegion.getId());
                    peers.removeIf(rp -> rp.regionId() == sourceRegion.getId());
                };

        var childHandler = CompositeApplyHandler.defaultFor(engine, childCm, childRegionId, cdcEventBus,
                        inMemoryLockTable)
                .withAdmin(childRaftEngine, engine, (p, ch) -> {}, childMergeObserver);

        var childPeer = new BatchRegionPeer(
                engine, childRaftEngine, childRegion, childSelf, childPeers,
                childTransport, childHandler,
                new RegionPeerImpl.Settings(10, 1, config.raft().heartbeatTickMs(),
                        config.raft().leaseBasedRead()),
                childCm, snapshotEngine, raftPoller, tickDriver);
        childPeerHolder.set(childPeer);
        dispatcher.register(childRegionId, childTransport);
        store.registerPeer(childPeer);
        peers.add(childPeer);

        if (pdManager != null) {
            startHeartbeater(childPeer);
        }

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

                log.info("recovered persisted region={} range=[{}, {})",
                        regionId,
                        region.getStartKey().toStringUtf8(),
                        region.getEndKey().toStringUtf8());
            }
        }
        if (recovered > 0) {
            log.info("recovered {} additional region(s) from RAFT CF", recovered);
        }
    }

    private void startHeartbeater(RegionPeer peer) {
        var pdAsyncStub = pdManager.asyncStub();
        var splitDriver = new SplitDriver(pdManager.blockingStub(), PROPOSE_TIMEOUT_MS);
        var hb = new RegionHeartbeater(pdAsyncStub, peer,
                HEARTBEAT_INTERVAL_MS, splitDriver::split);
        hb.start();
        heartbeaters.add(hb);
    }

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

        // 3) Shut down all peers.
        for (var peer : peers) {
            try { peer.shutdown(); } catch (Throwable t) {
                log.warn("peer shutdown failed: {}", t.getMessage(), t);
            }
        }

        // 3b) Shut down BatchSystem poller + tick driver.
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

    // ---- helpers ----

    public static void main(String[] args) throws Exception {
        var cfg = io.github.xinfra.lab.xkv.kv.config.KvConfigLoader.load(args);
        var srv = new KvServer(cfg);
        srv.start();
        Runtime.getRuntime().addShutdownHook(new Thread(srv::stop, "kv-shutdown"));
        srv.awaitTermination();
    }
}
