package io.github.xinfra.lab.xkv.test;

import com.google.protobuf.ByteString;
import io.github.xinfra.lab.raft.Peer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import io.github.xinfra.lab.xkv.kv.config.KvConfig;
import io.github.xinfra.lab.xkv.kv.engine.PerRegionRaftEngine;
import io.github.xinfra.lab.xkv.kv.engine.RocksStorageEngine;
import io.github.xinfra.lab.xkv.kv.raft.CompositeApplyHandler;
import io.github.xinfra.lab.xkv.kv.raft.RegionPeerImpl;
import io.github.xinfra.lab.xkv.kv.server.KvRaftServiceImpl;
import io.github.xinfra.lab.xkv.kv.server.RawKvService;
import io.github.xinfra.lab.xkv.kv.server.TikvServiceImpl;
import io.github.xinfra.lab.xkv.kv.server.TransactionService;
import io.github.xinfra.lab.xkv.kv.store.RegionHeartbeater;
import io.github.xinfra.lab.xkv.kv.transport.GrpcRaftTransport;
import io.github.xinfra.lab.xkv.kv.transport.RaftMessageDispatcher;
import io.github.xinfra.lab.xkv.pd.config.PdConfig;
import io.github.xinfra.lab.xkv.pd.server.PdServer;
import io.github.xinfra.lab.xkv.proto.Metapb;
import io.github.xinfra.lab.xkv.proto.PDGrpc;
import io.github.xinfra.lab.xkv.proto.Pdpb;
import io.github.xinfra.lab.xkv.proto.TikvGrpc;
import io.grpc.ManagedChannel;
import io.grpc.Server;
import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder;
import io.grpc.netty.shaded.io.grpc.netty.NettyServerBuilder;
import org.awaitility.Awaitility;

import java.net.ServerSocket;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Test-only cluster harness: brings up 1 PD + N KV stores in this process.
 *
 * <p>Each KV store hosts a single region with N peers (one per store).
 * Stores are wired with a real {@link GrpcRaftTransport} so the cluster
 * exercises the production raft transport, not loopback.
 *
 * <p>The harness registers the bootstrap region + all stores into PD,
 * so a client can discover routing through PD.
 */
public final class ClusterHarness implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(ClusterHarness.class);

    private final Path baseDir;
    private final int kvCount;
    private PdServer pd;

    public PdServer pdServer() { return pd; }
    private int pdPort;
    private final List<KvNode> kvNodes = new ArrayList<>();
    private long bootstrapRegionId = 1;

    public ClusterHarness(Path baseDir, int kvCount) {
        this.baseDir = baseDir;
        this.kvCount = kvCount;
    }

    public int pdPort() { return pdPort; }
    public List<KvNode> kvNodes() { return kvNodes; }
    public long bootstrapRegionId() { return bootstrapRegionId; }

    public ClusterHarness start() throws Exception {
        // 1) Boot PD on a free port.
        pdPort = freePort();
        int pdRaftPort = freePort();
        var pdCfg = PdConfig.builder()
                .nodeId(1)
                .clusterId(1)
                .clientAddress("127.0.0.1:" + pdPort)
                .raftAddress("127.0.0.1:" + pdRaftPort)
                .dataDir(Files.createDirectories(baseDir.resolve("pd")))
                .build();
        releasePort(pdPort);
        releasePort(pdRaftPort);
        pd = new PdServer(pdCfg);
        pd.start();

        // 2) Allocate ports for all KV stores up-front.
        int[] clientPorts = new int[kvCount];
        int[] raftPorts = new int[kvCount];
        for (int i = 0; i < kvCount; i++) {
            clientPorts[i] = freePort();
            raftPorts[i] = freePort();
        }

        // 3) Build the bootstrap region descriptor with all peers.
        var regionBuilder = Metapb.Region.newBuilder()
                .setId(bootstrapRegionId)
                .setRegionEpoch(Metapb.RegionEpoch.newBuilder().setConfVer(1).setVersion(1));
        for (int i = 0; i < kvCount; i++) {
            long peerId = i + 1;
            long storeId = peerId;
            regionBuilder.addPeers(Metapb.Peer.newBuilder().setId(peerId).setStoreId(storeId).setRole(Metapb.PeerRole.Voter));
        }
        var region = regionBuilder.build();

        // 4) Bootstrap PD with the first store + region.
        var pdChannel = NettyChannelBuilder.forAddress("127.0.0.1", pdPort).usePlaintext().build();
        var pdStub = PDGrpc.newBlockingStub(pdChannel);
        var firstStore = Metapb.Store.newBuilder()
                .setId(1).setAddress("127.0.0.1:" + clientPorts[0])
                .setPeerAddress("127.0.0.1:" + raftPorts[0])
                .setState(Metapb.StoreState.Up)
                .build();
        pdStub.bootstrap(Pdpb.BootstrapRequest.newBuilder()
                .setStore(firstStore).setRegion(region).build());

        for (int i = 1; i < kvCount; i++) {
            var s = Metapb.Store.newBuilder()
                    .setId(i + 1).setAddress("127.0.0.1:" + clientPorts[i])
                    .setPeerAddress("127.0.0.1:" + raftPorts[i])
                    .setState(Metapb.StoreState.Up)
                    .build();
            pdStub.putStore(Pdpb.PutStoreRequest.newBuilder().setStore(s).build());
        }
        pdChannel.shutdownNow().awaitTermination(2, TimeUnit.SECONDS);

        // 5) Build each KV node. Release reserved sockets just before bind.
        var raftAddrs = new java.util.HashMap<Long, String>();
        for (int i = 0; i < kvCount; i++) raftAddrs.put((long)(i + 1), "127.0.0.1:" + raftPorts[i]);

        for (int i = 0; i < kvCount; i++) {
            kvNodes.add(buildNode(i + 1, region, clientPorts[i], raftPorts[i], raftAddrs));
        }

        // 6) Each KV node now has a real RegionHeartbeater wiring it to PD;
        //    the leader's heartbeat publishes the elected leader to PD's
        //    region table so clients route correctly.
        var pdAsyncStub = io.github.xinfra.lab.xkv.proto.PDGrpc.newStub(
                io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder
                        .forAddress("127.0.0.1", pdPort).usePlaintext().build());
        // Each node gets a SplitTrigger that delegates to its own SplitDriver
        // (talks to the same PD for ID allocation). The trigger only fires on
        // the leader peer (RegionHeartbeater checks isLeader before dispatch).
        for (var n : kvNodes) {
            var pdBlockingForSplit = io.github.xinfra.lab.xkv.proto.PDGrpc.newBlockingStub(
                    io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder
                            .forAddress("127.0.0.1", pdPort).usePlaintext().build());
            var splitDriver = new io.github.xinfra.lab.xkv.kv.store.SplitDriver(
                    pdBlockingForSplit, 10_000);
            io.github.xinfra.lab.xkv.kv.store.RegionHeartbeater.SplitTrigger trigger =
                    splitDriver::split;
            var hb = new RegionHeartbeater(pdAsyncStub, n.peer, 100, trigger, n.engine);
            hb.start();
            n.heartbeater = hb;
        }

        // Wait for leader election + initial heartbeat.
        Awaitility.await().atMost(Duration.ofSeconds(15))
                .until(() -> kvNodes.stream().anyMatch(n -> n.peer.isLeader()));
        // Wait one heartbeat cycle for PD to learn who the leader is
        // via the dedicated leader field (not peer ordering).
        Awaitility.await()
                .atMost(Duration.ofSeconds(10))
                .pollInterval(Duration.ofMillis(50))
                .until(() -> {
                    var leader = kvNodes.stream().filter(n -> n.peer.isLeader()).findFirst().orElse(null);
                    if (leader == null) return false;
                    var pdLeader = pd.state().getLeader(bootstrapRegionId).orElse(null);
                    return pdLeader != null && pdLeader.getId() == leader.peerId;
                });
        return this;
    }

    /**
     * Publish the current leader to PD's leader cache. The heartbeater does
     * this automatically, but manual publishing is useful in tests that need
     * the leader info to be visible to PD immediately.
     */
    public void publishLeaderToPd() {
        var leader = kvNodes.stream().filter(n -> n.peer.isLeader()).findFirst().orElse(null);
        if (leader == null) return;
        pd.state().updateLeader(bootstrapRegionId, leader.peer.self());
    }

    public KvNode leader() {
        return kvNodes.stream().filter(n -> n.peer.isLeader()).findFirst().orElseThrow();
    }

    /**
     * Restart a previously-shut-down node using the same dataDir, ports, and
     * peer addresses. RocksDB re-opens the existing data; the raft engine
     * recovers term/vote/log from the RAFT CF; and RegionPeerImpl detects
     * a non-fresh state and calls {@code Node.restartNode}.
     *
     * <p>The node is re-added to {@link #kvNodes} and its heartbeater is
     * wired to PD. On return the peer is running but may still be catching
     * up with the leader via raft log replay or snapshot install.
     *
     * @param peerId    the store/peer ID of the node to restart
     * @param raftAddrs peer address map (peerId → "host:port") — same as the
     *                  original start()
     */
    public KvNode restartNode(long peerId, java.util.Map<Long, String> raftAddrs) throws Exception {
        // Look up the bootstrap region from PD — we need the latest region
        // descriptor (may have been updated by split/merge/conf-change).
        var pdChannel = NettyChannelBuilder.forAddress("127.0.0.1", pdPort)
                .usePlaintext().build();
        try {
            var pdStub = PDGrpc.newBlockingStub(pdChannel);
            var regionResp = pdStub.getRegionByID(io.github.xinfra.lab.xkv.proto.Pdpb.GetRegionByIDRequest
                    .newBuilder().setRegionId(bootstrapRegionId).build());
            var region = regionResp.getRegion();

            // Find original ports from peerAddrs and dataDir convention.
            int clientPort = -1, raftPort = -1;
            String raftAddr = raftAddrs.get(peerId);
            if (raftAddr != null) {
                raftPort = Integer.parseInt(raftAddr.split(":")[1]);
            }
            // We need the client port — derive from convention (stored in KvNode).
            // Since we don't store it externally, scan the PD store registry.
            var storeResp = pdStub.getStore(io.github.xinfra.lab.xkv.proto.Pdpb.GetStoreRequest
                    .newBuilder().setStoreId(peerId).build());
            if (storeResp.hasStore()) {
                String addr = storeResp.getStore().getAddress();
                clientPort = Integer.parseInt(addr.split(":")[1]);
            }

            if (clientPort < 0 || raftPort < 0) {
                throw new IllegalStateException("cannot resolve ports for peer " + peerId);
            }

            var node = buildNode(peerId, region, clientPort, raftPort, raftAddrs);

            // Wire heartbeater.
            var pdAsyncStub = PDGrpc.newStub(
                    NettyChannelBuilder.forAddress("127.0.0.1", pdPort)
                            .usePlaintext().build());
            var splitPdStub = PDGrpc.newBlockingStub(
                    NettyChannelBuilder.forAddress("127.0.0.1", pdPort)
                            .usePlaintext().build());
            var splitDriver = new io.github.xinfra.lab.xkv.kv.store.SplitDriver(splitPdStub, 10_000);
            var hb = new RegionHeartbeater(pdAsyncStub, node.peer, 100, splitDriver::split, node.engine);
            hb.start();
            node.heartbeater = hb;

            kvNodes.add(node);
            return node;
        } finally {
            pdChannel.shutdownNow().awaitTermination(2, TimeUnit.SECONDS);
        }
    }

    @Override
    public void close() {
        for (var n : kvNodes) {
            try { n.shutdown(); } catch (Throwable ignored) {}
        }
        kvNodes.clear();
        if (pd != null) pd.stop();
        releaseAllPorts();
    }

    private KvNode buildNode(long peerId, Metapb.Region region,
                             int clientPort, int raftPort,
                             java.util.Map<Long, String> peerAddrs) throws Exception {
        var dataDir = baseDir.resolve("kv-" + peerId);
        Files.createDirectories(dataDir);

        var engine = RocksStorageEngine.open(dataDir, KvConfig.EngineConfig.defaults());
        var raftEngine = new PerRegionRaftEngine(engine, region.getId());

        var dispatcher = new RaftMessageDispatcher();
        var transport = new GrpcRaftTransport(region.getId(), peerId);
        for (var e : peerAddrs.entrySet()) {
            if (e.getKey() != peerId) transport.addPeer(e.getKey(), e.getValue());
        }

        // nodeHolder is set below (after KvNode is constructed). The
        // missing-region handler runs ASYNC after the gRPC server is up,
        // so by the time it fires the nodeHolder is filled.
        var nodeHolder = new java.util.concurrent.atomic.AtomicReference<KvNode>();
        // Create-peer-on-demand: when a raft message arrives for a region
        // we don't host yet (typically because PD just AddPeer'd us), query
        // PD for the region descriptor, find ourselves in its peer list,
        // and spawn a local RegionPeer.
        dispatcher.setMissingHandler((unknownRegionId, firstMsg) -> {
            var n = nodeHolder.get();
            if (n == null) {
                dispatcher.onSpawnDone(unknownRegionId);
                return;
            }
            spawnOnDemand(n, unknownRegionId);
        });

        var raftService = new KvRaftServiceImpl(dispatcher);
        var raftServer = startServerWithRetry(
                port -> NettyServerBuilder.forPort(port).addService(raftService),
                raftPort, 3);

        var peers = new ArrayList<Peer>();
        for (var pe : region.getPeersList()) peers.add(new Peer(pe.getId()));
        var self = region.getPeersList().stream().filter(p -> p.getId() == peerId).findFirst().orElseThrow();

        // Bootstrap MaxTsTracker from any persisted floor — without this, a
        // restarted leader starts at max_ts=0 and can stamp a prewrite whose
        // commit_ts sinks below an in-flight reader's read_ts.
        var cm = new io.github.xinfra.lab.xkv.kv.mvcc.ConcurrencyManager(
                new io.github.xinfra.lab.xkv.kv.mvcc.MaxTsTracker(raftEngine.persistedMaxTs()));
        // Forward-ref so the split observer can update the parent's in-memory
        // descriptor. ADMIN_SPLIT cannot apply before the peer constructor
        // returns (no proposal has been made yet), so the ref is safe to fill
        // post-construct.
        var peerHolder = new java.util.concurrent.atomic.AtomicReference<RegionPeerImpl>();
        // We need a KvNode-shaped context for spawnChildPeer, but the node
        // isn't built yet. nodeHolder is the one declared at the top of this
        // method (shared with the missing-region handler).
        var splitObserver = (io.github.xinfra.lab.xkv.kv.raft.AdminApplyHandler.SplitObserver)
                (updatedParent, children) -> {
                    var p = peerHolder.get();
                    if (p != null) p.updateRegion(updatedParent);
                    // Spawn the child as a live RegionPeer for THIS store's
                    // peer slot. Picks the peer whose store_id matches; if
                    // none, this store is not part of the child's replica set
                    // (only happens with placement rules; ignore for now).
                    var n = nodeHolder.get();
                    if (n == null) return;
                    for (var child : children) {
                        var childSelfOpt = child.getPeersList().stream()
                                .filter(pe -> pe.getStoreId() == peerId)
                                .findFirst();
                        if (childSelfOpt.isEmpty()) continue;
                        try { spawnChildPeer(n, child, childSelfOpt.get()); }
                        catch (Exception e) {
                            throw new RuntimeException(e);
                        }
                    }
                };
        var mergeObserver = (io.github.xinfra.lab.xkv.kv.raft.AdminApplyHandler.MergeObserver)
                (mergedTarget, sourceRegion) -> {
                    var p = peerHolder.get();
                    if (p != null) p.updateRegion(mergedTarget);
                    var n = nodeHolder.get();
                    if (n == null) return;
                    // Destroy source peer locally. It's either the main peer
                    // (rare — source = parent) or in childPeers (common —
                    // source was a split child).
                    if (n.peer.regionId() == sourceRegion.getId()) {
                        // Cannot destroy ourselves — would shut down the
                        // node. Should never happen if merge target ≠ source.
                        log.warn("merge observer asked to destroy node's main peer");
                    } else {
                        n.childPeers.removeIf(c -> {
                            if (c.regionId() == sourceRegion.getId()) {
                                try { n.store.destroyPeer(c.regionId()); }
                                catch (Throwable t) { log.warn("destroy source peer failed: {}", t.getMessage()); }
                                return true;
                            }
                            return false;
                        });
                    }
                };
        // SnapshotEngine for follower catch-up via raft snapshots.
        var snapshotEngine = new io.github.xinfra.lab.xkv.kv.engine.SnapshotEngineImpl(
                engine, dataDir.resolve("snap"));
        var peer = new RegionPeerImpl(
                engine, raftEngine, region, self, peers,
                transport,
                CompositeApplyHandler.defaultFor(engine, cm)
                        .withAdmin(raftEngine, engine, splitObserver, mergeObserver),
                new RegionPeerImpl.Settings(10, 1, 30),
                cm,
                snapshotEngine);
        peerHolder.set(peer);
        dispatcher.register(region.getId(), transport);

        // Store routes by key — split children registered via the observer
        // become visible to subsequent RPCs without further wiring.
        var store = new io.github.xinfra.lab.xkv.kv.store.StoreImpl(peerId,
                io.github.xinfra.lab.xkv.proto.Metapb.Store.newBuilder().setId(peerId).build());
        store.registerPeer(peer);

        var rawKv = new RawKvService(engine, key -> store.peerForKey(key).orElse(peer), 10_000);
        var txn = new TransactionService(engine, key -> store.peerForKey(key).orElse(peer), 10_000, cm);

        // SplitDriver needs a PD blocking stub; we open one per node so the
        // store-side SplitRegion RPC can drive split end-to-end via PD.
        var pdChannelForSplit = NettyChannelBuilder.forAddress("127.0.0.1", pdPort)
                .usePlaintext().build();
        var splitPdStub = io.github.xinfra.lab.xkv.proto.PDGrpc.newBlockingStub(pdChannelForSplit);
        var splitDriver = new io.github.xinfra.lab.xkv.kv.store.SplitDriver(splitPdStub, 10_000);

        // Deadlock detector client — same PD, separate channel so a slow
        // detect call can't backpressure split allocs.
        var pdChannelForDeadlock = NettyChannelBuilder.forAddress("127.0.0.1", pdPort)
                .usePlaintext().build();
        var deadlockPdStub = io.github.xinfra.lab.xkv.proto.PDGrpc.newBlockingStub(pdChannelForDeadlock);
        var deadlockClient = new io.github.xinfra.lab.xkv.kv.transport.DeadlockClient(
                deadlockPdStub, /* clusterId= */ 1L);
        txn.setDeadlockClient(deadlockClient);

        var tikvService = new TikvServiceImpl(rawKv, txn, splitDriver,
                key -> store.peerForKey(key).orElse(peer));
        var clientServer = startServerWithRetry(
                port -> NettyServerBuilder.forPort(port).addService(tikvService),
                clientPort, 3);

        var node = new KvNode(peerId, clientPort, raftPort, engine, peer, raftServer, clientServer, transport);
        node.store = store;
        node.dispatcher = dispatcher;
        node.peerAddrs = new java.util.HashMap<>(peerAddrs);
        node.pdChannels.add(pdChannelForSplit);
        node.pdChannels.add(pdChannelForDeadlock);
        nodeHolder.set(node);
        return node;
    }

    /**
     * Spawn a freshly-split child region as a live {@link RegionPeerImpl}
     * on this store. Uses the same engine + dispatcher; allocates a new
     * raft engine for the child's region_id and a new transport bound to
     * that child's peer_id.
     */
    /**
     * Spawn a brand-new {@link RegionPeerImpl} on demand: the dispatcher
     * just received a raft message for a region this store doesn't yet
     * host. Query PD for the region descriptor, find this store's peer in
     * the peer list, and spawn it.
     *
     * <p>Runs on the gRPC server thread but kicks off a daemon for the
     * actual spawn so we don't block message dispatching.
     */
    private void spawnOnDemand(KvNode hostNode, long regionId) {
        var t = new Thread(() -> {
            try {
                // PD blocking stub — shared per harness; build per-call so
                // we don't tie up gRPC server thread.
                var pdChannel = io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder
                        .forAddress("127.0.0.1", pdPort).usePlaintext().build();
                io.github.xinfra.lab.xkv.proto.Metapb.Region regionDesc;
                try {
                    var pdStub = io.github.xinfra.lab.xkv.proto.PDGrpc.newBlockingStub(pdChannel);
                    var resp = pdStub.getRegionByID(
                            io.github.xinfra.lab.xkv.proto.Pdpb.GetRegionByIDRequest.newBuilder()
                                    .setRegionId(regionId).build());
                    if (!resp.hasRegion()) {
                        log.warn("on-demand spawn: PD has no region={}", regionId);
                        return;
                    }
                    regionDesc = resp.getRegion();
                } finally {
                    pdChannel.shutdownNow().awaitTermination(2, TimeUnit.SECONDS);
                }
                // Find self in the region's peer list.
                var selfPeer = regionDesc.getPeersList().stream()
                        .filter(p -> p.getStoreId() == hostNode.peerId)
                        .findFirst().orElse(null);
                if (selfPeer == null) {
                    log.warn("on-demand spawn: region={} has no peer on store={}",
                            regionId, hostNode.peerId);
                    return;
                }
                if (hostNode.store.peerForRegion(regionId).isPresent()) return;     // race
                spawnChildPeer(hostNode, regionDesc, selfPeer);
                log.info("on-demand spawn: region={} peer={} on store={} created",
                        regionId, selfPeer.getId(), hostNode.peerId);
            } catch (Throwable err) {
                log.warn("on-demand spawn for region={} failed: {}", regionId, err.getMessage());
            } finally {
                hostNode.dispatcher.onSpawnDone(regionId);
            }
        }, "spawn-on-demand-" + regionId);
        t.setDaemon(true);
        t.start();
    }

    private void spawnChildPeer(KvNode parentNode,
                                Metapb.Region childRegion,
                                Metapb.Peer childSelf) throws Exception {
        long childPeerId = childSelf.getId();
        long childRegionId = childRegion.getId();
        var childRaftEngine = new PerRegionRaftEngine(parentNode.engine, childRegionId);
        var childPeers = new ArrayList<Peer>();
        for (var pe : childRegion.getPeersList()) childPeers.add(new Peer(pe.getId()));

        var childTransport = new GrpcRaftTransport(childRegionId, childPeerId);
        // The child's peers' store_ids match the parent's, so we look up
        // each child peer's store address via the parent's peerAddrs map
        // (peerId == storeId in this harness).
        for (var cpe : childRegion.getPeersList()) {
            if (cpe.getId() == childPeerId) continue;
            var addr = parentNode.peerAddrs.get(cpe.getStoreId());
            if (addr != null) childTransport.addPeer(cpe.getId(), addr);
        }

        var childCm = new io.github.xinfra.lab.xkv.kv.mvcc.ConcurrencyManager(
                new io.github.xinfra.lab.xkv.kv.mvcc.MaxTsTracker(childRaftEngine.persistedMaxTs()));
        // Forward-ref so the child's merge observer can refresh its own
        // descriptor (when it's the merge TARGET).
        var childPeerHolder = new java.util.concurrent.atomic.AtomicReference<RegionPeerImpl>();
        var childMergeObserver = (io.github.xinfra.lab.xkv.kv.raft.AdminApplyHandler.MergeObserver)
                (mergedTarget, sourceRegion) -> {
                    var cp = childPeerHolder.get();
                    if (cp != null) cp.updateRegion(mergedTarget);
                    // Destroy the source peer on this store.
                    if (parentNode.peer.regionId() == sourceRegion.getId()) {
                        // Source is the node's main peer — rare; production
                        // would tombstone it via raft destroy. For tests
                        // we'd never merge the main peer away mid-flight.
                        log.warn("merge observer (child) asked to destroy main peer");
                    } else {
                        parentNode.childPeers.removeIf(c -> {
                            if (c.regionId() == sourceRegion.getId()) {
                                try { parentNode.store.destroyPeer(c.regionId()); }
                                catch (Throwable t) { log.warn("destroy source peer failed: {}", t.getMessage()); }
                                return true;
                            }
                            return false;
                        });
                    }
                };
        var childHandler = CompositeApplyHandler.defaultFor(parentNode.engine, childCm)
                .withAdmin(childRaftEngine, parentNode.engine, (p, ch) -> {}, childMergeObserver);
        var childPeer = new RegionPeerImpl(
                parentNode.engine, childRaftEngine, childRegion, childSelf, childPeers,
                childTransport,
                childHandler,
                new RegionPeerImpl.Settings(10, 1, 30),
                childCm);
        childPeerHolder.set(childPeer);
        parentNode.dispatcher.register(childRegionId, childTransport);
        parentNode.store.registerPeer(childPeer);
        parentNode.childPeers.add(childPeer);

        var pdAsyncForChild = PDGrpc.newStub(
                NettyChannelBuilder.forAddress("127.0.0.1", pdPort).usePlaintext().build());
        var pdBlockingForChild = PDGrpc.newBlockingStub(
                NettyChannelBuilder.forAddress("127.0.0.1", pdPort).usePlaintext().build());
        var childSplitDriver = new io.github.xinfra.lab.xkv.kv.store.SplitDriver(
                pdBlockingForChild, 10_000);
        var childHb = new RegionHeartbeater(pdAsyncForChild, childPeer, 100,
                childSplitDriver::split, parentNode.engine);
        childHb.start();
        parentNode.childHeartbeaters.add(childHb);
    }

    private static final java.util.List<ServerSocket> reservedSockets =
            java.util.Collections.synchronizedList(new ArrayList<>());

    /**
     * Allocate a free port and HOLD the socket open so no other
     * {@code freePort()} call (from a parallel test class) can collide.
     * Call {@link #releasePort(int)} just before binding.
     */
    public static int freePort() throws Exception {
        var s = new ServerSocket(0);
        reservedSockets.add(s);
        return s.getLocalPort();
    }

    public static void releasePort(int port) {
        reservedSockets.removeIf(s -> {
            if (s.getLocalPort() == port) {
                try { s.close(); } catch (Exception ignored) {}
                return true;
            }
            return false;
        });
    }

    static io.grpc.Server startServerWithRetry(
            java.util.function.Function<Integer, io.grpc.ServerBuilder<?>> builderFactory,
            int preferredPort, int maxRetries) throws Exception {
        releasePort(preferredPort);
        for (int attempt = 0; ; attempt++) {
            int port = (attempt == 0) ? preferredPort : quickFreePort();
            try {
                return builderFactory.apply(port).build().start();
            } catch (java.io.IOException e) {
                if (attempt >= maxRetries) throw e;
                log.warn("bind port {} failed (attempt {}), retrying with new port", port, attempt + 1);
            }
        }
    }

    private static int quickFreePort() throws Exception {
        try (var s = new ServerSocket(0)) {
            return s.getLocalPort();
        }
    }

    static void releaseAllPorts() {
        synchronized (reservedSockets) {
            for (var s : reservedSockets) {
                try { s.close(); } catch (Exception ignored) {}
            }
            reservedSockets.clear();
        }
    }

    /** Public bag holding everything a test might want. */
    public static final class KvNode {
        public final long peerId;
        public final int clientPort;
        public final int raftPort;
        public final RocksStorageEngine engine;
        public final RegionPeerImpl peer;
        public final Server raftServer;
        public final Server clientServer;
        public final GrpcRaftTransport transport;
        public RegionHeartbeater heartbeater;     // attached after init in start()
        public io.github.xinfra.lab.xkv.kv.store.StoreImpl store;
        public RaftMessageDispatcher dispatcher;
        public java.util.Map<Long, String> peerAddrs;
        public final java.util.List<RegionPeerImpl> childPeers = new java.util.ArrayList<>();
        public final java.util.List<RegionHeartbeater> childHeartbeaters = new java.util.ArrayList<>();
        public final java.util.List<ManagedChannel> pdChannels = new java.util.ArrayList<>();

        private ManagedChannel cachedChannel;

        public KvNode(long peerId, int clientPort, int raftPort,
                      RocksStorageEngine engine, RegionPeerImpl peer,
                      Server raftServer, Server clientServer,
                      GrpcRaftTransport transport) {
            this.peerId = peerId;
            this.clientPort = clientPort;
            this.raftPort = raftPort;
            this.engine = engine;
            this.peer = peer;
            this.raftServer = raftServer;
            this.clientServer = clientServer;
            this.transport = transport;
        }

        public TikvGrpc.TikvBlockingStub blockingStub() {
            if (cachedChannel == null) {
                cachedChannel = NettyChannelBuilder.forAddress("127.0.0.1", clientPort)
                        .usePlaintext()
                        .build();
            }
            return TikvGrpc.newBlockingStub(cachedChannel);
        }

        void shutdown() {
            if (heartbeater != null) try { heartbeater.close(); } catch (Exception ignored) {}
            for (var chb : childHeartbeaters) {
                try { chb.close(); } catch (Exception ignored) {}
            }
            if (cachedChannel != null) {
                try { cachedChannel.shutdownNow().awaitTermination(2, TimeUnit.SECONDS); } catch (Exception ignored) {}
            }
            // Stop the gRPC servers FIRST so no in-flight RPC tries to touch
            // RocksDB while we're tearing down the engine.
            try { clientServer.shutdownNow().awaitTermination(2, TimeUnit.SECONDS); } catch (Exception ignored) {}
            try { raftServer.shutdownNow().awaitTermination(2, TimeUnit.SECONDS); } catch (Exception ignored) {}
            // Stop the main peer FIRST — its ready thread is where split
            // applies happen, so stopping it prevents new child peers from
            // being created during shutdown.
            try { peer.shutdown(); } catch (Exception ignored) {}
            // Now stop all child peers (the list is final — no more
            // children can be added because the main peer's ready thread
            // has exited).
            for (var child : childPeers) {
                try { child.shutdown(); } catch (Throwable ignored) {}
            }
            try { transport.close(); } catch (Exception ignored) {}
            // PD channels (split + deadlock) — shut down AFTER the peer is
            // stopped so any in-flight call completes.
            for (var ch : pdChannels) {
                try { ch.shutdownNow().awaitTermination(2, TimeUnit.SECONDS); } catch (Exception ignored) {}
            }
            // Wait for ALL raft event-loop and ready threads to fully exit
            // before closing the engine — a still-running thread would
            // SIGSEGV on the freed RocksDB handle.
            long deadline = System.nanoTime() + 10_000_000_000L;
            while (System.nanoTime() < deadline) {
                boolean anyAlive = peer.readyThreadAlive();
                if (!anyAlive) {
                    anyAlive = childPeers.stream().anyMatch(RegionPeerImpl::readyThreadAlive);
                }
                if (!anyAlive) break;
                try { Thread.sleep(50); } catch (InterruptedException ignored) { break; }
            }
            try { engine.close(); } catch (Exception ignored) {}
        }
    }

    /** Simple raw KV helper used by tests that don't yet have a full client SDK. */
    public static byte[] keyOf(String s) { return s.getBytes(); }
    public static ByteString bs(String s) { return ByteString.copyFromUtf8(s); }
}
