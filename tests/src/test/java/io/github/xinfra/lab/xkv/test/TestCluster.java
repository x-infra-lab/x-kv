package io.github.xinfra.lab.xkv.test;

import com.google.protobuf.ByteString;
import io.github.xinfra.lab.xkv.client.TxnClient;
import io.github.xinfra.lab.xkv.client.XKvClient;
import io.github.xinfra.lab.xkv.client.config.ClientConfig;
import io.github.xinfra.lab.xkv.client.pd.PdClient;
import io.github.xinfra.lab.xkv.client.raw.RawKvClient;
import io.github.xinfra.lab.xkv.kv.config.KvConfig;
import io.github.xinfra.lab.xkv.kv.raft.RegionPeer;
import io.github.xinfra.lab.xkv.kv.server.KvServer;
import io.github.xinfra.lab.xkv.kv.store.StoreImpl;
import io.github.xinfra.lab.xkv.pd.config.PdConfig;
import io.github.xinfra.lab.xkv.pd.config.PdConfig.PeerAddress;
import io.github.xinfra.lab.xkv.pd.server.PdServer;
import io.github.xinfra.lab.xkv.proto.Metapb;
import io.github.xinfra.lab.xkv.proto.PDGrpc;
import io.github.xinfra.lab.xkv.proto.Pdpb;
import io.github.xinfra.lab.xkv.proto.TikvGrpc;
import io.grpc.ManagedChannel;
import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder;
import org.awaitility.Awaitility;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.ServerSocket;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;

/**
 * Black-box cluster harness for end-to-end tests.
 *
 * <p>Unlike the legacy hand-wired harness — which built {@code RegionPeer}s,
 * transports, and heartbeaters in-process — this harness treats PD, KV, and the
 * client SDK as opaque units. It launches the <em>production</em>
 * {@link PdServer} and {@link KvServer} entrypoints (in-process, but through
 * their real {@code start()}/{@code stop()} lifecycle and real bootstrap / join
 * / heartbeat / on-demand-spawn paths) and observes the cluster primarily
 * through the client SDK and PD's public gRPC API.
 *
 * <p>Because the servers run in-process, tests that genuinely need to inspect
 * or drive internal state can obtain the <em>real</em> running server handles
 * ({@link StoreNode#server}, {@link #kvStore}, {@link #realPeer}, {@link #pd})
 * without any hand-wiring.
 *
 * <p>Capabilities:
 * <ul>
 *   <li>Controllable PD node count (single PD, or a multi-node raft PD group
 *       when {@code pdCount > 1}).</li>
 *   <li>Controllable initial KV store count.</li>
 *   <li>Dynamic {@link #addStore()} / {@link #removeStore(long)} and chaos
 *       {@link #killStore(long)} / {@link #restartStore(long)}.</li>
 *   <li>Dynamic {@link #addPd()} / {@link #removePd(long)} (raft PD only).</li>
 *   <li>Topology / leader queries for a region and access to the real
 *       in-process {@link RegionPeer} handles.</li>
 * </ul>
 */
public final class TestCluster implements AutoCloseable {
    private static final Logger log = LoggerFactory.getLogger(TestCluster.class);

    /** The bootstrap region id created by the first KV store. */
    public static final long BOOTSTRAP_REGION_ID = 1;

    /** Handle to one PD node. */
    public static final class PdNode {
        public final long nodeId;
        public final int clientPort;
        public final int raftPort;
        public final PdServer server;

        PdNode(long nodeId, int clientPort, int raftPort, PdServer server) {
            this.nodeId = nodeId;
            this.clientPort = clientPort;
            this.raftPort = raftPort;
            this.server = server;
        }

        public String clientAddress() { return "127.0.0.1:" + clientPort; }
        public String raftAddress()   { return "127.0.0.1:" + raftPort; }
    }

    /** Handle to one KV store node. */
    public static final class StoreNode {
        public final long storeId;
        public final int clientPort;
        public final int raftPort;
        public final KvServer server;

        StoreNode(long storeId, int clientPort, int raftPort, KvServer server) {
            this.storeId = storeId;
            this.clientPort = clientPort;
            this.raftPort = raftPort;
            this.server = server;
        }

        public String clientAddress() { return "127.0.0.1:" + clientPort; }

        /** The real in-process store container. */
        public StoreImpl store() { return server.store(); }
    }

    private final Path baseDir;
    private final List<PdNode> pdNodes = new CopyOnWriteArrayList<>();
    private final List<StoreNode> storeNodes = new CopyOnWriteArrayList<>();
    private final List<AutoCloseable> clients = new CopyOnWriteArrayList<>();
    private final java.util.Map<Long, ManagedChannel> kvChannels = new ConcurrentHashMap<>();
    /** Remembers ports of stores that were killed, so they can be restarted on the same address. */
    private final java.util.Map<Long, int[]> stoppedStorePorts = new ConcurrentHashMap<>();

    private PdClient pdClient;      // observation + membership ops (connects to leader)
    private long nextStoreId = 1;
    private boolean pdRaftMode;
    private PdConfig.SchedulerConfig schedulerConfig;

    public TestCluster(Path baseDir) {
        this.baseDir = baseDir;
    }

    /** Override the PD scheduler config used when PD nodes boot. Chainable. */
    public TestCluster pdScheduler(PdConfig.SchedulerConfig scheduler) {
        this.schedulerConfig = scheduler;
        return this;
    }

    /** Boot {@code pdCount} PD node(s) then {@code kvCount} KV store(s). */
    public TestCluster start(int pdCount, int kvCount) throws Exception {
        startPdCluster(pdCount);
        startInitialStores(kvCount);
        return this;
    }

    /**
     * Boot the cluster and wait until the bootstrap region is replicated to
     * {@code kvCount} peers with an elected leader — the "N-replica raft group"
     * shape the legacy harness produced out of the box.
     */
    public TestCluster startReplicated(int pdCount, int kvCount) throws Exception {
        start(pdCount, kvCount);
        if (kvCount > 1) {
            awaitRegionReplicas(BOOTSTRAP_REGION_ID, kvCount);
            // PD metadata reaching N replicas doesn't guarantee every follower
            // store has spawned its local RegionPeer yet (peers materialize
            // on-demand when raft traffic arrives). Wait for them so white-box
            // topology queries (followerStoresFor / realPeer) see a full group.
            awaitResidentPeers(BOOTSTRAP_REGION_ID, kvCount);
        }
        waitForRegionLeader(BOOTSTRAP_REGION_ID);
        return this;
    }

    // =====================================================================
    // PD lifecycle
    // =====================================================================

    private void startPdCluster(int pdCount) throws Exception {
        if (pdCount < 1) throw new IllegalArgumentException("pdCount must be >= 1");
        pdRaftMode = pdCount > 1;

        int[] clientPorts = new int[pdCount];
        int[] raftPorts = new int[pdCount];
        for (int i = 0; i < pdCount; i++) {
            clientPorts[i] = freePort();
            raftPorts[i] = freePort();
        }

        List<PeerAddress> peers = new ArrayList<>();
        if (pdRaftMode) {
            for (int i = 0; i < pdCount; i++) {
                peers.add(new PeerAddress(i + 1, "127.0.0.1:" + raftPorts[i], "127.0.0.1:" + clientPorts[i]));
            }
        }

        for (int i = 0; i < pdCount; i++) {
            var b = PdConfig.builder()
                    .nodeId(i + 1)
                    .clusterId(1)
                    .clientAddress("127.0.0.1:" + clientPorts[i])
                    .raftAddress("127.0.0.1:" + raftPorts[i])
                    .dataDir(Files.createDirectories(baseDir.resolve("pd-" + (i + 1))))
                    .peers(pdRaftMode ? peers : List.of());
            if (schedulerConfig != null) b.scheduler(schedulerConfig);
            var cfg = b.build();
            releasePort(clientPorts[i]);
            releasePort(raftPorts[i]);
            var srv = new PdServer(cfg);
            srv.start();
            pdNodes.add(new PdNode(i + 1, clientPorts[i], raftPorts[i], srv));
        }
        awaitPdReady();
        pdClient = new PdClient(pdEndpoints());
        log.info("PD cluster up: nodes={} raftMode={}", pdNodes.size(), pdRaftMode);
    }

    private void awaitPdReady() {
        Awaitility.await("pd-leader")
                .atMost(Duration.ofSeconds(30))
                .pollInterval(Duration.ofMillis(200))
                .until(() -> pdNodes.stream().anyMatch(
                        n -> n.server.raftNode() != null && n.server.raftNode().isLeader()));
    }

    /** Add a PD node to a running raft PD group. Requires {@code pdCount > 1}. */
    public PdNode addPd() throws Exception {
        if (!pdRaftMode) {
            throw new IllegalStateException("addPd requires a raft PD group (start with >= 2 PD nodes)");
        }
        long nodeId = pdNodes.stream().mapToLong(n -> n.nodeId).max().orElse(0) + 1;
        int clientPort = freePort();
        int raftPort = freePort();

        // 1) Ask the current leader to admit the member (raft ConfChange + metadata).
        pdClient.switchLeader();
        pdClient.blockingStub().addMember(Pdpb.AddMemberRequest.newBuilder()
                .setMember(Pdpb.Member.newBuilder()
                        .setMemberId(nodeId)
                        .setName("pd-" + nodeId)
                        .addClientUrls("127.0.0.1:" + clientPort)
                        .addPeerUrls("127.0.0.1:" + raftPort))
                .setRaftUrl("127.0.0.1:" + raftPort)
                .build());

        // 2) Boot the joining node with joinMode=true; it syncs via raft snapshot.
        List<PeerAddress> peers = new ArrayList<>();
        for (var n : pdNodes) peers.add(new PeerAddress(n.nodeId, n.raftAddress(), n.clientAddress()));
        peers.add(new PeerAddress(nodeId, "127.0.0.1:" + raftPort, "127.0.0.1:" + clientPort));

        var cfg = PdConfig.builder()
                .nodeId(nodeId)
                .clusterId(1)
                .clientAddress("127.0.0.1:" + clientPort)
                .raftAddress("127.0.0.1:" + raftPort)
                .dataDir(Files.createDirectories(baseDir.resolve("pd-" + nodeId)))
                .peers(peers)
                .joinMode(true)
                .build();
        releasePort(clientPort);
        releasePort(raftPort);
        var srv = new PdServer(cfg);
        srv.start();
        var node = new PdNode(nodeId, clientPort, raftPort, srv);
        pdNodes.add(node);
        awaitPdMembers(pdNodes.size());
        log.info("added PD node {} (client=127.0.0.1:{})", nodeId, clientPort);
        return node;
    }

    /** Remove a PD node from a running raft PD group. Requires {@code pdCount > 1}. */
    public void removePd(long nodeId) throws Exception {
        if (!pdRaftMode) {
            throw new IllegalStateException("removePd requires a raft PD group");
        }
        var node = pdNodes.stream().filter(n -> n.nodeId == nodeId).findFirst()
                .orElseThrow(() -> new IllegalArgumentException("no PD node " + nodeId));
        pdClient.switchLeader();
        pdClient.blockingStub().removeMember(Pdpb.RemoveMemberRequest.newBuilder()
                .setMemberId(nodeId).build());
        node.server.stop();
        pdNodes.remove(node);
        pdClient.switchLeader();
        awaitPdMembers(pdNodes.size());
        log.info("removed PD node {}", nodeId);
    }

    /** Stop a PD node without removing it from the raft membership (crash-like). */
    public void stopPd(long nodeId) {
        var node = pdNodes.stream().filter(n -> n.nodeId == nodeId).findFirst()
                .orElseThrow(() -> new IllegalArgumentException("no PD node " + nodeId));
        node.server.stop();
        pdNodes.remove(node);
        if (pdClient != null) pdClient.switchLeader();
        log.info("stopped PD node {}", nodeId);
    }

    /** The PD node that currently holds raft leadership. */
    public PdNode pdLeader() {
        return pdNodes.stream()
                .filter(n -> n.server.raftNode() != null && n.server.raftNode().isLeader())
                .findFirst()
                .orElseThrow(() -> new IllegalStateException("no PD leader"));
    }

    public PdNode pd(long nodeId) {
        return pdNodes.stream().filter(n -> n.nodeId == nodeId).findFirst()
                .orElseThrow(() -> new IllegalArgumentException("no PD node " + nodeId));
    }

    /** Returns a PD member id that is not the current leader. */
    public long anyPdFollowerId() {
        pdClient.switchLeader();
        long leaderId = pdClient.leaderMemberId();
        return pdNodes.stream().mapToLong(n -> n.nodeId)
                .filter(id -> id != leaderId)
                .findFirst()
                .orElseThrow(() -> new IllegalStateException("no PD follower found"));
    }

    // =====================================================================
    // KV store lifecycle
    // =====================================================================

    private void startInitialStores(int kvCount) throws Exception {
        if (kvCount <= 0) return;
        // The first store bootstraps the cluster; wait until PD is bootstrapped
        // before starting the rest so they take the join path deterministically.
        startStore(nextStoreId);
        awaitBootstrapped();
        for (int i = 1; i < kvCount; i++) {
            startStore(nextStoreId);
        }
        awaitStoreUpCount(kvCount);
        log.info("initial {} store(s) up", kvCount);
    }

    private StoreNode startStore(long storeId) throws Exception {
        // Fresh-port start: reserve two ephemeral ports, then bind. There is an
        // unavoidable release→bind window in which a parallel surefire fork or
        // a lingering socket can steal the port ("Address already in use"),
        // which shows up as flaky failures only when the full suite runs.
        // Retry with freshly-allocated ports on a bind conflict.
        Exception last = null;
        for (int attempt = 1; attempt <= BIND_RETRIES; attempt++) {
            int clientPort = freePort();
            int raftPort = freePort();
            try {
                return startStoreOn(storeId, clientPort, raftPort);
            } catch (Exception e) {
                releasePort(clientPort);
                releasePort(raftPort);
                if (!isBindConflict(e) || attempt == BIND_RETRIES) throw e;
                last = e;
                log.warn("store {} bind conflict (attempt {}/{}); retrying with fresh ports: {}",
                        storeId, attempt, BIND_RETRIES, rootCauseMessage(e));
            }
        }
        throw last;
    }

    private StoreNode startStoreOn(long storeId, int clientPort, int raftPort) throws Exception {
        var cfg = KvConfig.builder()
                .storeId(storeId)
                .pdEndpoints(pdEndpoints())
                .clientAddress("127.0.0.1:" + clientPort)
                .raftAddress("127.0.0.1:" + raftPort)
                .dataDir(baseDir.resolve("kv-" + storeId))
                .drainTimeoutMs(3_000)
                .build();
        releasePort(clientPort);
        releasePort(raftPort);
        var srv = new KvServer(cfg);
        try {
            srv.start();
        } catch (Exception e) {
            // Bind lost the release→bind race. Tear the half-started server
            // down so the caller can retry cleanly with fresh ports.
            try { srv.stop(); } catch (Exception ignore) { }
            throw e;
        }
        var node = new StoreNode(storeId, clientPort, raftPort, srv);
        storeNodes.add(node);
        stoppedStorePorts.remove(storeId);
        if (storeId >= nextStoreId) nextStoreId = storeId + 1;
        log.info("started store {} client=127.0.0.1:{} raft=127.0.0.1:{}",
                storeId, clientPort, raftPort);
        return node;
    }

    /** Dynamically add a new KV store; it joins the existing cluster via PD. */
    public StoreNode addStore() throws Exception {
        var node = startStore(nextStoreId);
        awaitStoreState(node.storeId, Metapb.StoreState.Up);
        return node;
    }

    /** Gracefully take a KV store offline (drain + leader transfer + stop). */
    public void removeStore(long storeId) {
        var node = requireStore(storeId);
        node.server.stop();      // drain() marks Offline in PD and transfers leadership
        storeNodes.remove(node);
        closeKvChannel(storeId);
        log.info("removed store {}", storeId);
    }

    /**
     * Stop a KV store, remembering its address so it can be brought back with
     * {@link #restartStore(long)}. The store's on-disk data is left intact.
     */
    public void killStore(long storeId) {
        var node = requireStore(storeId);
        stoppedStorePorts.put(storeId, new int[]{node.clientPort, node.raftPort});
        node.server.stop();
        storeNodes.remove(node);
        closeKvChannel(storeId);
        log.info("killed store {}", storeId);
    }

    /**
     * Restart a previously {@link #killStore(long) killed} store on the same
     * address and data directory. RocksDB re-opens the existing data; the store
     * recovers its regions and rejoins the cluster.
     */
    public StoreNode restartStore(long storeId) throws Exception {
        int[] ports = stoppedStorePorts.get(storeId);
        if (ports == null) {
            throw new IllegalStateException("store " + storeId + " was not killed via killStore");
        }
        var node = startStoreOn(storeId, ports[0], ports[1]);
        awaitStoreState(storeId, Metapb.StoreState.Up);
        return node;
    }

    public List<StoreNode> stores() { return storeNodes; }
    public List<PdNode> pds() { return pdNodes; }

    public StoreNode storeFor(long storeId) { return requireStore(storeId); }

    private StoreNode requireStore(long storeId) {
        return storeNodes.stream().filter(n -> n.storeId == storeId).findFirst()
                .orElseThrow(() -> new IllegalArgumentException("no store " + storeId));
    }

    // =====================================================================
    // Topology / real in-process handles
    // =====================================================================

    /** The real in-process store container for {@code storeId}. */
    public StoreImpl kvStore(long storeId) { return requireStore(storeId).store(); }

    /** The real in-process peer for {@code regionId} on {@code storeId}, if resident. */
    public RegionPeer realPeer(long storeId, long regionId) {
        return requireStore(storeId).store().peerForRegion(regionId).orElse(null);
    }

    /** All resident peers for {@code regionId} across every running store. */
    public List<RegionPeer> regionPeers(long regionId) {
        var out = new ArrayList<RegionPeer>();
        for (var n : storeNodes) {
            n.store().peerForRegion(regionId).ifPresent(out::add);
        }
        return out;
    }

    /** The store whose resident peer for {@code regionId} currently believes it is leader. */
    public StoreNode leaderStoreFor(long regionId) {
        return storeNodes.stream()
                .filter(n -> n.store().peerForRegion(regionId).map(RegionPeer::isLeader).orElse(false))
                .findFirst()
                .orElseThrow(() -> new IllegalStateException("no leader for region " + regionId));
    }

    /** Stores hosting a non-leader (follower) peer for {@code regionId}. */
    public List<StoreNode> followerStoresFor(long regionId) {
        var out = new ArrayList<StoreNode>();
        for (var n : storeNodes) {
            var p = n.store().peerForRegion(regionId).orElse(null);
            if (p != null && !p.isLeader()) out.add(n);
        }
        return out;
    }

    public void waitForRegionLeader(long regionId) {
        Awaitility.await("region-" + regionId + "-leader")
                .atMost(Duration.ofSeconds(30))
                .pollInterval(Duration.ofMillis(100))
                .until(() -> storeNodes.stream()
                        .anyMatch(n -> n.store().peerForRegion(regionId)
                                .map(RegionPeer::isLeader).orElse(false)));
    }

    /** Wait until a leader for {@code regionId} emerges on a store other than {@code oldStoreId}. */
    public StoreNode waitForNewLeaderOtherThan(long regionId, long oldStoreId) {
        Awaitility.await("region-" + regionId + "-new-leader")
                .atMost(Duration.ofSeconds(30))
                .pollInterval(Duration.ofMillis(100))
                .until(() -> storeNodes.stream()
                        .anyMatch(n -> n.storeId != oldStoreId
                                && n.store().peerForRegion(regionId)
                                .map(RegionPeer::isLeader).orElse(false)));
        return storeNodes.stream()
                .filter(n -> n.storeId != oldStoreId
                        && n.store().peerForRegion(regionId).map(RegionPeer::isLeader).orElse(false))
                .findFirst().orElseThrow();
    }

    // =====================================================================
    // Observation (PD gRPC)
    // =====================================================================

    public List<String> pdEndpoints() {
        var l = new ArrayList<String>();
        for (var n : pdNodes) l.add(n.clientAddress());
        return l;
    }

    /** Blocking PD stub connected to the current PD leader. */
    public PDGrpc.PDBlockingStub pdStub() {
        pdClient.switchLeader();
        return pdClient.blockingStub();
    }

    /** Raw TiKV blocking stub for a specific store's client port. */
    public TikvGrpc.TikvBlockingStub clientStub(long storeId) {
        var node = requireStore(storeId);
        var ch = kvChannels.computeIfAbsent(storeId, id ->
                NettyChannelBuilder.forAddress("127.0.0.1", node.clientPort).usePlaintext().build());
        return TikvGrpc.newBlockingStub(ch);
    }

    private void closeKvChannel(long storeId) {
        var ch = kvChannels.remove(storeId);
        if (ch != null) {
            try { ch.shutdownNow().awaitTermination(2, TimeUnit.SECONDS); }
            catch (Exception e) {
                Thread.currentThread().interrupt();
                log.warn("kv channel close failed: {}", e.getMessage());
            }
        }
    }

    private Pdpb.GetMembersResponse getMembers() {
        return pdClient.blockingStub().getMembers(Pdpb.GetMembersRequest.newBuilder().build());
    }

    /** Number of stores currently in {@code Up} state per PD. */
    public int storeUpCount() {
        var resp = pdClient.blockingStub().getAllStores(
                Pdpb.GetAllStoresRequest.newBuilder().build());
        int n = 0;
        for (var s : resp.getStoresList()) {
            if (s.getState() == Metapb.StoreState.Up) n++;
        }
        return n;
    }

    /** Replica (peer) count of a region per PD's metadata. */
    public int regionReplicaCount(long regionId) {
        var resp = pdClient.blockingStub().getRegionByID(
                Pdpb.GetRegionByIDRequest.newBuilder().setRegionId(regionId).build());
        return resp.hasRegion() ? resp.getRegion().getPeersCount() : 0;
    }

    public void awaitBootstrapped() {
        Awaitility.await("bootstrapped")
                .atMost(Duration.ofSeconds(30))
                .pollInterval(Duration.ofMillis(200))
                .until(() -> {
                    try {
                        return pdClient.blockingStub().isBootstrapped(
                                Pdpb.IsBootstrappedRequest.newBuilder().build()).getBootstrapped();
                    } catch (Exception e) {
                        pdClient.switchLeader();
                        return false;
                    }
                });
    }

    public void awaitStoreUpCount(int count) {
        Awaitility.await("store-up-count=" + count)
                .atMost(Duration.ofSeconds(60))
                .pollInterval(Duration.ofMillis(500))
                .until(() -> {
                    try { return storeUpCount() == count; }
                    catch (Exception e) { pdClient.switchLeader(); return false; }
                });
    }

    public void awaitStoreState(long storeId, Metapb.StoreState state) {
        Awaitility.await("store-" + storeId + "-state=" + state)
                .atMost(Duration.ofSeconds(60))
                .pollInterval(Duration.ofMillis(500))
                .until(() -> {
                    try {
                        var resp = pdClient.blockingStub().getStore(
                                Pdpb.GetStoreRequest.newBuilder().setStoreId(storeId).build());
                        return resp.hasStore() && resp.getStore().getState() == state;
                    } catch (Exception e) {
                        pdClient.switchLeader();
                        return false;
                    }
                });
    }

    public void awaitRegionReplicas(long regionId, int count) {
        Awaitility.await("region-" + regionId + "-replicas>=" + count)
                .atMost(Duration.ofSeconds(90))
                .pollInterval(Duration.ofSeconds(1))
                .until(() -> {
                    try {
                        int c = regionReplicaCount(regionId);
                        log.info("await replicas region={} count={} (target {})", regionId, c, count);
                        return c >= count;
                    } catch (Exception e) {
                        pdClient.switchLeader();
                        return false;
                    }
                });
    }

    /**
     * Wait until at least {@code count} stores have spawned a resident
     * {@link RegionPeer} for {@code regionId}. PD metadata reaching N replicas
     * only means the peers are scheduled; followers materialize their local
     * peer on-demand once raft traffic arrives, so white-box topology queries
     * ({@link #followerStoresFor}/{@link #realPeer}) need this stronger wait.
     */
    public void awaitResidentPeers(long regionId, int count) {
        Awaitility.await("region-" + regionId + "-resident-peers>=" + count)
                .atMost(Duration.ofSeconds(60))
                .pollInterval(Duration.ofMillis(200))
                .until(() -> regionPeers(regionId).size() >= count);
    }

    public void awaitPdMembers(int count) {
        Awaitility.await("pd-members=" + count)
                .atMost(Duration.ofSeconds(30))
                .pollInterval(Duration.ofMillis(300))
                .until(() -> {
                    try { return getMembers().getMembersCount() == count; }
                    catch (Exception e) { pdClient.switchLeader(); return false; }
                });
    }

    // =====================================================================
    // Clients (black-box SDK)
    // =====================================================================

    public TxnClient newTxnClient() {
        var c = TxnClient.create(ClientConfig.builder().pdEndpoints(pdEndpoints()).build());
        clients.add(c);
        return c;
    }

    public RawKvClient newRawKvClient() {
        var x = XKvClient.create(ClientConfig.builder().pdEndpoints(pdEndpoints()).build());
        clients.add(x);
        return x.raw();
    }

    /** Register a client for automatic cleanup on {@link #close()}. */
    public <T extends AutoCloseable> T register(T client) {
        clients.add(client);
        return client;
    }

    // =====================================================================
    // Teardown
    // =====================================================================

    @Override
    public void close() {
        for (var c : clients) {
            try { c.close(); } catch (Throwable e) { log.warn("client close failed: {}", e.getMessage()); }
        }
        clients.clear();
        for (var storeId : new ArrayList<>(kvChannels.keySet())) {
            closeKvChannel(storeId);
        }
        if (pdClient != null) {
            try { pdClient.close(); } catch (Throwable e) { log.warn("pdClient close failed: {}", e.getMessage()); }
        }
        for (var s : storeNodes) {
            try { s.server.stop(); } catch (Throwable e) { log.warn("store stop failed: {}", e.getMessage()); }
        }
        storeNodes.clear();
        for (var p : pdNodes) {
            try { p.server.stop(); } catch (Throwable e) { log.warn("pd stop failed: {}", e.getMessage()); }
        }
        pdNodes.clear();
        releaseAllPorts();
    }

    // =====================================================================
    // Port allocation (shared across all test harnesses in this process)
    // =====================================================================

    private static final List<ServerSocket> reservedSockets =
            java.util.Collections.synchronizedList(new ArrayList<>());

    /** Max attempts to (re)allocate fresh ports when a bind loses the race. */
    private static final int BIND_RETRIES = 5;

    /**
     * True if {@code t}'s cause chain indicates a port-bind conflict
     * (the release→bind race lost to a parallel fork / lingering socket).
     */
    public static boolean isBindConflict(Throwable t) {
        for (Throwable c = t; c != null; c = c.getCause()) {
            if (c instanceof java.net.BindException) return true;
            String m = c.getMessage();
            if (m != null && m.toLowerCase(java.util.Locale.ROOT)
                    .contains("address already in use")) {
                return true;
            }
        }
        return false;
    }

    private static String rootCauseMessage(Throwable t) {
        Throwable c = t;
        while (c.getCause() != null) c = c.getCause();
        return c.getClass().getSimpleName() + ": " + c.getMessage();
    }

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
                try { s.close(); } catch (Exception e) {
                    log.warn("socket close failed for port {}: {}", port, e.getMessage());
                }
                return true;
            }
            return false;
        });
    }

    public static void releaseAllPorts() {
        synchronized (reservedSockets) {
            for (var s : reservedSockets) {
                try { s.close(); } catch (Exception e) {
                    log.warn("socket close failed: {}", e.getMessage());
                }
            }
            reservedSockets.clear();
        }
    }

    // =====================================================================
    // Small helpers used by tests
    // =====================================================================

    public static byte[] keyOf(String s) { return s.getBytes(); }
    public static ByteString bs(String s) { return ByteString.copyFromUtf8(s); }
}
