package io.github.xinfra.lab.xkv.test;

import io.github.xinfra.lab.xkv.client.TxnClient;
import io.github.xinfra.lab.xkv.client.XKvClient;
import io.github.xinfra.lab.xkv.client.config.ClientConfig;
import io.github.xinfra.lab.xkv.client.pd.PdClient;
import io.github.xinfra.lab.xkv.client.raw.RawKvClient;
import io.github.xinfra.lab.xkv.kv.config.KvConfig;
import io.github.xinfra.lab.xkv.kv.server.KvServer;
import io.github.xinfra.lab.xkv.pd.config.PdConfig;
import io.github.xinfra.lab.xkv.pd.config.PdConfig.PeerAddress;
import io.github.xinfra.lab.xkv.pd.server.PdServer;
import io.github.xinfra.lab.xkv.proto.Metapb;
import io.github.xinfra.lab.xkv.proto.Pdpb;
import org.awaitility.Awaitility;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * Black-box cluster harness for end-to-end tests.
 *
 * <p>Unlike the legacy {@link ClusterHarness} — which hand-wires
 * {@code RegionPeerImpl}, transports, and heartbeaters in-process — this
 * harness treats PD, KV, and the client SDK as opaque units. It launches the
 * <em>production</em> {@link PdServer} and {@link KvServer} entrypoints
 * (in-process, but through their real {@code start()}/{@code stop()} lifecycle
 * and real bootstrap / join / heartbeat / on-demand-spawn paths) and observes
 * the cluster only through the client SDK and PD's public gRPC API.
 *
 * <p>Capabilities:
 * <ul>
 *   <li>Controllable PD node count (single in-memory PD, or a multi-node
 *       raft PD group when {@code pdCount > 1}).</li>
 *   <li>Controllable initial KV store count.</li>
 *   <li>Dynamic {@link #addStore()} / {@link #removeStore(long)}.</li>
 *   <li>Dynamic {@link #addPd()} / {@link #removePd(long)} (raft PD only).</li>
 * </ul>
 */
public final class BlackBoxCluster implements AutoCloseable {
    private static final Logger log = LoggerFactory.getLogger(BlackBoxCluster.class);

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
    }

    private final Path baseDir;
    private final List<PdNode> pdNodes = new CopyOnWriteArrayList<>();
    private final List<StoreNode> storeNodes = new CopyOnWriteArrayList<>();
    private final List<AutoCloseable> clients = new CopyOnWriteArrayList<>();

    private PdClient pdClient;      // observation + membership ops (connects to leader)
    private long nextStoreId = 1;
    private boolean pdRaftMode;

    public BlackBoxCluster(Path baseDir) {
        this.baseDir = baseDir;
    }

    /** Boot {@code pdCount} PD node(s) then {@code kvCount} KV store(s). */
    public BlackBoxCluster start(int pdCount, int kvCount) throws Exception {
        startPdCluster(pdCount);
        startInitialStores(kvCount);
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
            clientPorts[i] = ClusterHarness.freePort();
            raftPorts[i] = ClusterHarness.freePort();
        }

        List<PeerAddress> peers = new ArrayList<>();
        if (pdRaftMode) {
            for (int i = 0; i < pdCount; i++) {
                peers.add(new PeerAddress(i + 1, "127.0.0.1:" + raftPorts[i], "127.0.0.1:" + clientPorts[i]));
            }
        }

        for (int i = 0; i < pdCount; i++) {
            var cfg = PdConfig.builder()
                    .nodeId(i + 1)
                    .clusterId(1)
                    .clientAddress("127.0.0.1:" + clientPorts[i])
                    .raftAddress("127.0.0.1:" + raftPorts[i])
                    .dataDir(Files.createDirectories(baseDir.resolve("pd-" + (i + 1))))
                    .peers(pdRaftMode ? peers : List.of())
                    .build();
            ClusterHarness.releasePort(clientPorts[i]);
            ClusterHarness.releasePort(raftPorts[i]);
            var srv = new PdServer(cfg);
            srv.start();
            pdNodes.add(new PdNode(i + 1, clientPorts[i], raftPorts[i], srv));
        }
        awaitPdReady();
        pdClient = new PdClient(pdEndpoints());
        log.info("PD cluster up: nodes={} raftMode={}", pdNodes.size(), pdRaftMode);
    }

    private void awaitPdReady() {
        if (!pdRaftMode) return;    // single in-memory PD is ready after start()
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
        int clientPort = ClusterHarness.freePort();
        int raftPort = ClusterHarness.freePort();

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
        ClusterHarness.releasePort(clientPort);
        ClusterHarness.releasePort(raftPort);
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
        int clientPort = ClusterHarness.freePort();
        int raftPort = ClusterHarness.freePort();
        var cfg = KvConfig.builder()
                .storeId(storeId)
                .pdEndpoints(pdEndpoints())
                .clientAddress("127.0.0.1:" + clientPort)
                .raftAddress("127.0.0.1:" + raftPort)
                .dataDir(baseDir.resolve("kv-" + storeId))
                .drainTimeoutMs(3_000)
                .build();
        ClusterHarness.releasePort(clientPort);
        ClusterHarness.releasePort(raftPort);
        var srv = new KvServer(cfg);
        srv.start();
        var node = new StoreNode(storeId, clientPort, raftPort, srv);
        storeNodes.add(node);
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
        var node = storeNodes.stream().filter(n -> n.storeId == storeId).findFirst()
                .orElseThrow(() -> new IllegalArgumentException("no store " + storeId));
        node.server.stop();      // drain() marks Offline in PD and transfers leadership
        storeNodes.remove(node);
        log.info("removed store {}", storeId);
    }

    public List<StoreNode> stores() { return storeNodes; }
    public List<PdNode> pds() { return pdNodes; }

    // =====================================================================
    // Observation (PD gRPC only)
    // =====================================================================

    public List<String> pdEndpoints() {
        var l = new ArrayList<String>();
        for (var n : pdNodes) l.add(n.clientAddress());
        return l;
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

    // =====================================================================
    // Teardown
    // =====================================================================

    @Override
    public void close() {
        for (var c : clients) {
            try { c.close(); } catch (Throwable e) { log.warn("client close failed: {}", e.getMessage()); }
        }
        clients.clear();
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
        ClusterHarness.releaseAllPorts();
    }
}
