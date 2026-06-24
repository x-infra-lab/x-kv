package io.github.xinfra.lab.xkv.pd.server;

import io.github.xinfra.lab.xkv.pd.state.DeadlockDetector;
import io.github.xinfra.lab.xkv.pd.state.InMemoryPdStateMachine;
import io.github.xinfra.lab.xkv.pd.state.PdStateMachine;
import io.github.xinfra.lab.xkv.pd.state.SafePointService;
import io.github.xinfra.lab.xkv.pd.state.StoreStatsCache;
import io.github.xinfra.lab.xkv.pd.state.Tso;
import io.github.xinfra.lab.xkv.proto.Kvrpcpb;
import io.github.xinfra.lab.xkv.proto.PDGrpc;
import io.github.xinfra.lab.xkv.proto.Pdpb.AllocIDRequest;
import io.github.xinfra.lab.xkv.proto.Pdpb.AllocIDResponse;
import io.github.xinfra.lab.xkv.proto.Pdpb.AskBatchSplitRequest;
import io.github.xinfra.lab.xkv.proto.Pdpb.AskBatchSplitResponse;
import io.github.xinfra.lab.xkv.proto.Pdpb.AskSplitRequest;
import io.github.xinfra.lab.xkv.proto.Pdpb.AskSplitResponse;
import io.github.xinfra.lab.xkv.proto.Pdpb.BootstrapRequest;
import io.github.xinfra.lab.xkv.proto.Pdpb.BootstrapResponse;
import io.github.xinfra.lab.xkv.proto.Pdpb.CleanupWaitForRequest;
import io.github.xinfra.lab.xkv.proto.Pdpb.CleanupWaitForResponse;
import io.github.xinfra.lab.xkv.proto.Pdpb.DetectDeadlockRequest;
import io.github.xinfra.lab.xkv.proto.Pdpb.DetectDeadlockResponse;
import io.github.xinfra.lab.xkv.proto.Pdpb.GetAllServiceGCSafePointsRequest;
import io.github.xinfra.lab.xkv.proto.Pdpb.GetAllServiceGCSafePointsResponse;
import io.github.xinfra.lab.xkv.proto.Pdpb.GetAllStoresRequest;
import io.github.xinfra.lab.xkv.proto.Pdpb.GetAllStoresResponse;
import io.github.xinfra.lab.xkv.proto.Pdpb.GetClusterInfoRequest;
import io.github.xinfra.lab.xkv.proto.Pdpb.GetClusterInfoResponse;
import io.github.xinfra.lab.xkv.proto.Pdpb.GetGCSafePointRequest;
import io.github.xinfra.lab.xkv.proto.Pdpb.GetGCSafePointResponse;
import io.github.xinfra.lab.xkv.proto.Pdpb.GetMembersRequest;
import io.github.xinfra.lab.xkv.proto.Pdpb.GetMembersResponse;
import io.github.xinfra.lab.xkv.proto.Pdpb.GetOperatorRequest;
import io.github.xinfra.lab.xkv.proto.Pdpb.GetOperatorResponse;
import io.github.xinfra.lab.xkv.proto.Pdpb.GetRegionByIDRequest;
import io.github.xinfra.lab.xkv.proto.Pdpb.GetRegionRequest;
import io.github.xinfra.lab.xkv.proto.Pdpb.GetRegionResponse;
import io.github.xinfra.lab.xkv.proto.Pdpb.GetStoreRequest;
import io.github.xinfra.lab.xkv.proto.Pdpb.GetStoreResponse;
import io.github.xinfra.lab.xkv.proto.Pdpb.IsBootstrappedRequest;
import io.github.xinfra.lab.xkv.proto.Pdpb.IsBootstrappedResponse;
import io.github.xinfra.lab.xkv.proto.Pdpb.Member;
import io.github.xinfra.lab.xkv.proto.Pdpb.OperatorStatus;
import io.github.xinfra.lab.xkv.proto.Pdpb.PutStoreRequest;
import io.github.xinfra.lab.xkv.proto.Pdpb.PutStoreResponse;
import io.github.xinfra.lab.xkv.proto.Pdpb.RegionHeartbeatRequest;
import io.github.xinfra.lab.xkv.proto.Pdpb.RegionHeartbeatResponse;
import io.github.xinfra.lab.xkv.proto.Pdpb.ReportBatchSplitRequest;
import io.github.xinfra.lab.xkv.proto.Pdpb.ReportBatchSplitResponse;
import io.github.xinfra.lab.xkv.proto.Pdpb.ReportSplitRequest;
import io.github.xinfra.lab.xkv.proto.Pdpb.ReportSplitResponse;
import io.github.xinfra.lab.xkv.proto.Pdpb.ResponseHeader;
import io.github.xinfra.lab.xkv.proto.Pdpb.ScanRegionsRequest;
import io.github.xinfra.lab.xkv.proto.Pdpb.ScanRegionsResponse;
import io.github.xinfra.lab.xkv.proto.Pdpb.ScatterRegionRequest;
import io.github.xinfra.lab.xkv.proto.Pdpb.ScatterRegionResponse;
import io.github.xinfra.lab.xkv.proto.Pdpb.ServiceSafePoint;
import io.github.xinfra.lab.xkv.proto.Pdpb.SplitID;
import io.github.xinfra.lab.xkv.proto.Pdpb.SplitRegion;
import io.github.xinfra.lab.xkv.proto.Pdpb.SplitRegionsRequest;
import io.github.xinfra.lab.xkv.proto.Pdpb.SplitRegionsResponse;
import io.github.xinfra.lab.xkv.proto.Pdpb.StoreHeartbeatRequest;
import io.github.xinfra.lab.xkv.proto.Pdpb.StoreHeartbeatResponse;
import io.github.xinfra.lab.xkv.proto.Pdpb.Timestamp;
import io.github.xinfra.lab.xkv.proto.Pdpb.TsoRequest;
import io.github.xinfra.lab.xkv.proto.Pdpb.TsoResponse;
import io.github.xinfra.lab.xkv.proto.Pdpb.UpdateGCSafePointRequest;
import io.github.xinfra.lab.xkv.proto.Pdpb.UpdateGCSafePointResponse;
import io.github.xinfra.lab.xkv.proto.Pdpb.UpdateServiceGCSafePointRequest;
import io.github.xinfra.lab.xkv.proto.Pdpb.UpdateServiceGCSafePointResponse;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicLong;

/**
 * gRPC frontend for the PD service.
 *
 * <p>When a {@link io.github.xinfra.lab.xkv.pd.raft.PdRaftNode} is wired
 * (multi-PD mode), mutating RPCs (bootstrap, putStore, allocID, etc.) are
 * proposed through raft and applied deterministically on every PD node.
 * Read-only RPCs (getRegion, getStore, scanRegions) go to the local state.
 *
 * <p>When no raft node is wired (single-PD mode), mutations go directly
 * to the state machine for backward compatibility with tests and demos.
 */
public final class PdServiceImpl extends PDGrpc.PDImplBase {
    private static final Logger log = LoggerFactory.getLogger(PdServiceImpl.class);

    private static final long RAFT_PROPOSE_TIMEOUT_MS = 5_000;

    private final PdStateMachine state;
    private final Tso tso;
    private final SafePointService safePoint;
    private final io.github.xinfra.lab.xkv.pd.state.OperatorQueue operators;
    private final DeadlockDetector deadlock;
    private final long clusterId;
    private final long nodeId;
    private final String clientAddress;
    private final java.util.List<MemberInfo> members;
    private final StoreStatsCache storeStatsCache;
    private final io.github.xinfra.lab.xkv.pd.state.placement.PlacementRuleManager placementRuleManager;
    /** Persisted cluster GC safe-point separate from per-service. */
    private final AtomicLong gcSafePoint = new AtomicLong(0);

    /**
     * Optional raft node for multi-PD replication. When non-null, all
     * mutating RPCs are proposed through raft; when null, mutations go
     * directly to the state machine (single-node mode).
     */
    private volatile io.github.xinfra.lab.xkv.pd.raft.PdRaftNode raftNode;

    private volatile io.github.xinfra.lab.xkv.pd.state.OperatorController operatorController;

    public record MemberInfo(long id, String name, String clientAddress) {}

    public PdServiceImpl(PdStateMachine state,
                         Tso tso,
                         SafePointService safePoint,
                         long clusterId,
                         long nodeId) {
        this(state, tso, safePoint, new io.github.xinfra.lab.xkv.pd.state.OperatorQueue(),
                new DeadlockDetector(), clusterId, nodeId, "", java.util.List.of(),
                new StoreStatsCache());
    }

    public PdServiceImpl(PdStateMachine state,
                         Tso tso,
                         SafePointService safePoint,
                         io.github.xinfra.lab.xkv.pd.state.OperatorQueue operators,
                         long clusterId,
                         long nodeId) {
        this(state, tso, safePoint, operators, new DeadlockDetector(), clusterId, nodeId,
                "", java.util.List.of(), new StoreStatsCache());
    }

    public PdServiceImpl(PdStateMachine state,
                         Tso tso,
                         SafePointService safePoint,
                         io.github.xinfra.lab.xkv.pd.state.OperatorQueue operators,
                         DeadlockDetector deadlock,
                         long clusterId,
                         long nodeId) {
        this(state, tso, safePoint, operators, deadlock, clusterId, nodeId,
                "", java.util.List.of(), new StoreStatsCache());
    }

    public PdServiceImpl(PdStateMachine state,
                         Tso tso,
                         SafePointService safePoint,
                         io.github.xinfra.lab.xkv.pd.state.OperatorQueue operators,
                         DeadlockDetector deadlock,
                         long clusterId,
                         long nodeId,
                         String clientAddress,
                         java.util.List<MemberInfo> members) {
        this(state, tso, safePoint, operators, deadlock, clusterId, nodeId, clientAddress,
                members, new StoreStatsCache());
    }

    public PdServiceImpl(PdStateMachine state,
                         Tso tso,
                         SafePointService safePoint,
                         io.github.xinfra.lab.xkv.pd.state.OperatorQueue operators,
                         DeadlockDetector deadlock,
                         long clusterId,
                         long nodeId,
                         String clientAddress,
                         java.util.List<MemberInfo> members,
                         StoreStatsCache storeStatsCache) {
        this.state = state;
        this.tso = tso;
        this.safePoint = safePoint;
        this.operators = operators;
        this.deadlock = deadlock;
        this.clusterId = clusterId;
        this.nodeId = nodeId;
        this.clientAddress = clientAddress;
        this.members = java.util.List.copyOf(members);
        this.storeStatsCache = storeStatsCache;
        this.placementRuleManager = null;
    }

    public PdServiceImpl(PdStateMachine state,
                         Tso tso,
                         SafePointService safePoint,
                         io.github.xinfra.lab.xkv.pd.state.OperatorQueue operators,
                         DeadlockDetector deadlock,
                         long clusterId,
                         long nodeId,
                         String clientAddress,
                         java.util.List<MemberInfo> members,
                         StoreStatsCache storeStatsCache,
                         io.github.xinfra.lab.xkv.pd.state.placement.PlacementRuleManager placementRuleManager) {
        this.state = state;
        this.tso = tso;
        this.safePoint = safePoint;
        this.operators = operators;
        this.deadlock = deadlock;
        this.clusterId = clusterId;
        this.nodeId = nodeId;
        this.clientAddress = clientAddress;
        this.members = java.util.List.copyOf(members);
        this.storeStatsCache = storeStatsCache;
        this.placementRuleManager = placementRuleManager;
    }

    /** Phase-0 stub constructor still used by {@link PdServer} until full wiring. */
    public PdServiceImpl() {
        this(new InMemoryPdStateMachine(), null, null, 1, 1);
    }

    public StoreStatsCache storeStatsCache() { return storeStatsCache; }

    /** Wire the raft node for multi-PD replication. */
    public void setRaftNode(io.github.xinfra.lab.xkv.pd.raft.PdRaftNode node) {
        this.raftNode = node;
    }

    public void setOperatorController(io.github.xinfra.lab.xkv.pd.state.OperatorController oc) {
        this.operatorController = oc;
    }

    /** Test / scheduler hook: enqueue operators visible to the next heartbeat. */
    public io.github.xinfra.lab.xkv.pd.state.OperatorQueue operators() { return operators; }

    /** Diagnostic / scheduler hook: the global deadlock-detector instance. */
    public DeadlockDetector deadlock() { return deadlock; }

    /**
     * Propose a PD command through raft (if wired) or apply directly.
     * Returns true on success; on failure, sends an error response and returns false.
     */
    private <T> boolean proposeOrApply(
            io.github.xinfra.lab.xkv.proto.PdInternalpb.PdCommand cmd,
            StreamObserver<T> obs,
            java.util.function.Supplier<T> onSuccess) {
        var rn = raftNode;
        if (rn == null) {
            // Single-PD mode: apply directly.
            state.applyCommand(cmd.toByteArray());
            obs.onNext(onSuccess.get());
            obs.onCompleted();
            return true;
        }
        if (!rn.isLeader()) {
            obs.onError(io.grpc.Status.UNAVAILABLE
                    .withDescription("not PD leader").asRuntimeException());
            return false;
        }
        try {
            rn.propose(cmd.toByteArray())
                    .get(RAFT_PROPOSE_TIMEOUT_MS, java.util.concurrent.TimeUnit.MILLISECONDS);
            obs.onNext(onSuccess.get());
            obs.onCompleted();
            return true;
        } catch (Exception e) {
            obs.onError(io.grpc.Status.INTERNAL
                    .withDescription("raft propose failed: " + e.getMessage())
                    .asRuntimeException());
            return false;
        }
    }

    private ResponseHeader header() {
        return ResponseHeader.newBuilder().setClusterId(clusterId).build();
    }

    private ResponseHeader errorHeader(io.github.xinfra.lab.xkv.proto.Pdpb.Error.ErrorType type, String message) {
        return ResponseHeader.newBuilder()
                .setClusterId(clusterId)
                .setError(io.github.xinfra.lab.xkv.proto.Pdpb.Error.newBuilder()
                        .setType(type).setMessage(message).build())
                .build();
    }

    // =====================================================================
    // Cluster bootstrap / membership
    // =====================================================================

    @Override
    public void getMembers(GetMembersRequest req, StreamObserver<GetMembersResponse> obs) {
        var b = GetMembersResponse.newBuilder().setHeader(header());
        if (members.isEmpty()) {
            var self = Member.newBuilder()
                    .setMemberId(nodeId)
                    .setName("pd-" + nodeId);
            if (!clientAddress.isEmpty()) self.addClientUrls(clientAddress);
            b.addMembers(self.build()).setLeader(self.build());
        } else {
            for (var m : members) {
                var mb = Member.newBuilder()
                        .setMemberId(m.id())
                        .setName(m.name());
                if (!m.clientAddress().isEmpty()) mb.addClientUrls(m.clientAddress());
                b.addMembers(mb.build());
            }
            var rn = raftNode;
            boolean rnIsLeader = rn != null && rn.isLeader();
            long leaderId = rnIsLeader ? nodeId : findLeaderMemberId();
            if (rn != null && !rnIsLeader && leaderId == nodeId) {
                leaderId = 0;
            }
            for (var m : members) {
                if (m.id() == leaderId) {
                    var lb = Member.newBuilder()
                            .setMemberId(m.id())
                            .setName(m.name());
                    if (!m.clientAddress().isEmpty()) lb.addClientUrls(m.clientAddress());
                    b.setLeader(lb.build());
                    break;
                }
            }
        }
        obs.onNext(b.build());
        obs.onCompleted();
    }

    private long findLeaderMemberId() {
        var rn = raftNode;
        if (rn != null) return rn.leaderNodeId();
        return nodeId;
    }

    @Override
    public void getClusterInfo(GetClusterInfoRequest req, StreamObserver<GetClusterInfoResponse> obs) {
        var c = state.cluster();
        var b = GetClusterInfoResponse.newBuilder().setHeader(header());
        if (c != null) b.setCluster(c);
        obs.onNext(b.build()); obs.onCompleted();
    }

    @Override
    public void bootstrap(BootstrapRequest req, StreamObserver<BootstrapResponse> obs) {
        if (!req.hasStore() || !req.hasRegion()) {
            obs.onError(Status.INVALID_ARGUMENT
                    .withDescription("bootstrap missing store or region")
                    .asRuntimeException());
            return;
        }
        if (state.isBootstrapped()) {
            obs.onNext(BootstrapResponse.newBuilder()
                    .setHeader(errorHeader(
                            io.github.xinfra.lab.xkv.proto.Pdpb.Error.ErrorType.ALREADY_BOOTSTRAPPED,
                            "cluster already bootstrapped"))
                    .build());
            obs.onCompleted();
            return;
        }
        var cmd = io.github.xinfra.lab.xkv.proto.PdInternalpb.PdCommand.newBuilder()
                .setType(io.github.xinfra.lab.xkv.proto.PdInternalpb.CommandType.CMD_BOOTSTRAP)
                .setBootstrap(io.github.xinfra.lab.xkv.proto.PdInternalpb.BootstrapPayload.newBuilder()
                        .setStore(req.getStore())
                        .setRegion(req.getRegion()))
                .build();
        proposeOrApply(cmd, obs,
                () -> BootstrapResponse.newBuilder().setHeader(header()).build());
    }

    @Override
    public void isBootstrapped(IsBootstrappedRequest req, StreamObserver<IsBootstrappedResponse> obs) {
        obs.onNext(IsBootstrappedResponse.newBuilder()
                .setHeader(header())
                .setBootstrapped(state.isBootstrapped())
                .build());
        obs.onCompleted();
    }

    // =====================================================================
    // ID allocator
    // =====================================================================

    @Override
    public void allocID(AllocIDRequest req, StreamObserver<AllocIDResponse> obs) {
        int count = req.getCount() <= 0 ? 1 : req.getCount();
        // allocId pre-allocates on the leader, then proposes so followers
        // advance their allocator past the consumed range.
        long first = state.allocId(count);
        var cmd = io.github.xinfra.lab.xkv.proto.PdInternalpb.PdCommand.newBuilder()
                .setType(io.github.xinfra.lab.xkv.proto.PdInternalpb.CommandType.CMD_ALLOC_ID)
                .setAllocId(io.github.xinfra.lab.xkv.proto.PdInternalpb.AllocIdPayload.newBuilder()
                        .setCount(count)
                        .setBaseId(first))
                .build();
        proposeOrApply(cmd, obs,
                () -> AllocIDResponse.newBuilder()
                        .setHeader(header())
                        .setId(first)
                        .setCount(count)
                        .build());
    }

    // =====================================================================
    // TSO (bidi stream)
    // =====================================================================

    @Override
    public StreamObserver<TsoRequest> getTimestamp(StreamObserver<TsoResponse> resp) {
        if (tso == null) {
            resp.onError(Status.UNAVAILABLE.withDescription("TSO not configured").asRuntimeException());
            return new io.grpc.stub.StreamObserver<>() {
                @Override public void onNext(TsoRequest value) {}
                @Override public void onError(Throwable t) {}
                @Override public void onCompleted() {}
            };
        }
        return new io.grpc.stub.StreamObserver<>() {
            @Override
            public void onNext(TsoRequest req) {
                int count = req.getCount() <= 0 ? 1 : req.getCount();
                try {
                    long firstTs = tso.alloc(count);
                    long physical = Tso.physicalPart(firstTs);
                    long logical = Tso.logicalPart(firstTs) + count - 1;
                    resp.onNext(TsoResponse.newBuilder()
                            .setHeader(header())
                            .setCount(count)
                            .setTimestamp(Timestamp.newBuilder()
                                    .setPhysical(physical)
                                    .setLogical(logical)
                                    .build())
                            .build());
                } catch (Throwable t) {
                    log.warn("TSO alloc failed", t);
                    resp.onNext(TsoResponse.newBuilder()
                            .setHeader(errorHeader(
                                    io.github.xinfra.lab.xkv.proto.Pdpb.Error.ErrorType.UNKNOWN,
                                    t.getMessage()))
                            .build());
                }
            }
            @Override public void onError(Throwable t) {
                log.debug("TSO client error: {}", t.getMessage());
            }
            @Override public void onCompleted() { resp.onCompleted(); }
        };
    }

    // =====================================================================
    // Stores
    // =====================================================================

    @Override
    public void putStore(PutStoreRequest req, StreamObserver<PutStoreResponse> obs) {
        var cmd = io.github.xinfra.lab.xkv.proto.PdInternalpb.PdCommand.newBuilder()
                .setType(io.github.xinfra.lab.xkv.proto.PdInternalpb.CommandType.CMD_PUT_STORE)
                .setStore(req.getStore())
                .build();
        proposeOrApply(cmd, obs,
                () -> PutStoreResponse.newBuilder().setHeader(header()).build());
    }

    @Override
    public void getStore(GetStoreRequest req, StreamObserver<GetStoreResponse> obs) {
        var b = GetStoreResponse.newBuilder().setHeader(header());
        state.getStore(req.getStoreId()).ifPresent(b::setStore);
        storeStatsCache.get(req.getStoreId()).ifPresent(b::setStats);
        obs.onNext(b.build());
        obs.onCompleted();
    }

    @Override
    public void getAllStores(GetAllStoresRequest req, StreamObserver<GetAllStoresResponse> obs) {
        var b = GetAllStoresResponse.newBuilder().setHeader(header());
        for (var s : state.allStores()) b.addStores(s);
        obs.onNext(b.build()); obs.onCompleted();
    }

    @Override
    public void storeHeartbeat(StoreHeartbeatRequest req, StreamObserver<StoreHeartbeatResponse> obs) {
        if (req.hasStats()) {
            storeStatsCache.update(req.getStats());
        }
        obs.onNext(StoreHeartbeatResponse.newBuilder().setHeader(header()).build());
        obs.onCompleted();
    }

    @Override
    public StreamObserver<RegionHeartbeatRequest> regionHeartbeat(StreamObserver<RegionHeartbeatResponse> obs) {
        return new io.grpc.stub.StreamObserver<>() {
            @Override
            public void onNext(RegionHeartbeatRequest hb) {
                if (hb.hasRegion() && shouldUpdateRegion(hb.getRegion())) {
                    var rn = raftNode;
                    if (rn != null && rn.isLeader()) {
                        var cmd = io.github.xinfra.lab.xkv.proto.PdInternalpb.PdCommand.newBuilder()
                                .setType(io.github.xinfra.lab.xkv.proto.PdInternalpb.CommandType.CMD_UPDATE_REGION)
                                .setRegion(hb.getRegion())
                                .build();
                        try {
                            rn.propose(cmd.toByteArray())
                                    .get(RAFT_PROPOSE_TIMEOUT_MS, java.util.concurrent.TimeUnit.MILLISECONDS);
                        } catch (Exception e) {
                            log.debug("heartbeat region update propose failed: {}", e.getMessage());
                        }
                    } else {
                        state.updateRegion(hb.getRegion());
                    }
                }
                // Record region stats from heartbeat.
                if (hb.hasRegion()) {
                    state.updateRegionStats(hb.getRegion().getId(),
                            hb.getApproximateSize(), hb.getApproximateKeys());
                }
                // Leader identity is volatile routing state — NOT replicated.
                if (hb.hasLeader() && hb.hasRegion()) {
                    state.updateLeader(hb.getRegion().getId(), hb.getLeader());
                }
                // Drive operator lifecycle via the controller — this evicts
                // expired operators, calls observe() for step tracking, and
                // decrements per-store in-flight counters on completion.
                var oc = operatorController;
                var opResp = oc != null
                        ? oc.dispatch(hb)
                        : java.util.Optional.<RegionHeartbeatResponse>empty();
                if (opResp.isPresent()) {
                    obs.onNext(opResp.get().toBuilder().setHeader(header()).build());
                } else {
                    obs.onNext(RegionHeartbeatResponse.newBuilder().setHeader(header()).build());
                }
            }
            @Override public void onError(Throwable t) {}
            @Override public void onCompleted() { obs.onCompleted(); }
        };
    }

    // =====================================================================
    // Region lookup
    // =====================================================================

    @Override
    public void getRegion(GetRegionRequest req, StreamObserver<GetRegionResponse> obs) {
        var b = GetRegionResponse.newBuilder().setHeader(header());
        state.getRegionByKey(req.getRegionKey().toByteArray()).ifPresent(r -> {
            b.setRegion(r);
            state.getLeader(r.getId()).ifPresentOrElse(
                    b::setLeader,
                    () -> { if (r.getPeersCount() > 0) b.setLeader(r.getPeers(0)); });
        });
        obs.onNext(b.build()); obs.onCompleted();
    }

    @Override
    public void getRegionByID(GetRegionByIDRequest req, StreamObserver<GetRegionResponse> obs) {
        var b = GetRegionResponse.newBuilder().setHeader(header());
        state.getRegion(req.getRegionId()).ifPresent(r -> {
            b.setRegion(r);
            state.getLeader(r.getId()).ifPresentOrElse(
                    b::setLeader,
                    () -> { if (r.getPeersCount() > 0) b.setLeader(r.getPeers(0)); });
        });
        obs.onNext(b.build()); obs.onCompleted();
    }

    @Override
    public void scanRegions(ScanRegionsRequest req, StreamObserver<ScanRegionsResponse> obs) {
        var b = ScanRegionsResponse.newBuilder().setHeader(header());
        int limit = req.getLimit() <= 0 ? 100 : req.getLimit();
        for (var r : state.scanRegions(req.getStartKey().toByteArray(),
                req.getEndKey().isEmpty() ? null : req.getEndKey().toByteArray(), limit)) {
            b.addRegions(r);
            if (r.getPeersCount() > 0) b.addLeaders(r.getPeers(0));
        }
        obs.onNext(b.build()); obs.onCompleted();
    }

    // =====================================================================
    // Split (Phase 4)
    // =====================================================================

    @Override
    public void askSplit(AskSplitRequest req, StreamObserver<AskSplitResponse> obs) {
        if (!req.hasRegion()) {
            obs.onError(io.grpc.Status.INVALID_ARGUMENT
                    .withDescription("region required").asRuntimeException());
            return;
        }
        int peerCount = req.getRegion().getPeersCount();
        long regionId = state.allocId(1);
        var b = AskSplitResponse.newBuilder()
                .setHeader(header())
                .setNewRegionId(regionId);
        for (int p = 0; p < peerCount; p++) {
            b.addNewPeerIds(state.allocId(1));
        }
        obs.onNext(b.build());
        obs.onCompleted();
    }
    @Override
    public void askBatchSplit(AskBatchSplitRequest req, StreamObserver<AskBatchSplitResponse> obs) {
        // Allocate (split_count) new region IDs and per region (peers_count)
        // new peer IDs. PD's AllocID is monotonic so children get IDs > parent.
        int splitCount = req.getSplitCount();
        if (splitCount <= 0) {
            obs.onError(io.grpc.Status.INVALID_ARGUMENT
                    .withDescription("split_count must be > 0").asRuntimeException());
            return;
        }
        int peerCount = req.getRegion().getPeersCount();
        var b = AskBatchSplitResponse.newBuilder().setHeader(header());
        for (int i = 0; i < splitCount; i++) {
            long regionId = state.allocId(1);
            var sb = SplitID.newBuilder().setNewRegionId(regionId);
            for (int p = 0; p < peerCount; p++) {
                sb.addNewPeerIds(state.allocId(1));
            }
            b.addIds(sb.build());
        }
        obs.onNext(b.build());
        obs.onCompleted();
    }
    @Override public void reportSplit(ReportSplitRequest r, StreamObserver<ReportSplitResponse> o) {
        o.onNext(ReportSplitResponse.newBuilder().setHeader(header()).build()); o.onCompleted();
    }
    @Override public void reportBatchSplit(ReportBatchSplitRequest r, StreamObserver<ReportBatchSplitResponse> o) {
        o.onNext(ReportBatchSplitResponse.newBuilder().setHeader(header()).build()); o.onCompleted();
    }
    @Override
    public void splitRegions(SplitRegionsRequest req, StreamObserver<SplitRegionsResponse> obs) {
        if (req.getSplitKeysCount() == 0) {
            obs.onNext(SplitRegionsResponse.newBuilder().setHeader(header()).build());
            obs.onCompleted();
            return;
        }
        var regionIds = new java.util.ArrayList<Long>();
        for (var keyBs : req.getSplitKeysList()) {
            byte[] key = keyBs.toByteArray();
            var regionOpt = state.getRegionByKey(key);
            if (regionOpt.isEmpty()) continue;
            var region = regionOpt.get();
            operators.scheduleSplit(region.getId(),
                    java.util.List.of(key), SplitRegion.Policy.USER_KEY);
            regionIds.add(region.getId());
        }
        obs.onNext(SplitRegionsResponse.newBuilder()
                .setHeader(header())
                .setFinishedPercentage(0)
                .addAllRegionsId(regionIds)
                .build());
        obs.onCompleted();
    }
    @Override
    public void scatterRegion(ScatterRegionRequest req, StreamObserver<ScatterRegionResponse> obs) {
        long regionId = req.getRegionId();
        if (regionId == 0 && req.hasRegion()) regionId = req.getRegion().getId();
        if (regionId == 0 || state.getRegion(regionId).isEmpty()) {
            obs.onNext(ScatterRegionResponse.newBuilder()
                    .setHeader(errorHeader(
                            io.github.xinfra.lab.xkv.proto.Pdpb.Error.ErrorType.REGION_NOT_FOUND,
                            "region not found"))
                    .build());
            obs.onCompleted();
            return;
        }
        obs.onNext(ScatterRegionResponse.newBuilder()
                .setHeader(header())
                .setFinishedPercentage(100)
                .build());
        obs.onCompleted();
    }

    // =====================================================================
    // GC safe-point
    // =====================================================================

    @Override
    public void getGCSafePoint(GetGCSafePointRequest req, StreamObserver<GetGCSafePointResponse> obs) {
        long sp = safePoint == null ? gcSafePoint.get() : safePoint.currentSafePoint();
        obs.onNext(GetGCSafePointResponse.newBuilder()
                .setHeader(header())
                .setSafePoint(sp)
                .build());
        obs.onCompleted();
    }

    @Override
    public void updateGCSafePoint(UpdateGCSafePointRequest req, StreamObserver<UpdateGCSafePointResponse> obs) {
        // Route through the SafePointService so the operator-driven floor,
        // service safe-points, and the time-based floor all converge through
        // one effective-safe-point computation. Falling back to the bare
        // AtomicLong when no SafePointService is configured matches single-
        // node demo deployments.
        long target = req.getSafePoint();
        long updated = (safePoint == null)
                ? gcSafePoint.updateAndGet(prev -> Math.max(prev, target))
                : safePoint.updateGcSafePoint(target);
        obs.onNext(UpdateGCSafePointResponse.newBuilder()
                .setHeader(header())
                .setNewSafePoint(updated)
                .build());
        obs.onCompleted();
    }

    @Override
    public void updateServiceGCSafePoint(UpdateServiceGCSafePointRequest req,
                                         StreamObserver<UpdateServiceGCSafePointResponse> obs) {
        if (safePoint == null) {
            obs.onError(Status.UNAVAILABLE.withDescription("SafePointService not configured").asRuntimeException());
            return;
        }
        var sid = req.getServiceId().toStringUtf8();
        long minSp = safePoint.updateServiceSafePoint(sid, req.getTtl(), req.getSafePoint());
        obs.onNext(UpdateServiceGCSafePointResponse.newBuilder()
                .setHeader(header())
                .setServiceId(sid)
                .setTtl(req.getTtl())
                .setMinSafePoint(minSp)
                .build());
        obs.onCompleted();
    }

    @Override
    public void getAllServiceGCSafePoints(GetAllServiceGCSafePointsRequest req,
                                          StreamObserver<GetAllServiceGCSafePointsResponse> obs) {
        var b = GetAllServiceGCSafePointsResponse.newBuilder().setHeader(header());
        if (safePoint != null) {
            for (var e : safePoint.listServiceSafePoints()) {
                b.addSafePoints(ServiceSafePoint.newBuilder()
                        .setServiceId(e.serviceId())
                        .setExpiredAt(e.expiresAtMs() / 1000L)
                        .setSafePoint(e.safePoint())
                        .build());
            }
        }
        obs.onNext(b.build()); obs.onCompleted();
    }

    @Override
    public void getOperator(GetOperatorRequest req, StreamObserver<GetOperatorResponse> obs) {
        // Phase 4 wires this when the operator framework lands.
        obs.onNext(GetOperatorResponse.newBuilder()
                .setHeader(header())
                .setRegionId(req.getRegionId())
                .setStatus(OperatorStatus.NOT_FOUND)
                .build());
        obs.onCompleted();
    }

    // =====================================================================
    // Deadlock detection
    // =====================================================================

    @Override
    public void detectDeadlock(DetectDeadlockRequest req,
                               StreamObserver<DetectDeadlockResponse> obs) {
        var entry = req.getEntry();
        if (entry.getTxn() == 0L || entry.getWaitForTxn() == 0L) {
            obs.onError(Status.INVALID_ARGUMENT
                    .withDescription("entry.txn and entry.wait_for_txn must be non-zero")
                    .asRuntimeException());
            return;
        }
        var edge = new DeadlockDetector.WaitForEdge(
                entry.getTxn(),
                entry.getWaitForTxn(),
                entry.getKey().toByteArray());
        var cycle = deadlock.addWaitFor(edge);
        var b = DetectDeadlockResponse.newBuilder().setHeader(header());
        if (!cycle.isEmpty()) {
            // Surface the chain in proto shape — txn / wait_for_txn / key.
            for (var e : cycle) {
                b.addWaitChain(Kvrpcpb.WaitForEntry.newBuilder()
                        .setTxn(e.waiterTxn())
                        .setWaitForTxn(e.holderTxn())
                        .setKey(com.google.protobuf.ByteString.copyFrom(e.key()))
                        .setKeyHash(hashKey(e.key()))
                        .build());
            }
            b.setDeadlockKeyHash(hashKey(cycle.get(0).key()));
        }
        obs.onNext(b.build());
        obs.onCompleted();
    }

    @Override
    public void cleanupWaitFor(CleanupWaitForRequest req,
                               StreamObserver<CleanupWaitForResponse> obs) {
        switch (req.getMode()) {
            case REMOVE_HOLDER -> deadlock.removeHolder(req.getTxn());
            case REMOVE_WAITER -> deadlock.removeWaiter(req.getTxn());
            default -> {
                obs.onError(Status.INVALID_ARGUMENT
                        .withDescription("unknown cleanup mode: " + req.getMode())
                        .asRuntimeException());
                return;
            }
        }
        obs.onNext(CleanupWaitForResponse.newBuilder().setHeader(header()).build());
        obs.onCompleted();
    }

    private boolean shouldUpdateRegion(io.github.xinfra.lab.xkv.proto.Metapb.Region incoming) {
        var existing = state.getRegion(incoming.getId());
        if (existing.isEmpty()) return true;
        var a = incoming.getRegionEpoch();
        var b = existing.get().getRegionEpoch();
        if (a.getConfVer() > b.getConfVer()) return true;
        if (a.getConfVer() < b.getConfVer()) return false;
        return a.getVersion() >= b.getVersion();
    }

    // ---- Placement rules ----

    @Override
    public void getPlacementRules(
            io.github.xinfra.lab.xkv.proto.Pdpb.GetPlacementRulesRequest req,
            StreamObserver<io.github.xinfra.lab.xkv.proto.Pdpb.GetPlacementRulesResponse> obs) {
        var b = io.github.xinfra.lab.xkv.proto.Pdpb.GetPlacementRulesResponse.newBuilder()
                .setHeader(header());
        if (placementRuleManager != null) {
            for (var rule : placementRuleManager.getRules()) {
                b.addRules(rule.toProto());
            }
        }
        obs.onNext(b.build());
        obs.onCompleted();
    }

    @Override
    public void setPlacementRule(
            io.github.xinfra.lab.xkv.proto.Pdpb.SetPlacementRuleRequest req,
            StreamObserver<io.github.xinfra.lab.xkv.proto.Pdpb.SetPlacementRuleResponse> obs) {
        if (placementRuleManager == null) {
            obs.onError(Status.UNIMPLEMENTED
                    .withDescription("placement rules not enabled")
                    .asRuntimeException());
            return;
        }
        var rule = io.github.xinfra.lab.xkv.pd.state.placement.PlacementRule.fromProto(req.getRule());
        placementRuleManager.setRule(rule);
        obs.onNext(io.github.xinfra.lab.xkv.proto.Pdpb.SetPlacementRuleResponse.newBuilder()
                .setHeader(header()).build());
        obs.onCompleted();
    }

    @Override
    public void deletePlacementRule(
            io.github.xinfra.lab.xkv.proto.Pdpb.DeletePlacementRuleRequest req,
            StreamObserver<io.github.xinfra.lab.xkv.proto.Pdpb.DeletePlacementRuleResponse> obs) {
        if (placementRuleManager == null) {
            obs.onError(Status.UNIMPLEMENTED
                    .withDescription("placement rules not enabled")
                    .asRuntimeException());
            return;
        }
        placementRuleManager.deleteRule(req.getGroupId(), req.getId());
        obs.onNext(io.github.xinfra.lab.xkv.proto.Pdpb.DeletePlacementRuleResponse.newBuilder()
                .setHeader(header()).build());
        obs.onCompleted();
    }

    // =====================================================================
    // Keyspace
    // =====================================================================

    @Override
    public void loadKeyspace(
            io.github.xinfra.lab.xkv.proto.Pdpb.LoadKeyspaceRequest req,
            StreamObserver<io.github.xinfra.lab.xkv.proto.Pdpb.LoadKeyspaceResponse> obs) {
        var ksm = state.keyspaceManager();
        if (ksm == null) {
            obs.onError(Status.UNIMPLEMENTED.withDescription("keyspace not enabled").asRuntimeException());
            return;
        }
        var meta = req.getName().isEmpty() ? null : ksm.loadByName(req.getName());
        var b = io.github.xinfra.lab.xkv.proto.Pdpb.LoadKeyspaceResponse.newBuilder().setHeader(header());
        if (meta != null) {
            b.setKeyspace(meta);
        }
        obs.onNext(b.build());
        obs.onCompleted();
    }

    @Override
    public void updateKeyspaceState(
            io.github.xinfra.lab.xkv.proto.Pdpb.UpdateKeyspaceStateRequest req,
            StreamObserver<io.github.xinfra.lab.xkv.proto.Pdpb.UpdateKeyspaceStateResponse> obs) {
        var ksm = state.keyspaceManager();
        if (ksm == null) {
            obs.onError(Status.UNIMPLEMENTED.withDescription("keyspace not enabled").asRuntimeException());
            return;
        }
        var updated = ksm.updateState(req.getId(), req.getState());
        var b = io.github.xinfra.lab.xkv.proto.Pdpb.UpdateKeyspaceStateResponse.newBuilder().setHeader(header());
        if (updated != null) {
            b.setKeyspace(updated);
            var cmd = io.github.xinfra.lab.xkv.proto.PdInternalpb.PdCommand.newBuilder()
                    .setType(io.github.xinfra.lab.xkv.proto.PdInternalpb.CommandType.CMD_SET_KEYSPACE)
                    .setKeyspace(io.github.xinfra.lab.xkv.proto.PdInternalpb.KeyspacePayload.newBuilder()
                            .setKeyspace(updated))
                    .build();
            proposeOrApply(cmd, obs, () -> b.build());
        } else {
            obs.onNext(b.build());
            obs.onCompleted();
        }
    }

    @Override
    public void listKeyspaces(
            io.github.xinfra.lab.xkv.proto.Pdpb.ListKeyspacesRequest req,
            StreamObserver<io.github.xinfra.lab.xkv.proto.Pdpb.ListKeyspacesResponse> obs) {
        var ksm = state.keyspaceManager();
        if (ksm == null) {
            obs.onError(Status.UNIMPLEMENTED.withDescription("keyspace not enabled").asRuntimeException());
            return;
        }
        io.github.xinfra.lab.xkv.proto.Pdpb.KeyspaceState filter =
                req.getStateFilterValue() == 0 ? null : req.getStateFilter();
        var list = ksm.listKeyspaces(filter);
        var b = io.github.xinfra.lab.xkv.proto.Pdpb.ListKeyspacesResponse.newBuilder().setHeader(header());
        for (var ks : list) b.addKeyspaces(ks);
        obs.onNext(b.build());
        obs.onCompleted();
    }

    // =====================================================================
    // Resource Group
    // =====================================================================

    @Override
    public void getResourceGroup(
            io.github.xinfra.lab.xkv.proto.Pdpb.GetResourceGroupRequest req,
            StreamObserver<io.github.xinfra.lab.xkv.proto.Pdpb.GetResourceGroupResponse> obs) {
        var rgm = state.resourceGroupManager();
        if (rgm == null) {
            obs.onError(Status.UNIMPLEMENTED.withDescription("resource group not enabled").asRuntimeException());
            return;
        }
        var b = io.github.xinfra.lab.xkv.proto.Pdpb.GetResourceGroupResponse.newBuilder().setHeader(header());
        var group = rgm.getGroup(req.getName());
        if (group != null) b.setGroup(group);
        obs.onNext(b.build());
        obs.onCompleted();
    }

    @Override
    public void addResourceGroup(
            io.github.xinfra.lab.xkv.proto.Pdpb.AddResourceGroupRequest req,
            StreamObserver<io.github.xinfra.lab.xkv.proto.Pdpb.AddResourceGroupResponse> obs) {
        var rgm = state.resourceGroupManager();
        if (rgm == null) {
            obs.onError(Status.UNIMPLEMENTED.withDescription("resource group not enabled").asRuntimeException());
            return;
        }
        if (!req.hasGroup()) {
            obs.onError(Status.INVALID_ARGUMENT.withDescription("group required").asRuntimeException());
            return;
        }
        if (!rgm.addGroup(req.getGroup())) {
            obs.onNext(io.github.xinfra.lab.xkv.proto.Pdpb.AddResourceGroupResponse.newBuilder()
                    .setHeader(errorHeader(
                            io.github.xinfra.lab.xkv.proto.Pdpb.Error.ErrorType.UNKNOWN,
                            "group already exists or invalid name"))
                    .build());
            obs.onCompleted();
            return;
        }
        var cmd = io.github.xinfra.lab.xkv.proto.PdInternalpb.PdCommand.newBuilder()
                .setType(io.github.xinfra.lab.xkv.proto.PdInternalpb.CommandType.CMD_SET_RESOURCE_GROUP)
                .setResourceGroup(io.github.xinfra.lab.xkv.proto.PdInternalpb.ResourceGroupPayload.newBuilder()
                        .setGroup(req.getGroup()))
                .build();
        proposeOrApply(cmd, obs,
                () -> io.github.xinfra.lab.xkv.proto.Pdpb.AddResourceGroupResponse.newBuilder()
                        .setHeader(header()).build());
    }

    @Override
    public void modifyResourceGroup(
            io.github.xinfra.lab.xkv.proto.Pdpb.ModifyResourceGroupRequest req,
            StreamObserver<io.github.xinfra.lab.xkv.proto.Pdpb.ModifyResourceGroupResponse> obs) {
        var rgm = state.resourceGroupManager();
        if (rgm == null) {
            obs.onError(Status.UNIMPLEMENTED.withDescription("resource group not enabled").asRuntimeException());
            return;
        }
        if (!req.hasGroup()) {
            obs.onError(Status.INVALID_ARGUMENT.withDescription("group required").asRuntimeException());
            return;
        }
        if (!rgm.modifyGroup(req.getGroup())) {
            obs.onNext(io.github.xinfra.lab.xkv.proto.Pdpb.ModifyResourceGroupResponse.newBuilder()
                    .setHeader(errorHeader(
                            io.github.xinfra.lab.xkv.proto.Pdpb.Error.ErrorType.UNKNOWN,
                            "group not found"))
                    .build());
            obs.onCompleted();
            return;
        }
        var cmd = io.github.xinfra.lab.xkv.proto.PdInternalpb.PdCommand.newBuilder()
                .setType(io.github.xinfra.lab.xkv.proto.PdInternalpb.CommandType.CMD_SET_RESOURCE_GROUP)
                .setResourceGroup(io.github.xinfra.lab.xkv.proto.PdInternalpb.ResourceGroupPayload.newBuilder()
                        .setGroup(req.getGroup()))
                .build();
        proposeOrApply(cmd, obs,
                () -> io.github.xinfra.lab.xkv.proto.Pdpb.ModifyResourceGroupResponse.newBuilder()
                        .setHeader(header()).build());
    }

    @Override
    public void deleteResourceGroup(
            io.github.xinfra.lab.xkv.proto.Pdpb.DeleteResourceGroupRequest req,
            StreamObserver<io.github.xinfra.lab.xkv.proto.Pdpb.DeleteResourceGroupResponse> obs) {
        var rgm = state.resourceGroupManager();
        if (rgm == null) {
            obs.onError(Status.UNIMPLEMENTED.withDescription("resource group not enabled").asRuntimeException());
            return;
        }
        if (!rgm.deleteGroup(req.getName())) {
            obs.onNext(io.github.xinfra.lab.xkv.proto.Pdpb.DeleteResourceGroupResponse.newBuilder()
                    .setHeader(errorHeader(
                            io.github.xinfra.lab.xkv.proto.Pdpb.Error.ErrorType.UNKNOWN,
                            "group not found or cannot delete default"))
                    .build());
            obs.onCompleted();
            return;
        }
        var cmd = io.github.xinfra.lab.xkv.proto.PdInternalpb.PdCommand.newBuilder()
                .setType(io.github.xinfra.lab.xkv.proto.PdInternalpb.CommandType.CMD_DELETE_RESOURCE_GROUP)
                .setResourceGroup(io.github.xinfra.lab.xkv.proto.PdInternalpb.ResourceGroupPayload.newBuilder()
                        .setName(req.getName()))
                .build();
        proposeOrApply(cmd, obs,
                () -> io.github.xinfra.lab.xkv.proto.Pdpb.DeleteResourceGroupResponse.newBuilder()
                        .setHeader(header()).build());
    }

    @Override
    public void listResourceGroups(
            io.github.xinfra.lab.xkv.proto.Pdpb.ListResourceGroupsRequest req,
            StreamObserver<io.github.xinfra.lab.xkv.proto.Pdpb.ListResourceGroupsResponse> obs) {
        var rgm = state.resourceGroupManager();
        if (rgm == null) {
            obs.onError(Status.UNIMPLEMENTED.withDescription("resource group not enabled").asRuntimeException());
            return;
        }
        var b = io.github.xinfra.lab.xkv.proto.Pdpb.ListResourceGroupsResponse.newBuilder().setHeader(header());
        for (var g : rgm.listGroups()) b.addGroups(g);
        obs.onNext(b.build());
        obs.onCompleted();
    }

    /** Stable 64-bit hash for the lock-key — matches TiKV's deadlock_key_hash. */
    private static long hashKey(byte[] key) {
        // Simple FNV-1a 64. Cheap, deterministic, no external dep.
        long h = 0xcbf29ce484222325L;
        for (byte b : key) {
            h ^= (b & 0xffL);
            h *= 0x100000001b3L;
        }
        return h;
    }

}
