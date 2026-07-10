package io.github.xinfra.lab.xkv.pd.server;

import io.github.xinfra.lab.raft.Transport;
import io.github.xinfra.lab.raft.proto.Eraftpb;
import io.github.xinfra.lab.xkv.pd.state.DeadlockDetector;
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

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * gRPC frontend for the PD service.
 *
 * <p>All mutating RPCs are proposed through raft and applied deterministically
 * on every PD node. All read RPCs require linearizable read (readIndex) to
 * guarantee the serving node is still the authoritative leader.
 */
public final class PdServiceImpl extends PDGrpc.PDImplBase {
    private static final Logger log = LoggerFactory.getLogger(PdServiceImpl.class);

    private static final long RAFT_PROPOSE_TIMEOUT_MS = 5_000;
    private static final long READ_INDEX_TIMEOUT_MS = 5_000;

    private final PdStateMachine state;
    private final Tso tso;
    private final SafePointService safePoint;
    private final DeadlockDetector deadlock;
    private final long clusterId;
    private final long nodeId;
    private final StoreStatsCache storeStatsCache;
    private final io.github.xinfra.lab.xkv.pd.state.placement.PlacementRuleManager placementRuleManager;
    private final AtomicLong gcSafePoint = new AtomicLong(0);

    private volatile io.github.xinfra.lab.xkv.pd.raft.PdRaftNode raftNode;
    private volatile Transport transport;
    private volatile io.github.xinfra.lab.xkv.pd.state.OperatorController operatorController;

    public PdServiceImpl(PdStateMachine state,
                         Tso tso,
                         SafePointService safePoint,
                         DeadlockDetector deadlock,
                         long clusterId,
                         long nodeId,
                         StoreStatsCache storeStatsCache,
                         io.github.xinfra.lab.xkv.pd.state.placement.PlacementRuleManager placementRuleManager) {
        this.state = state;
        this.tso = tso;
        this.safePoint = safePoint;
        this.deadlock = deadlock;
        this.clusterId = clusterId;
        this.nodeId = nodeId;
        this.storeStatsCache = storeStatsCache;
        this.placementRuleManager = placementRuleManager;
    }


    public StoreStatsCache storeStatsCache() { return storeStatsCache; }

    public void setRaftNode(io.github.xinfra.lab.xkv.pd.raft.PdRaftNode node) {
        this.raftNode = node;
    }

    public void setTransport(Transport transport) {
        this.transport = transport;
    }

    public void setOperatorController(io.github.xinfra.lab.xkv.pd.state.OperatorController oc) {
        this.operatorController = oc;
    }

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

    /**
     * Ensure linearizable read: if raft is wired, confirm leadership via ReadIndex
     * before serving a read. Returns true if read is safe to proceed. On failure,
     * sends an error via the observer and returns false.
     */
    private <T> boolean ensureLinearizableRead(StreamObserver<T> obs) {
        var rn = raftNode;
        if (rn == null) return true;
        if (!rn.isLeader()) {
            obs.onError(Status.UNAVAILABLE
                    .withDescription("not PD leader").asRuntimeException());
            return false;
        }
        try {
            rn.readIndex().get(READ_INDEX_TIMEOUT_MS, TimeUnit.MILLISECONDS);
            return true;
        } catch (Exception e) {
            obs.onError(Status.UNAVAILABLE
                    .withDescription("readIndex failed: " + e.getMessage())
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
        if (!ensureLinearizableRead(obs)) return;
        var b = GetMembersResponse.newBuilder().setHeader(header());
        for (var m : state.allMembers()) {
            var mb = Member.newBuilder()
                    .setMemberId(m.id())
                    .setName(m.name());
            if (!m.clientAddress().isEmpty()) mb.addClientUrls(m.clientAddress());
            if (!m.raftAddress().isEmpty()) mb.addPeerUrls(m.raftAddress());
            b.addMembers(mb.build());
        }
        long leaderId = raftNode != null ? raftNode.leaderNodeId() : nodeId;
        for (var mb : b.getMembersList()) {
            if (mb.getMemberId() == leaderId) {
                b.setLeader(mb);
                break;
            }
        }
        obs.onNext(b.build());
        obs.onCompleted();
    }

    @Override
    public void getClusterInfo(GetClusterInfoRequest req, StreamObserver<GetClusterInfoResponse> obs) {
        if (!ensureLinearizableRead(obs)) return;
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
        if (!ensureLinearizableRead(obs)) return;
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
        var rn = raftNode;
        if (rn != null && !rn.isLeader()) {
            obs.onError(Status.UNAVAILABLE
                    .withDescription("not PD leader").asRuntimeException());
            return;
        }
        int count = req.getCount() <= 0 ? 1 : req.getCount();
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
                var rn = raftNode;
                if (rn != null && !rn.isLeader()) {
                    resp.onNext(TsoResponse.newBuilder()
                            .setHeader(errorHeader(
                                    io.github.xinfra.lab.xkv.proto.Pdpb.Error.ErrorType.UNKNOWN,
                                    "not PD leader"))
                            .build());
                    return;
                }
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
        if (!ensureLinearizableRead(obs)) return;
        var b = GetStoreResponse.newBuilder().setHeader(header());
        state.getStore(req.getStoreId()).ifPresent(b::setStore);
        storeStatsCache.get(req.getStoreId()).ifPresent(b::setStats);
        obs.onNext(b.build());
        obs.onCompleted();
    }

    @Override
    public void getAllStores(GetAllStoresRequest req, StreamObserver<GetAllStoresResponse> obs) {
        if (!ensureLinearizableRead(obs)) return;
        var b = GetAllStoresResponse.newBuilder().setHeader(header());
        for (var s : state.allStores()) b.addStores(s);
        obs.onNext(b.build()); obs.onCompleted();
    }

    @Override
    public void storeHeartbeat(StoreHeartbeatRequest req, StreamObserver<StoreHeartbeatResponse> obs) {
        var rn = raftNode;
        if (rn != null && !rn.isLeader()) {
            obs.onError(Status.UNAVAILABLE
                    .withDescription("not PD leader").asRuntimeException());
            return;
        }
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
                    if (rn == null) {
                        state.updateRegion(hb.getRegion());
                    } else if (rn.isLeader()) {
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
        if (!ensureLinearizableRead(obs)) return;
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
        if (!ensureLinearizableRead(obs)) return;
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
        if (!ensureLinearizableRead(obs)) return;
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
        var rn = raftNode;
        if (rn != null && !rn.isLeader()) {
            obs.onError(Status.UNAVAILABLE
                    .withDescription("not PD leader").asRuntimeException());
            return;
        }
        int peerCount = req.getRegion().getPeersCount();
        int totalIds = 1 + peerCount;
        long base = state.allocId(totalIds);
        var cmd = io.github.xinfra.lab.xkv.proto.PdInternalpb.PdCommand.newBuilder()
                .setType(io.github.xinfra.lab.xkv.proto.PdInternalpb.CommandType.CMD_ALLOC_ID)
                .setAllocId(io.github.xinfra.lab.xkv.proto.PdInternalpb.AllocIdPayload.newBuilder()
                        .setCount(totalIds)
                        .setBaseId(base))
                .build();
        proposeOrApply(cmd, obs, () -> {
            var b = AskSplitResponse.newBuilder()
                    .setHeader(header())
                    .setNewRegionId(base);
            for (int p = 0; p < peerCount; p++) {
                b.addNewPeerIds(base + 1 + p);
            }
            return b.build();
        });
    }
    @Override
    public void askBatchSplit(AskBatchSplitRequest req, StreamObserver<AskBatchSplitResponse> obs) {
        int splitCount = req.getSplitCount();
        if (splitCount <= 0) {
            obs.onError(io.grpc.Status.INVALID_ARGUMENT
                    .withDescription("split_count must be > 0").asRuntimeException());
            return;
        }
        var rn = raftNode;
        if (rn != null && !rn.isLeader()) {
            obs.onError(Status.UNAVAILABLE
                    .withDescription("not PD leader").asRuntimeException());
            return;
        }
        int peerCount = req.getRegion().getPeersCount();
        int totalIds = splitCount * (1 + peerCount);
        long base = state.allocId(totalIds);
        var cmd = io.github.xinfra.lab.xkv.proto.PdInternalpb.PdCommand.newBuilder()
                .setType(io.github.xinfra.lab.xkv.proto.PdInternalpb.CommandType.CMD_ALLOC_ID)
                .setAllocId(io.github.xinfra.lab.xkv.proto.PdInternalpb.AllocIdPayload.newBuilder()
                        .setCount(totalIds)
                        .setBaseId(base))
                .build();
        proposeOrApply(cmd, obs, () -> {
            var b = AskBatchSplitResponse.newBuilder().setHeader(header());
            long cursor = base;
            for (int i = 0; i < splitCount; i++) {
                var sb = SplitID.newBuilder().setNewRegionId(cursor++);
                for (int p = 0; p < peerCount; p++) {
                    sb.addNewPeerIds(cursor++);
                }
                b.addIds(sb.build());
            }
            return b.build();
        });
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
        var oc = operatorController;
        for (var keyBs : req.getSplitKeysList()) {
            byte[] key = keyBs.toByteArray();
            var regionOpt = state.getRegionByKey(key);
            if (regionOpt.isEmpty()) continue;
            var region = regionOpt.get();
            if (oc != null) {
                var sr = SplitRegion.newBuilder()
                        .setPolicy(SplitRegion.Policy.USER_KEY)
                        .addKeys(com.google.protobuf.ByteString.copyFrom(key))
                        .build();
                var resp = io.github.xinfra.lab.xkv.proto.Pdpb.RegionHeartbeatResponse.newBuilder()
                        .setRegionId(region.getId())
                        .setSplitRegion(sr)
                        .build();
                var storeIds = new java.util.HashSet<Long>();
                for (var p : region.getPeersList()) storeIds.add(p.getStoreId());
                long currentVersion = region.getRegionEpoch().getVersion();
                var op = new io.github.xinfra.lab.xkv.pd.state.SimpleOperator(
                        System.nanoTime(), region.getId(),
                        io.github.xinfra.lab.xkv.pd.state.Operator.Kind.SPLIT,
                        "admin-split: user-requested split",
                        resp, storeIds,
                        java.util.List.of(new io.github.xinfra.lab.xkv.pd.state.OperatorSteps.SplitRegionStep(currentVersion + 1)),
                        io.github.xinfra.lab.xkv.pd.state.Operator.PRIORITY_ADMIN);
                oc.addOperator(op);
            }
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
        if (!ensureLinearizableRead(obs)) return;
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
        if (!ensureLinearizableRead(obs)) return;
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
        if (!ensureLinearizableRead(obs)) return;
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
        if (!ensureLinearizableRead(obs)) return;
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
        if (!ensureLinearizableRead(obs)) return;
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
        if (!ensureLinearizableRead(obs)) return;
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
        if (!ensureLinearizableRead(obs)) return;
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

    // =====================================================================
    // Dynamic membership
    // =====================================================================

    @Override
    public void addMember(io.github.xinfra.lab.xkv.proto.Pdpb.AddMemberRequest req,
                          StreamObserver<io.github.xinfra.lab.xkv.proto.Pdpb.AddMemberResponse> obs) {
        var rn = raftNode;
        if (rn == null) {
            obs.onError(Status.UNAVAILABLE
                    .withDescription("single-PD mode does not support membership change")
                    .asRuntimeException());
            return;
        }
        if (!rn.isLeader()) {
            obs.onError(Status.UNAVAILABLE
                    .withDescription("not PD leader").asRuntimeException());
            return;
        }
        if (!req.hasMember()) {
            obs.onError(Status.INVALID_ARGUMENT
                    .withDescription("member required").asRuntimeException());
            return;
        }
        long memberId = req.getMember().getMemberId();
        if (state.getMember(memberId).isPresent()) {
            obs.onError(Status.ALREADY_EXISTS
                    .withDescription("member " + memberId + " already exists")
                    .asRuntimeException());
            return;
        }
        try {
            // Step 1: propose ConfChange (AddNode) through raft.
            var cc = Eraftpb.ConfChangeV2.newBuilder()
                    .addChanges(Eraftpb.ConfChangeSingle.newBuilder()
                            .setType(Eraftpb.ConfChangeType.ConfChangeAddNode)
                            .setNodeId(memberId))
                    .build();
            rn.proposeConfChange(cc).get(RAFT_PROPOSE_TIMEOUT_MS, TimeUnit.MILLISECONDS);

            // Step 2: propose CMD_ADD_MEMBER to persist member metadata.
            String raftAddr = req.getRaftUrl().isEmpty()
                    ? (req.getMember().getPeerUrlsCount() > 0 ? req.getMember().getPeerUrls(0) : "")
                    : req.getRaftUrl();
            String clientAddr = req.getMember().getClientUrlsCount() > 0
                    ? req.getMember().getClientUrls(0) : "";
            var cmd = io.github.xinfra.lab.xkv.proto.PdInternalpb.PdCommand.newBuilder()
                    .setType(io.github.xinfra.lab.xkv.proto.PdInternalpb.CommandType.CMD_ADD_MEMBER)
                    .setMemberChange(io.github.xinfra.lab.xkv.proto.PdInternalpb.MemberChangePayload.newBuilder()
                            .setMemberId(memberId)
                            .setName(req.getMember().getName())
                            .setRaftAddress(raftAddr)
                            .setClientAddress(clientAddr))
                    .build();
            rn.propose(cmd.toByteArray()).get(RAFT_PROPOSE_TIMEOUT_MS, TimeUnit.MILLISECONDS);

            // Step 3: update transport with the new peer.
            if (!raftAddr.isEmpty()) {
                transport.addPeer(memberId, raftAddr);
            }

            // Return updated member list.
            var b = io.github.xinfra.lab.xkv.proto.Pdpb.AddMemberResponse.newBuilder()
                    .setHeader(header());
            for (var m : state.allMembers()) {
                b.addMembers(Member.newBuilder()
                        .setMemberId(m.id())
                        .setName(m.name())
                        .addClientUrls(m.clientAddress())
                        .addPeerUrls(m.raftAddress()));
            }
            obs.onNext(b.build());
            obs.onCompleted();
        } catch (Exception e) {
            obs.onError(Status.INTERNAL
                    .withDescription("addMember failed: " + e.getMessage())
                    .asRuntimeException());
        }
    }

    @Override
    public void removeMember(io.github.xinfra.lab.xkv.proto.Pdpb.RemoveMemberRequest req,
                             StreamObserver<io.github.xinfra.lab.xkv.proto.Pdpb.RemoveMemberResponse> obs) {
        var rn = raftNode;
        if (rn == null) {
            obs.onError(Status.UNAVAILABLE
                    .withDescription("single-PD mode does not support membership change")
                    .asRuntimeException());
            return;
        }
        if (!rn.isLeader()) {
            obs.onError(Status.UNAVAILABLE
                    .withDescription("not PD leader").asRuntimeException());
            return;
        }
        long memberId = req.getMemberId();
        if (state.getMember(memberId).isEmpty()) {
            obs.onError(Status.NOT_FOUND
                    .withDescription("member " + memberId + " not found")
                    .asRuntimeException());
            return;
        }
        try {
            // Step 1: propose CMD_REMOVE_MEMBER to remove metadata.
            var cmd = io.github.xinfra.lab.xkv.proto.PdInternalpb.PdCommand.newBuilder()
                    .setType(io.github.xinfra.lab.xkv.proto.PdInternalpb.CommandType.CMD_REMOVE_MEMBER)
                    .setMemberChange(io.github.xinfra.lab.xkv.proto.PdInternalpb.MemberChangePayload.newBuilder()
                            .setMemberId(memberId))
                    .build();
            rn.propose(cmd.toByteArray()).get(RAFT_PROPOSE_TIMEOUT_MS, TimeUnit.MILLISECONDS);

            // Step 2: propose ConfChange (RemoveNode) through raft.
            var cc = Eraftpb.ConfChangeV2.newBuilder()
                    .addChanges(Eraftpb.ConfChangeSingle.newBuilder()
                            .setType(Eraftpb.ConfChangeType.ConfChangeRemoveNode)
                            .setNodeId(memberId))
                    .build();
            rn.proposeConfChange(cc).get(RAFT_PROPOSE_TIMEOUT_MS, TimeUnit.MILLISECONDS);

            // Step 3: update transport.
            transport.removePeer(memberId);

            // Return updated member list.
            var b = io.github.xinfra.lab.xkv.proto.Pdpb.RemoveMemberResponse.newBuilder()
                    .setHeader(header());
            for (var m : state.allMembers()) {
                b.addMembers(Member.newBuilder()
                        .setMemberId(m.id())
                        .setName(m.name())
                        .addClientUrls(m.clientAddress())
                        .addPeerUrls(m.raftAddress()));
            }
            obs.onNext(b.build());
            obs.onCompleted();
        } catch (Exception e) {
            obs.onError(Status.INTERNAL
                    .withDescription("removeMember failed: " + e.getMessage())
                    .asRuntimeException());
        }
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
