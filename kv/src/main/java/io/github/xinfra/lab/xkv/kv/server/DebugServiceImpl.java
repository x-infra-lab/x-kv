package io.github.xinfra.lab.xkv.kv.server;

import com.google.protobuf.ByteString;
import io.github.xinfra.lab.xkv.common.metrics.XKvMetrics;
import io.github.xinfra.lab.xkv.kv.config.ConfigManager;
import io.github.xinfra.lab.xkv.kv.engine.StorageEngine;
import io.github.xinfra.lab.xkv.kv.store.Store;
import io.github.xinfra.lab.xkv.proto.DebugGrpc;
import io.github.xinfra.lab.xkv.proto.Debugpb;
import io.github.xinfra.lab.xkv.proto.Metapb;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import io.micrometer.prometheusmetrics.PrometheusMeterRegistry;

import java.io.File;
import java.nio.file.Path;
import java.util.List;

public final class DebugServiceImpl extends DebugGrpc.DebugImplBase {

    private final PrometheusMeterRegistry registry;
    private final Store store;
    private final StorageEngine engine;
    private final Metapb.Store storeMetadata;
    private final Path dataDir;
    private final ConfigManager configManager;

    public DebugServiceImpl() {
        this(null);
    }

    public DebugServiceImpl(PrometheusMeterRegistry registry) {
        this(registry, null, null, null, null);
    }

    public DebugServiceImpl(PrometheusMeterRegistry registry, Store store,
                            StorageEngine engine, Metapb.Store storeMetadata,
                            Path dataDir) {
        this(registry, store, engine, storeMetadata, dataDir, null);
    }

    public DebugServiceImpl(PrometheusMeterRegistry registry, Store store,
                            StorageEngine engine, Metapb.Store storeMetadata,
                            Path dataDir, ConfigManager configManager) {
        this.registry = registry;
        this.store = store;
        this.engine = engine;
        this.storeMetadata = storeMetadata;
        this.dataDir = dataDir;
        this.configManager = configManager;
    }

    @Override
    public void getMetrics(Debugpb.GetMetricsRequest request,
                           StreamObserver<Debugpb.GetMetricsResponse> responseObserver) {
        var reg = this.registry != null ? this.registry : XKvMetrics.registry();
        String payload = reg.scrape();
        responseObserver.onNext(Debugpb.GetMetricsResponse.newBuilder()
                .setPayload(ByteString.copyFromUtf8(payload))
                .setContentType("text/plain; version=0.0.4; charset=utf-8")
                .build());
        responseObserver.onCompleted();
    }

    @Override
    public void getRegionInfo(Debugpb.GetRegionInfoRequest request,
                              StreamObserver<Debugpb.GetRegionInfoResponse> responseObserver) {
        if (store == null) {
            responseObserver.onError(Status.UNAVAILABLE.withDescription("store not initialized").asRuntimeException());
            return;
        }
        var peerOpt = store.peerForRegion(request.getRegionId());
        if (peerOpt.isEmpty()) {
            responseObserver.onError(Status.NOT_FOUND
                    .withDescription("region not found on this store")
                    .asRuntimeException());
            return;
        }
        var peer = peerOpt.get();
        var builder = Debugpb.GetRegionInfoResponse.newBuilder()
                .setRegion(peer.region())
                .setAppliedIndex(peer.appliedIndex())
                .setLastIndex(peer.lastIndex())
                .setCommitIndex(peer.commitIndex())
                .setTerm(peer.currentTerm());
        if (peer.isLeader()) {
            builder.setLeader(peer.self());
        }
        responseObserver.onNext(builder.build());
        responseObserver.onCompleted();
    }

    @Override
    public void getRaftState(Debugpb.GetRaftStateRequest request,
                             StreamObserver<Debugpb.GetRaftStateResponse> responseObserver) {
        if (store == null) {
            responseObserver.onError(Status.UNAVAILABLE.withDescription("store not initialized").asRuntimeException());
            return;
        }
        var peerOpt = store.peerForRegion(request.getRegionId());
        if (peerOpt.isEmpty()) {
            responseObserver.onError(Status.NOT_FOUND
                    .withDescription("region not found on this store")
                    .asRuntimeException());
            return;
        }
        var peer = peerOpt.get();
        responseObserver.onNext(Debugpb.GetRaftStateResponse.newBuilder()
                .setRegionId(request.getRegionId())
                .setTerm(peer.currentTerm())
                .setVote(peer.votedFor())
                .setCommit(peer.commitIndex())
                .setLastIndex(peer.lastIndex())
                .setFirstIndex(peer.firstIndex())
                .setAppliedIndex(peer.appliedIndex())
                .build());
        responseObserver.onCompleted();
    }

    @Override
    public void getSnapshotMeta(Debugpb.GetSnapshotMetaRequest request,
                                StreamObserver<Debugpb.GetSnapshotMetaResponse> responseObserver) {
        if (store == null) {
            responseObserver.onError(Status.UNAVAILABLE.withDescription("store not initialized").asRuntimeException());
            return;
        }
        var peerOpt = store.peerForRegion(request.getRegionId());
        if (peerOpt.isEmpty()) {
            responseObserver.onError(Status.NOT_FOUND
                    .withDescription("region not found on this store")
                    .asRuntimeException());
            return;
        }
        var peer = peerOpt.get();
        var region = peer.region();
        var builder = Debugpb.GetSnapshotMetaResponse.newBuilder();

        builder.setExists(true)
                .setRaftTerm(peer.currentTerm())
                .setRaftIndex(peer.appliedIndex())
                .setStartKey(region.getStartKey())
                .setEndKey(region.getEndKey())
                .addAllCfNames(List.of("default", "lock", "write"));

        responseObserver.onNext(builder.build());
        responseObserver.onCompleted();
    }

    @Override
    public void getClusterInfo(Debugpb.GetClusterInfoRequest request,
                               StreamObserver<Debugpb.GetClusterInfoResponse> responseObserver) {
        long regionCount = store != null ? store.peers().size() : 0;
        responseObserver.onNext(Debugpb.GetClusterInfoResponse.newBuilder()
                .setClusterId(storeMetadata != null ? 1 : 0)
                .setStoreCount(1)
                .setRegionCount(regionCount)
                .build());
        responseObserver.onCompleted();
    }

    @Override
    public void getStoreInfo(Debugpb.GetStoreInfoRequest request,
                             StreamObserver<Debugpb.GetStoreInfoResponse> responseObserver) {
        var builder = Debugpb.GetStoreInfoResponse.newBuilder();
        if (storeMetadata != null) {
            builder.setStore(storeMetadata);
        }
        if (dataDir != null) {
            File dir = dataDir.toFile();
            builder.setCapacity(dir.getTotalSpace());
            builder.setUsed(dir.getTotalSpace() - dir.getFreeSpace());
        }
        if (store != null) {
            builder.setRegionCount(store.peers().size());
        }
        responseObserver.onNext(builder.build());
        responseObserver.onCompleted();
    }

    @Override
    public void compactionEvent(Debugpb.CompactionEventRequest request,
                                StreamObserver<Debugpb.CompactionEventResponse> responseObserver) {
        if (engine == null) {
            responseObserver.onError(Status.UNAVAILABLE.withDescription("engine not initialized").asRuntimeException());
            return;
        }
        StorageEngine.Cf cf;
        try {
            cf = StorageEngine.Cf.valueOf(request.getCf().toUpperCase());
        } catch (IllegalArgumentException e) {
            responseObserver.onError(Status.INVALID_ARGUMENT
                    .withDescription("unknown CF: " + request.getCf()).asRuntimeException());
            return;
        }
        byte[] start = request.getStartKey().isEmpty() ? null : request.getStartKey().toByteArray();
        byte[] end = request.getEndKey().isEmpty() ? null : request.getEndKey().toByteArray();
        engine.compactRange(cf, start, end);
        responseObserver.onNext(Debugpb.CompactionEventResponse.getDefaultInstance());
        responseObserver.onCompleted();
    }

    @Override
    public void getAllRegions(Debugpb.GetAllRegionsRequest request,
                             StreamObserver<Debugpb.GetAllRegionsResponse> responseObserver) {
        var builder = Debugpb.GetAllRegionsResponse.newBuilder();
        if (store != null) {
            for (var peer : store.peers()) {
                builder.addRegions(peer.region());
            }
        }
        responseObserver.onNext(builder.build());
        responseObserver.onCompleted();
    }

    @Override
    public void unsafeForceLeader(Debugpb.UnsafeForceLeaderRequest request,
                                  StreamObserver<Debugpb.UnsafeForceLeaderResponse> responseObserver) {
        responseObserver.onError(Status.UNIMPLEMENTED
                .withDescription("UnsafeForceLeader is not yet supported").asRuntimeException());
    }

    @Override
    public void getConfig(Debugpb.GetConfigRequest request,
                           StreamObserver<Debugpb.GetConfigResponse> responseObserver) {
        if (configManager == null) {
            responseObserver.onError(Status.UNAVAILABLE
                    .withDescription("config manager not initialized").asRuntimeException());
            return;
        }
        var builder = Debugpb.GetConfigResponse.newBuilder();
        for (var e : configManager.getAll().entrySet()) {
            builder.addEntries(Debugpb.ConfigEntry.newBuilder()
                    .setKey(e.getKey())
                    .setValue(e.getValue()));
        }
        responseObserver.onNext(builder.build());
        responseObserver.onCompleted();
    }

    @Override
    public void modifyConfig(Debugpb.ModifyConfigRequest request,
                              StreamObserver<Debugpb.ModifyConfigResponse> responseObserver) {
        if (configManager == null) {
            responseObserver.onError(Status.UNAVAILABLE
                    .withDescription("config manager not initialized").asRuntimeException());
            return;
        }
        var errors = new StringBuilder();
        for (var entry : request.getEntriesList()) {
            String err = configManager.set(entry.getKey(), entry.getValue());
            if (err != null) {
                if (!errors.isEmpty()) errors.append("; ");
                errors.append(err);
            }
        }
        var resp = Debugpb.ModifyConfigResponse.newBuilder();
        if (!errors.isEmpty()) resp.setError(errors.toString());
        responseObserver.onNext(resp.build());
        responseObserver.onCompleted();
    }
}
