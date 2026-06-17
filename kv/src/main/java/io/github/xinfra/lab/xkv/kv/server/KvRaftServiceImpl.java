package io.github.xinfra.lab.xkv.kv.server;

import io.github.xinfra.lab.raft.proto.Eraftpb;
import io.github.xinfra.lab.xkv.common.metrics.XKvMetrics;
import io.github.xinfra.lab.xkv.kv.engine.SnapshotEngine;
import io.github.xinfra.lab.xkv.kv.transport.RaftMessageDispatcher;
import io.github.xinfra.lab.xkv.proto.KvRaftGrpc;
import io.github.xinfra.lab.xkv.proto.KvServerpb.Done;
import io.github.xinfra.lab.xkv.proto.KvServerpb.RaftMessage;
import io.github.xinfra.lab.xkv.proto.KvServerpb.SnapshotChunk;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import io.micrometer.core.instrument.Counter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * Inbound side of the KV-to-KV Raft transport.
 *
 * <p>One bidi stream per peering link; each {@link RaftMessage} on the
 * stream is decoded and dispatched by {@code region_id} to the local
 * region peer's transport via {@link RaftMessageDispatcher}.
 *
 * <p>{@link #sendSnapshot} accepts a stream of {@link SnapshotChunk}s,
 * verifies CRCs (delegated to {@link SnapshotEngine}), wipes the region's
 * stale data, and ingests the new payload — all under one atomic
 * {@code WriteBatch}. Chunks are collected in-memory; for large snapshots,
 * the design supports disk staging via {@code SnapshotEngine.checkpointDirFor}.
 */
public final class KvRaftServiceImpl extends KvRaftGrpc.KvRaftImplBase {
    private static final Logger log = LoggerFactory.getLogger(KvRaftServiceImpl.class);

    private static final int MAX_CONCURRENT_SNAPSHOTS = 2;

    private final RaftMessageDispatcher dispatcher;
    private final SnapshotEngine snapshotEngine;
    private final java.util.concurrent.Semaphore snapshotPermits =
            new java.util.concurrent.Semaphore(MAX_CONCURRENT_SNAPSHOTS);
    private final Counter raftParseErrors = XKvMetrics.errorCounter("kv_raft_service", "raft_parse");
    private final Counter snapshotInstallErrors = XKvMetrics.errorCounter("kv_raft_service", "snapshot_install");

    public KvRaftServiceImpl() { this(null, null); }

    public KvRaftServiceImpl(RaftMessageDispatcher dispatcher) { this(dispatcher, null); }

    public KvRaftServiceImpl(RaftMessageDispatcher dispatcher, SnapshotEngine snapshotEngine) {
        this.dispatcher = dispatcher;
        this.snapshotEngine = snapshotEngine;
    }

    @Override
    public StreamObserver<RaftMessage> raft(StreamObserver<Done> resp) {
        if (dispatcher == null) {
            resp.onError(Status.UNIMPLEMENTED.asRuntimeException());
            return new NoopRequestObserver<>();
        }
        return new StreamObserver<>() {
            @Override
            public void onNext(RaftMessage wire) {
                try {
                    var msg = Eraftpb.Message.parseFrom(wire.getMessage());
                    dispatcher.deliver(wire.getRegionId(), msg);
                } catch (Throwable t) {
                    raftParseErrors.increment();
                    log.warn("inbound raft parse failed: {}", t.getMessage());
                }
            }
            @Override public void onError(Throwable t) {
                log.debug("inbound raft stream error: {}", t.getMessage());
            }
            @Override public void onCompleted() {
                resp.onNext(Done.newBuilder().build());
                resp.onCompleted();
            }
        };
    }

    @Override
    public StreamObserver<SnapshotChunk> sendSnapshot(StreamObserver<Done> resp) {
        if (snapshotEngine == null) {
            resp.onError(Status.UNIMPLEMENTED
                    .withDescription("snapshot engine not configured on this node")
                    .asRuntimeException());
            return new NoopRequestObserver<>();
        }
        if (!snapshotPermits.tryAcquire()) {
            resp.onError(Status.RESOURCE_EXHAUSTED
                    .withDescription("too many concurrent snapshot receives")
                    .asRuntimeException());
            return new NoopRequestObserver<>();
        }
        return new StreamObserver<>() {
            private static final long MAX_SNAPSHOT_BYTES = 64L * 1024 * 1024;
            private final List<SnapshotChunk> buffered = new ArrayList<>();
            private long regionId = 0;
            private long totalBytes = 0;

            @Override
            public void onNext(SnapshotChunk chunk) {
                totalBytes += chunk.getData().size();
                if (totalBytes > MAX_SNAPSHOT_BYTES) {
                    throw Status.RESOURCE_EXHAUSTED
                            .withDescription("snapshot exceeds " + MAX_SNAPSHOT_BYTES + " bytes")
                            .asRuntimeException();
                }
                if (chunk.hasMeta() && regionId == 0) regionId = chunk.getMeta().getRegionId();
                buffered.add(chunk);
            }

            @Override
            public void onError(Throwable t) {
                log.warn("sendSnapshot stream error region={}: {}", regionId, t.getMessage());
                buffered.clear();
                snapshotPermits.release();
            }

            @Override
            public void onCompleted() {
                try {
                    if (regionId == 0) throw new IllegalStateException("snapshot has no meta");
                    snapshotEngine.receiveAndInstall(regionId, buffered);
                    resp.onNext(Done.newBuilder().build());
                    resp.onCompleted();
                } catch (Throwable t) {
                    snapshotInstallErrors.increment();
                    log.warn("snapshot install failed region={}: {}", regionId, t.getMessage());
                    resp.onError(Status.INTERNAL.withDescription(
                            "snapshot install failed: " + t.getMessage()).asRuntimeException());
                } finally {
                    buffered.clear();
                    snapshotPermits.release();
                }
            }
        };
    }

    private static final class NoopRequestObserver<T> implements StreamObserver<T> {
        @Override public void onNext(T value) {}
        @Override public void onError(Throwable t) {}
        @Override public void onCompleted() {}
    }
}
