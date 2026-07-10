package io.github.xinfra.lab.xkv.kv.server;

import com.google.protobuf.ByteString;
import io.github.xinfra.lab.xkv.proto.Coprocessor;
import io.github.xinfra.lab.xkv.proto.Kvrpcpb.*;
import io.github.xinfra.lab.xkv.proto.TikvGrpc;
import io.github.xinfra.lab.xkv.proto.Tikvpb;
import io.github.xinfra.lab.xkv.proto.Tikvpb.*;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.github.xinfra.lab.xkv.kv.coprocessor.eval.CopDatumComparator;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.Semaphore;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

public final class TikvServiceImpl extends TikvGrpc.TikvImplBase {
    private static final Logger log = LoggerFactory.getLogger(TikvServiceImpl.class);

    private static final int COP_POOL_CORE = 4;
    private static final int COP_POOL_MAX = 16;
    private static final long COP_POOL_KEEPALIVE_SEC = 60;
    private static final int COP_MAX_PENDING = 1024;

    private final RawKvService rawKv;
    private final TransactionService txn;
    private final CoprocessorService cop;
    private final io.github.xinfra.lab.xkv.kv.store.SplitDriver splitDriver;
    private final RawKvService.PeerLocator splitLocator;
    private volatile io.github.xinfra.lab.xkv.kv.backup.BackupManager backupManager;
    private volatile io.github.xinfra.lab.xkv.kv.backup.RestoreManager restoreManager;

    private final AtomicLong batchPending = new AtomicLong();
    private final ExecutorService copExecutor;
    private final Semaphore copPendingPermits = new Semaphore(COP_MAX_PENDING);

    public TikvServiceImpl() { this(null, null, null, null, null); }

    public TikvServiceImpl(RawKvService rawKv) { this(rawKv, null, null, null, null); }

    public TikvServiceImpl(RawKvService rawKv, TransactionService txn) {
        this(rawKv, txn, null, null, null);
    }

    public TikvServiceImpl(RawKvService rawKv, TransactionService txn,
                           io.github.xinfra.lab.xkv.kv.store.SplitDriver splitDriver,
                           RawKvService.PeerLocator splitLocator) {
        this(rawKv, txn, null, splitDriver, splitLocator);
    }

    public TikvServiceImpl(RawKvService rawKv, TransactionService txn,
                           CoprocessorService cop,
                           io.github.xinfra.lab.xkv.kv.store.SplitDriver splitDriver,
                           RawKvService.PeerLocator splitLocator) {
        this.rawKv = rawKv;
        this.txn = txn;
        this.cop = cop;
        this.splitDriver = splitDriver;
        this.splitLocator = splitLocator;

        AtomicLong copThreadId = new AtomicLong();
        ThreadFactory copTf = r1 -> {
            Thread t = new Thread(r1, "cop-worker-" + copThreadId.getAndIncrement());
            t.setDaemon(true);
            return t;
        };
        this.copExecutor = new ThreadPoolExecutor(
                COP_POOL_CORE, COP_POOL_MAX, COP_POOL_KEEPALIVE_SEC, TimeUnit.SECONDS,
                new PriorityBlockingQueue<>(64), copTf, new ThreadPoolExecutor.CallerRunsPolicy());
    }

    public void shutdown() {
        copExecutor.shutdownNow();
    }

    public void setBackupManager(io.github.xinfra.lab.xkv.kv.backup.BackupManager bm) {
        this.backupManager = bm;
    }

    public void setRestoreManager(io.github.xinfra.lab.xkv.kv.backup.RestoreManager rm) {
        this.restoreManager = rm;
    }

    // ---- Transactional KV ----
    @Override public void kvGet(GetRequest r,
            StreamObserver<GetResponse> o) {
        dispatchTxn(o, () -> txn.kvGet(r));
    }
    @Override public void kvScan(ScanRequest r,
            StreamObserver<ScanResponse> o) {
        dispatchTxn(o, () -> txn.kvScan(r));
    }
    @Override public void kvBatchGet(BatchGetRequest r,
            StreamObserver<BatchGetResponse> o) {
        dispatchTxn(o, () -> txn.kvBatchGet(r));
    }
    @Override public void kvPrewrite(PrewriteRequest r,
            StreamObserver<PrewriteResponse> o) {
        dispatchTxn(o, () -> txn.kvPrewrite(r));
    }
    @Override public void kvPessimisticLock(PessimisticLockRequest r,
            StreamObserver<PessimisticLockResponse> o) {
        dispatchTxn(o, () -> txn.kvPessimisticLock(r));
    }
    @Override public void kvPessimisticRollback(PessimisticRollbackRequest r,
            StreamObserver<PessimisticRollbackResponse> o) {
        dispatchTxn(o, () -> txn.kvPessimisticRollback(r));
    }
    @Override public void kvCommit(CommitRequest r,
            StreamObserver<CommitResponse> o) {
        dispatchTxn(o, () -> txn.kvCommit(r));
    }
    @Override public void kvCleanup(CleanupRequest r,
            StreamObserver<CleanupResponse> o) {
        dispatchTxn(o, () -> txn.kvCleanup(r));
    }
    @Override public void kvBatchRollback(BatchRollbackRequest r,
            StreamObserver<BatchRollbackResponse> o) {
        dispatchTxn(o, () -> txn.kvBatchRollback(r));
    }
    @Override public void kvCheckTxnStatus(CheckTxnStatusRequest r,
            StreamObserver<CheckTxnStatusResponse> o) {
        dispatchTxn(o, () -> txn.kvCheckTxnStatus(r));
    }
    @Override public void kvCheckSecondaryLocks(CheckSecondaryLocksRequest r,
            StreamObserver<CheckSecondaryLocksResponse> o) {
        dispatchTxn(o, () -> txn.kvCheckSecondaryLocks(r));
    }
    @Override public void kvScanLock(ScanLockRequest r,
            StreamObserver<ScanLockResponse> o) {
        dispatchTxn(o, () -> txn.kvScanLock(r));
    }
    @Override public void kvResolveLock(ResolveLockRequest r,
            StreamObserver<ResolveLockResponse> o) {
        dispatchTxn(o, () -> txn.kvResolveLock(r));
    }
    @Override public void kvTxnHeartBeat(TxnHeartBeatRequest r,
            StreamObserver<TxnHeartBeatResponse> o) {
        dispatchTxn(o, () -> txn.kvTxnHeartBeat(r));
    }
    @Override public void kvGC(GCRequest r,
            StreamObserver<GCResponse> o) {
        dispatchTxn(o, () -> txn.kvGC(r));
    }
    @Override public void kvDeleteRange(DeleteRangeRequest r,
            StreamObserver<DeleteRangeResponse> o) {
        dispatchTxn(o, () -> txn.kvDeleteRange(r));
    }

    // ---- Raw KV ----
    @Override public void rawGet(RawGetRequest r,
            StreamObserver<RawGetResponse> o) {
        dispatchRaw(o, () -> rawKv.rawGet(r));
    }
    @Override public void rawBatchGet(RawBatchGetRequest r,
            StreamObserver<RawBatchGetResponse> o) {
        dispatchRaw(o, () -> rawKv.rawBatchGet(r));
    }
    @Override public void rawPut(RawPutRequest r,
            StreamObserver<RawPutResponse> o) {
        dispatchRaw(o, () -> rawKv.rawPut(r));
    }
    @Override public void rawBatchPut(RawBatchPutRequest r,
            StreamObserver<RawBatchPutResponse> o) {
        dispatchRaw(o, () -> rawKv.rawBatchPut(r));
    }
    @Override public void rawDelete(RawDeleteRequest r,
            StreamObserver<RawDeleteResponse> o) {
        dispatchRaw(o, () -> rawKv.rawDelete(r));
    }
    @Override public void rawBatchDelete(RawBatchDeleteRequest r,
            StreamObserver<RawBatchDeleteResponse> o) {
        dispatchRaw(o, () -> rawKv.rawBatchDelete(r));
    }
    @Override public void rawScan(RawScanRequest r,
            StreamObserver<RawScanResponse> o) {
        dispatchRaw(o, () -> rawKv.rawScan(r));
    }
    @Override public void rawDeleteRange(RawDeleteRangeRequest r,
            StreamObserver<RawDeleteRangeResponse> o) {
        dispatchRaw(o, () -> rawKv.rawDeleteRange(r));
    }
    @Override public void rawCAS(RawCASRequest r,
            StreamObserver<RawCASResponse> o) {
        dispatchRaw(o, () -> rawKv.rawCAS(r));
    }

    // ---- Coprocessor ----
    @Override
    public void coprocessor(Coprocessor.Request r, StreamObserver<Coprocessor.Response> o) {
        if (cop == null) { unimpl(o); return; }
        if (!copPendingPermits.tryAcquire()) {
            o.onNext(Coprocessor.Response.newBuilder()
                    .setRegionError(io.github.xinfra.lab.xkv.proto.Errorpb.Error.newBuilder()
                            .setMessage("server is busy")
                            .setServerIsBusy(io.github.xinfra.lab.xkv.proto.Errorpb.ServerIsBusy.newBuilder()
                                    .setReason("coprocessor pool pending queue full"))
                            .build())
                    .build());
            o.onCompleted();
            return;
        }
        copExecutor.execute(prioritized(extractPriority(r), () -> {
            try {
                var checked = validateAndClipCopRegion(r);
                if (checked.error() != null) {
                    o.onNext(Coprocessor.Response.newBuilder().setRegionError(checked.error()).build());
                    o.onCompleted();
                    return;
                }
                o.onNext(cop.handle(checked.request()));
                o.onCompleted();
            } catch (Throwable t) {
                o.onError(Status.INTERNAL.withDescription(t.getMessage()).asRuntimeException());
            } finally {
                copPendingPermits.release();
            }
        }));
    }

    @Override
    public void coprocessorStream(Coprocessor.Request r, StreamObserver<Coprocessor.StreamResponse> o) {
        if (cop == null) { unimpl(o); return; }
        if (!copPendingPermits.tryAcquire()) {
            o.onNext(Coprocessor.StreamResponse.newBuilder()
                    .setRegionError(io.github.xinfra.lab.xkv.proto.Errorpb.Error.newBuilder()
                            .setMessage("server is busy")
                            .setServerIsBusy(io.github.xinfra.lab.xkv.proto.Errorpb.ServerIsBusy.newBuilder()
                                    .setReason("coprocessor pool pending queue full"))
                            .build())
                    .build());
            o.onCompleted();
            return;
        }
        copExecutor.execute(prioritized(extractPriority(r), () -> {
            try {
                var checked = validateAndClipCopRegion(r);
                if (checked.error() != null) {
                    o.onNext(Coprocessor.StreamResponse.newBuilder().setRegionError(checked.error()).build());
                    o.onCompleted();
                    return;
                }
                cop.handleStream(checked.request(), o::onNext);
                o.onCompleted();
            } catch (Throwable t) {
                o.onError(Status.INTERNAL.withDescription(t.getMessage()).asRuntimeException());
            } finally {
                copPendingPermits.release();
            }
        }));
    }

    @Override
    public void batchCoprocessor(Coprocessor.BatchRequest r, StreamObserver<Coprocessor.BatchResponse> o) {
        if (cop == null) { unimpl(o); return; }
        if (!copPendingPermits.tryAcquire()) {
            o.onError(Status.RESOURCE_EXHAUSTED
                    .withDescription("coprocessor pool pending queue full").asRuntimeException());
            return;
        }
        copExecutor.execute(prioritized(0, () -> {
            try {
                cop.handleBatch(r, o::onNext, singleReq -> {
                    var checked = validateAndClipCopRegion(singleReq);
                    return new CoprocessorService.RegionCheckResult(checked.error(), checked.request());
                });
                o.onCompleted();
            } catch (Throwable t) {
                o.onError(Status.INTERNAL.withDescription(t.getMessage()).asRuntimeException());
            } finally {
                copPendingPermits.release();
            }
        }));
    }

    // ---- Region admin ----
    @Override
    public void splitRegion(SplitRegionRequest r, StreamObserver<SplitRegionResponse> o) {
        if (splitDriver == null || splitLocator == null) { unimpl(o); return; }
        try {
            if (r.getSplitKeysCount() == 0) {
                o.onNext(SplitRegionResponse.newBuilder()
                        .setRegionError(io.github.xinfra.lab.xkv.proto.Errorpb.Error.newBuilder()
                                .setMessage("split_keys required")
                                .build())
                        .build());
                o.onCompleted();
                return;
            }
            var parentPeer = splitLocator.peerForKey(r.getSplitKeys(0).toByteArray());
            if (parentPeer == null) {
                o.onNext(SplitRegionResponse.newBuilder()
                        .setRegionError(io.github.xinfra.lab.xkv.proto.Errorpb.Error.newBuilder()
                                .setMessage("region not found for split_key").build()).build());
                o.onCompleted();
                return;
            }
            if (!parentPeer.isLeader()) {
                o.onNext(SplitRegionResponse.newBuilder()
                        .setRegionError(io.github.xinfra.lab.xkv.proto.Errorpb.Error.newBuilder()
                                .setMessage("not leader")
                                .setNotLeader(io.github.xinfra.lab.xkv.proto.Errorpb.NotLeader.newBuilder()
                                        .setRegionId(parentPeer.regionId()))
                                .build()).build());
                o.onCompleted();
                return;
            }
            var keys = new java.util.ArrayList<byte[]>(r.getSplitKeysCount());
            for (var k : r.getSplitKeysList()) keys.add(k.toByteArray());
            var resulting = splitDriver.split(parentPeer, keys);
            var resp = SplitRegionResponse.newBuilder();
            for (var rg : resulting) resp.addRegions(rg);
            o.onNext(resp.build());
            o.onCompleted();
        } catch (Throwable t) {
            o.onError(io.grpc.Status.INTERNAL.withDescription(t.getMessage()).asRuntimeException());
        }
    }

    // ---- MVCC inspection ----
    @Override public void mvccGetByKey(MvccGetByKeyRequest r,
            StreamObserver<MvccGetByKeyResponse> o) {
        dispatchTxn(o, () -> txn.kvMvccGetByKey(r));
    }
    @Override public void mvccGetByStartTs(MvccGetByStartTsRequest r,
            StreamObserver<MvccGetByStartTsResponse> o) {
        dispatchTxn(o, () -> txn.kvMvccGetByStartTs(r));
    }

    // ---- Version negotiation ----

    private static final String CLUSTER_VERSION = "8.0.0-xkv";
    private static final java.util.List<String> SUPPORTED_FEATURES = java.util.List.of(
            "batch-commands",
            "async-commit",
            "1pc",
            "pessimistic-lock",
            "pipelined-pessimistic-lock",
            "in-memory-lock",
            "resolved-ts",
            "cdc-incremental-scan",
            "online-config"
    );

    @Override
    public void getVersion(Tikvpb.GetVersionRequest r,
                           StreamObserver<Tikvpb.GetVersionResponse> o) {
        log.debug("getVersion: client_version={} features={}",
                r.getClientVersion(), r.getClientFeaturesList());
        o.onNext(Tikvpb.GetVersionResponse.newBuilder()
                .setClusterVersion(CLUSTER_VERSION)
                .addAllSupportedFeatures(SUPPORTED_FEATURES)
                .build());
        o.onCompleted();
    }

    // ---- BatchCommands ----
    @Override
    public StreamObserver<BatchCommandsRequest> batchCommands(StreamObserver<BatchCommandsResponse> resp) {
        return new StreamObserver<>() {
            @Override
            public void onNext(BatchCommandsRequest batch) {
                long pending = batchPending.incrementAndGet();
                try {
                    var rb = BatchCommandsResponse.newBuilder();
                    int count = batch.getRequestsCount();
                    for (int i = 0; i < count; i++) {
                        rb.addResponses(dispatchOne(batch.getRequests(i)));
                        if (i < batch.getRequestIdsCount()) {
                            rb.addRequestIds(batch.getRequestIds(i));
                        }
                    }
                    rb.setTransportLayerLoad(pending);
                    resp.onNext(rb.build());
                } catch (Throwable t) {
                    log.error("batchCommands dispatch failed", t);
                    resp.onError(Status.INTERNAL.withDescription(t.getMessage()).asRuntimeException());
                } finally {
                    batchPending.decrementAndGet();
                }
            }

            @Override
            public void onError(Throwable t) {
                log.debug("batchCommands client error: {}", t.getMessage());
            }

            @Override
            public void onCompleted() {
                resp.onCompleted();
            }
        };
    }

    private BatchCommandsResponse.Response dispatchOne(BatchCommandsRequest.Request req) {
        var rb = BatchCommandsResponse.Response.newBuilder();
        try {
            switch (req.getCmdCase()) {
                // Transactional KV
                case GET -> { if (txn != null) rb.setGet(txn.kvGet(req.getGet())); }
                case SCAN -> { if (txn != null) rb.setScan(txn.kvScan(req.getScan())); }
                case PREWRITE -> { if (txn != null) rb.setPrewrite(txn.kvPrewrite(req.getPrewrite())); }
                case COMMIT -> { if (txn != null) rb.setCommit(txn.kvCommit(req.getCommit())); }
                case CLEANUP -> { if (txn != null) rb.setCleanup(txn.kvCleanup(req.getCleanup())); }
                case BATCHGET -> { if (txn != null) rb.setBatchGet(txn.kvBatchGet(req.getBatchGet())); }
                case BATCHROLLBACK -> {
                    if (txn != null) rb.setBatchRollback(
                            txn.kvBatchRollback(req.getBatchRollback()));
                }
                case SCANLOCK -> { if (txn != null) rb.setScanLock(txn.kvScanLock(req.getScanLock())); }
                case RESOLVELOCK -> { if (txn != null) rb.setResolveLock(txn.kvResolveLock(req.getResolveLock())); }
                case GC -> { if (txn != null) rb.setGC(txn.kvGC(req.getGC())); }
                case DELETERANGE -> { if (txn != null) rb.setDeleteRange(txn.kvDeleteRange(req.getDeleteRange())); }
                case PESSIMISTICLOCK -> {
                    if (txn != null) rb.setPessimisticLock(
                            txn.kvPessimisticLock(req.getPessimisticLock()));
                }
                case PESSIMISTICROLLBACK -> {
                    if (txn != null) rb.setPessimisticRollback(
                            txn.kvPessimisticRollback(req.getPessimisticRollback()));
                }
                case CHECKTXNSTATUS -> {
                    if (txn != null) rb.setCheckTxnStatus(
                            txn.kvCheckTxnStatus(req.getCheckTxnStatus()));
                }
                case CHECKSECONDARYLOCKS -> {
                    if (txn != null) rb.setCheckSecondaryLocks(
                            txn.kvCheckSecondaryLocks(req.getCheckSecondaryLocks()));
                }
                case TXNHEARTBEAT -> { if (txn != null) rb.setTxnHeartBeat(txn.kvTxnHeartBeat(req.getTxnHeartBeat())); }

                // Raw KV
                case RAWGET -> { if (rawKv != null) rb.setRawGet(rawKv.rawGet(req.getRawGet())); }
                case RAWBATCHGET -> { if (rawKv != null) rb.setRawBatchGet(rawKv.rawBatchGet(req.getRawBatchGet())); }
                case RAWPUT -> { if (rawKv != null) rb.setRawPut(rawKv.rawPut(req.getRawPut())); }
                case RAWBATCHPUT -> { if (rawKv != null) rb.setRawBatchPut(rawKv.rawBatchPut(req.getRawBatchPut())); }
                case RAWDELETE -> { if (rawKv != null) rb.setRawDelete(rawKv.rawDelete(req.getRawDelete())); }
                case RAWBATCHDELETE -> {
                    if (rawKv != null) rb.setRawBatchDelete(
                            rawKv.rawBatchDelete(req.getRawBatchDelete()));
                }
                case RAWSCAN -> { if (rawKv != null) rb.setRawScan(rawKv.rawScan(req.getRawScan())); }
                case RAWDELETERANGE -> {
                    if (rawKv != null) rb.setRawDeleteRange(
                            rawKv.rawDeleteRange(req.getRawDeleteRange()));
                }
                case RAWCAS -> { if (rawKv != null) rb.setRawCAS(rawKv.rawCAS(req.getRawCAS())); }

                // Coprocessor
                case COPROCESSOR -> { if (cop != null) rb.setCoprocessor(cop.handle(req.getCoprocessor())); }

                // Heartbeat
                case EMPTY -> rb.setEmpty(Tikvpb.Empty.getDefaultInstance());

                case CMD_NOT_SET -> { /* skip */ }
            }
        } catch (Throwable t) {
            log.warn("batchCommands sub-request {} failed: {}", req.getCmdCase(), t.getMessage());
        }
        return rb.build();
    }

    // ---- Backup / Restore ----

    @Override
    public void backup(io.github.xinfra.lab.xkv.proto.KvServerpb.BackupRequest req,
                       StreamObserver<io.github.xinfra.lab.xkv.proto.KvServerpb.BackupResponse> obs) {
        var bm = backupManager;
        if (bm == null) { unimpl(obs); return; }
        try {
            byte[] startKey = req.getStartKey().isEmpty() ? null : req.getStartKey().toByteArray();
            byte[] endKey = req.getEndKey().isEmpty() ? null : req.getEndKey().toByteArray();
            bm.backup(startKey, endKey, resp -> obs.onNext(resp));
            obs.onCompleted();
        } catch (Throwable t) {
            obs.onError(Status.INTERNAL.withDescription(t.getMessage()).asRuntimeException());
        }
    }

    @Override
    public void restore(io.github.xinfra.lab.xkv.proto.KvServerpb.RestoreRequest req,
                        StreamObserver<io.github.xinfra.lab.xkv.proto.KvServerpb.RestoreResponse> obs) {
        var rm = restoreManager;
        if (rm == null) { unimpl(obs); return; }
        try {
            rm.restore(req.getSstsList());
            obs.onNext(io.github.xinfra.lab.xkv.proto.KvServerpb.RestoreResponse.newBuilder().build());
            obs.onCompleted();
        } catch (Throwable t) {
            obs.onError(Status.INTERNAL.withDescription(t.getMessage()).asRuntimeException());
        }
    }

    private static void unimpl(StreamObserver<?> o) {
        o.onError(Status.UNIMPLEMENTED.withDescription("phase 0 stub").asRuntimeException());
    }

    record CopRegionCheck(io.github.xinfra.lab.xkv.proto.Errorpb.Error error,
                          Coprocessor.Request request) {}

    private CopRegionCheck validateAndClipCopRegion(Coprocessor.Request r) {
        if (splitLocator == null) return new CopRegionCheck(null, r);
        if (r.getRangesCount() == 0) return new CopRegionCheck(null, r);
        byte[] key = r.getRanges(0).getStart().toByteArray();
        if (key.length == 0) return new CopRegionCheck(null, r);
        var peer = splitLocator.peerForKey(key);
        if (peer == null) {
            return new CopRegionCheck(
                    io.github.xinfra.lab.xkv.proto.Errorpb.Error.newBuilder()
                            .setMessage("region not found").build(), null);
        }
        var region = peer.region();
        if (r.hasContext() && r.getContext().hasRegionEpoch()) {
            var reqEpoch = r.getContext().getRegionEpoch();
            var liveEpoch = region.getRegionEpoch();
            if (reqEpoch.getVersion() != liveEpoch.getVersion()
                    || reqEpoch.getConfVer() != liveEpoch.getConfVer()) {
                return new CopRegionCheck(
                        io.github.xinfra.lab.xkv.proto.Errorpb.Error.newBuilder()
                                .setMessage("epoch not match")
                                .setEpochNotMatch(io.github.xinfra.lab.xkv.proto.Errorpb.EpochNotMatch.newBuilder()
                                        .addCurrentRegions(region))
                                .build(), null);
            }
        }

        byte[] regionStart = region.getStartKey().toByteArray();
        byte[] regionEnd = region.getEndKey().toByteArray();

        List<Coprocessor.KeyRange> clipped = new ArrayList<>();
        for (Coprocessor.KeyRange range : r.getRangesList()) {
            byte[] s = range.getStart().toByteArray();
            byte[] e = range.getEnd().toByteArray();
            if (regionStart.length > 0 && compareBytes(s, regionStart) < 0) {
                s = regionStart;
            }
            if (regionEnd.length > 0 && (e.length == 0 || compareBytes(e, regionEnd) > 0)) {
                e = regionEnd;
            }
            if (e.length > 0 && compareBytes(s, e) >= 0) {
                continue;
            }
            clipped.add(Coprocessor.KeyRange.newBuilder()
                    .setStart(ByteString.copyFrom(s))
                    .setEnd(ByteString.copyFrom(e))
                    .build());
        }

        if (clipped.isEmpty()) {
            return new CopRegionCheck(
                    io.github.xinfra.lab.xkv.proto.Errorpb.Error.newBuilder()
                            .setMessage("key not in region")
                            .setKeyNotInRegion(io.github.xinfra.lab.xkv.proto.Errorpb.KeyNotInRegion.newBuilder()
                                    .setRegionId(region.getId())
                                    .setStartKey(region.getStartKey())
                                    .setEndKey(region.getEndKey()))
                            .build(), null);
        }

        if (clipped.size() == r.getRangesCount()) {
            boolean unchanged = true;
            for (int i = 0; i < clipped.size(); i++) {
                var orig = r.getRanges(i);
                var clip = clipped.get(i);
                if (!orig.getStart().equals(clip.getStart())
                        || !orig.getEnd().equals(clip.getEnd())) {
                    unchanged = false;
                    break;
                }
            }
            if (unchanged) return new CopRegionCheck(null, r);
        }

        var clippedReq = r.toBuilder().clearRanges().addAllRanges(clipped).build();
        return new CopRegionCheck(null, clippedReq);
    }

    static int compareBytes(byte[] a, byte[] b) {
        int len = Math.min(a.length, b.length);
        for (int i = 0; i < len; i++) {
            int cmp = (a[i] & 0xFF) - (b[i] & 0xFF);
            if (cmp != 0) return cmp;
        }
        return a.length - b.length;
    }

    private <T> void dispatchRaw(StreamObserver<T> o, java.util.function.Supplier<T> work) {
        if (rawKv == null) { unimpl(o); return; }
        try {
            o.onNext(work.get());
            o.onCompleted();
        } catch (Throwable t) {
            o.onError(Status.INTERNAL.withDescription(t.getMessage()).asRuntimeException());
        }
    }

    private <T> void dispatchTxn(StreamObserver<T> o, java.util.function.Supplier<T> work) {
        if (txn == null) { unimpl(o); return; }
        try {
            o.onNext(work.get());
            o.onCompleted();
        } catch (Throwable t) {
            o.onError(Status.INTERNAL.withDescription(t.getMessage()).asRuntimeException());
        }
    }

    // ---- Priority queue helpers ----

    private static final AtomicLong TASK_SEQ = new AtomicLong();

    private static int extractPriority(Coprocessor.Request r) {
        if (r.hasContext()) {
            return r.getContext().getPriorityValue();
        }
        return 0;
    }

    private static PrioritizedTask prioritized(int priority, Runnable task) {
        return new PrioritizedTask(priority, TASK_SEQ.getAndIncrement(), task);
    }

    static final class PrioritizedTask implements Comparable<PrioritizedTask>, Runnable {
        private final int priority;
        private final long seqNo;
        private final Runnable task;

        PrioritizedTask(int priority, long seqNo, Runnable task) {
            this.priority = priority;
            this.seqNo = seqNo;
            this.task = task;
        }

        @Override
        public void run() {
            try {
                task.run();
            } finally {
                CopDatumComparator.clearCollation();
            }
        }

        @Override
        public int compareTo(PrioritizedTask other) {
            // High(2) before Normal(0) before Low(1)
            int cmp = Integer.compare(toOrdinal(other.priority), toOrdinal(this.priority));
            if (cmp != 0) return cmp;
            return Long.compare(this.seqNo, other.seqNo);
        }

        private static int toOrdinal(int commandPri) {
            // CommandPri: Normal=0, Low=1, High=2
            // Desired order: High first, Normal second, Low last
            return switch (commandPri) {
                case 2 -> 2;  // High
                case 0 -> 1;  // Normal
                case 1 -> 0;  // Low
                default -> 1; // treat unknown as Normal
            };
        }
    }

    private static final class NoopRequestObserver<T> implements StreamObserver<T> {
        @Override public void onNext(T value) {}
        @Override public void onError(Throwable t) {}
        @Override public void onCompleted() {}
    }
}
