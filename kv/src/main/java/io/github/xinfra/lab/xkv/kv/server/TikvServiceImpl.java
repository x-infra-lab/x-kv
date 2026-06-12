package io.github.xinfra.lab.xkv.kv.server;

import io.github.xinfra.lab.xkv.proto.Coprocessor;
import io.github.xinfra.lab.xkv.proto.Kvrpcpb.*;
import io.github.xinfra.lab.xkv.proto.TikvGrpc;
import io.github.xinfra.lab.xkv.proto.Tikvpb;
import io.github.xinfra.lab.xkv.proto.Tikvpb.*;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicLong;

public final class TikvServiceImpl extends TikvGrpc.TikvImplBase {
    private static final Logger log = LoggerFactory.getLogger(TikvServiceImpl.class);

    private final RawKvService rawKv;
    private final TransactionService txn;
    private final CoprocessorService cop;
    private final io.github.xinfra.lab.xkv.kv.store.SplitDriver splitDriver;
    private final RawKvService.PeerLocator splitLocator;

    private final AtomicLong batchPending = new AtomicLong();

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
        try {
            o.onNext(cop.handle(r));
            o.onCompleted();
        } catch (Throwable t) {
            o.onError(Status.INTERNAL.withDescription(t.getMessage()).asRuntimeException());
        }
    }

    @Override
    public void coprocessorStream(Coprocessor.Request r, StreamObserver<Coprocessor.StreamResponse> o) {
        if (cop == null) { unimpl(o); return; }
        try {
            cop.handleStream(r, o::onNext);
            o.onCompleted();
        } catch (Throwable t) {
            o.onError(Status.INTERNAL.withDescription(t.getMessage()).asRuntimeException());
        }
    }

    @Override
    public void batchCoprocessor(Coprocessor.BatchRequest r, StreamObserver<Coprocessor.BatchResponse> o) {
        if (cop == null) { unimpl(o); return; }
        try {
            cop.handleBatch(r, o::onNext);
            o.onCompleted();
        } catch (Throwable t) {
            o.onError(Status.INTERNAL.withDescription(t.getMessage()).asRuntimeException());
        }
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

    private static void unimpl(StreamObserver<?> o) {
        o.onError(Status.UNIMPLEMENTED.withDescription("phase 0 stub").asRuntimeException());
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

    private static final class NoopRequestObserver<T> implements StreamObserver<T> {
        @Override public void onNext(T value) {}
        @Override public void onError(Throwable t) {}
        @Override public void onCompleted() {}
    }
}
