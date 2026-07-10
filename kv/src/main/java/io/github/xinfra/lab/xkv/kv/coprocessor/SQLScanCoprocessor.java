package io.github.xinfra.lab.xkv.kv.coprocessor;

import com.google.protobuf.ByteString;
import io.github.xinfra.lab.xkv.kv.coprocessor.dag.CopChunk;
import io.github.xinfra.lab.xkv.kv.coprocessor.dag.CopMemTracker;
import io.github.xinfra.lab.xkv.kv.coprocessor.dag.CopRecord;
import io.github.xinfra.lab.xkv.kv.coprocessor.dag.VecDeadlineOp;
import io.github.xinfra.lab.xkv.kv.coprocessor.dag.VecLimitOp;
import io.github.xinfra.lab.xkv.kv.coprocessor.dag.VecOperator;
import io.github.xinfra.lab.xkv.kv.coprocessor.dag.VecProjectionOp;
import io.github.xinfra.lab.xkv.kv.coprocessor.dag.VecSelectionOp;
import io.github.xinfra.lab.xkv.kv.coprocessor.dag.VecTableScanOp;
import io.github.xinfra.lab.xkv.kv.coprocessor.dag.VecTopNOp;
import io.github.xinfra.lab.xkv.kv.coprocessor.eval.CopAggFunction;
import io.github.xinfra.lab.xkv.kv.coprocessor.eval.CopDatum;
import io.github.xinfra.lab.xkv.kv.coprocessor.eval.CopDatumComparator;
import io.github.xinfra.lab.xkv.kv.coprocessor.eval.CopRow;
import io.github.xinfra.lab.xkv.kv.coprocessor.eval.ExprEvaluator;
import io.github.xinfra.lab.xkv.kv.engine.StorageEngine;
import io.github.xinfra.lab.xkv.kv.mvcc.KeyLockedException;
import io.github.xinfra.lab.xkv.kv.mvcc.MvccReader;
import io.github.xinfra.lab.xkv.proto.Coprocessor.Request;
import io.github.xinfra.lab.xkv.proto.Coprocessor.Response;
import io.github.xinfra.lab.xkv.proto.Coprocessor.StreamResponse;
import io.github.xinfra.lab.xkv.proto.Kvrpcpb;
import io.github.xinfra.lab.xkv.proto.Tipb;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

public final class SQLScanCoprocessor implements Coprocessor {
    private static final Logger log = LoggerFactory.getLogger(SQLScanCoprocessor.class);

    private static final int DEFAULT_SCAN_LIMIT = 10_000;
    private static final int DEFAULT_SCAN_BATCH_SIZE = 1024;
    private static final int STREAM_CHUNK_SIZE = 256;
    private static final int MAX_STREAM_CHUNKS = 4096;
    private static final int MAX_AGG_GROUPS = 100_000;

    private final StorageEngine engine;
    final int scanBatchSize;
    final int streamChunkSize;

    public SQLScanCoprocessor(StorageEngine engine) {
        this(engine, DEFAULT_SCAN_BATCH_SIZE, STREAM_CHUNK_SIZE);
    }

    SQLScanCoprocessor(StorageEngine engine, int scanBatchSize) {
        this(engine, scanBatchSize, STREAM_CHUNK_SIZE);
    }

    SQLScanCoprocessor(StorageEngine engine, int scanBatchSize, int streamChunkSize) {
        this.engine = engine;
        this.scanBatchSize = scanBatchSize;
        this.streamChunkSize = streamChunkSize;
    }

    @Override
    public int requestType() {
        return 1;
    }

    @Override
    public Response handle(Request req) {
        long startTs = req.getStartTs();
        long deadlineMs = extractDeadlineMs(req);

        try (var snapshot = engine.newSnapshot();
             var reader = new MvccReader(engine, snapshot, false)) {

            Tipb.DAGRequest dagReq = Tipb.DAGRequest.parseFrom(req.getData().toByteArray());
            CopDatumComparator.setCollation(dagReq.getCollation());
            var memTracker = new CopMemTracker();
            int limit = req.getPagingSize() > 0 ? (int) req.getPagingSize() : DEFAULT_SCAN_LIMIT;
            boolean hasAgg = dagReq.getAggFuncsCount() > 0;
            boolean hasTopN = dagReq.getTopnLimit() > 0;

            PipelineResult pr = buildVecPipeline(reader, dagReq, req, startTs);
            VecOperator pipeline = pr.pipeline();

            if (hasAgg) {
                pipeline = wrapDeadline(pipeline, deadlineMs);
                pipeline.open();
                try {
                    Response resp = executeAgg(pipeline, dagReq, memTracker);
                    return withExecDetails(withLockError(resp, pr.scanOp().lockError()), pipeline);
                } finally {
                    pipeline.close();
                }
            }

            if (hasTopN) {
                pipeline = new VecTopNOp(pipeline, dagReq.getOrderByList(),
                        dagReq.getTopnLimit(), dagReq.getTopnOffset(), memTracker);
            } else {
                pipeline = new VecLimitOp(pipeline, limit);
            }

            pipeline = wrapDeadline(pipeline, deadlineMs);
            pipeline.open();
            try {
                Response resp = drainToKvPairResponse(pipeline);
                return withExecDetails(withLockError(resp, pr.scanOp().lockError()), pipeline);
            } finally {
                pipeline.close();
            }
        } catch (Throwable t) {
            log.warn("SQLScanCoprocessor error", t);
            return Response.newBuilder()
                    .setOtherError(t.getMessage())
                    .build();
        } finally {
            CopDatumComparator.clearCollation();
        }
    }

    @Override
    public void handleStream(Request req, Consumer<StreamResponse> sink) {
        long startTs = req.getStartTs();
        long deadlineMs = extractDeadlineMs(req);

        try (var snapshot = engine.newSnapshot();
             var reader = new MvccReader(engine, snapshot, false)) {

            Tipb.DAGRequest dagReq = Tipb.DAGRequest.parseFrom(req.getData().toByteArray());
            CopDatumComparator.setCollation(dagReq.getCollation());
            var memTracker = new CopMemTracker();
            boolean hasAgg = dagReq.getAggFuncsCount() > 0;
            boolean hasTopN = dagReq.getTopnLimit() > 0;
            int pagingSize = req.getPagingSize() > 0 ? (int) req.getPagingSize() : Integer.MAX_VALUE;

            PipelineResult pr = buildVecPipeline(reader, dagReq, req, startTs);
            VecOperator pipeline = pr.pipeline();

            if (hasAgg) {
                pipeline = wrapDeadline(pipeline, deadlineMs);
                pipeline.open();
                try {
                    Response resp = executeAgg(pipeline, dagReq, memTracker);
                    sink.accept(StreamResponse.newBuilder().setData(resp.getData()).build());
                    emitLockErrorStream(pr.scanOp().lockError(), sink);
                } finally {
                    pipeline.close();
                }
            } else if (hasTopN) {
                pipeline = new VecTopNOp(pipeline, dagReq.getOrderByList(),
                        dagReq.getTopnLimit(), dagReq.getTopnOffset(), memTracker);
                pipeline = wrapDeadline(pipeline, deadlineMs);
                pipeline.open();
                try {
                    Response resp = drainToKvPairResponse(pipeline);
                    sink.accept(StreamResponse.newBuilder().setData(resp.getData()).build());
                    emitLockErrorStream(pr.scanOp().lockError(), sink);
                } finally {
                    pipeline.close();
                }
            } else {
                pipeline = new VecLimitOp(pipeline, pagingSize);
                pipeline = wrapDeadline(pipeline, deadlineMs);
                pipeline.open();
                try {
                    streamKvPairChunks(pipeline, sink);
                    emitLockErrorStream(pr.scanOp().lockError(), sink);
                } finally {
                    pipeline.close();
                }
            }
        } catch (Throwable t) {
            log.warn("SQLScanCoprocessor stream error", t);
            sink.accept(StreamResponse.newBuilder()
                    .setOtherError(t.getMessage())
                    .build());
        } finally {
            CopDatumComparator.clearCollation();
        }
    }

    // --- Pipeline assembly ---

    record PipelineResult(VecOperator pipeline, VecTableScanOp scanOp) {}

    private PipelineResult buildVecPipeline(MvccReader reader, Tipb.DAGRequest dagReq,
                                             Request req, long startTs) {
        var scan = new VecTableScanOp(reader, dagReq, req.getRangesList(), startTs);
        VecOperator pipeline = scan;
        if (dagReq.getConditionsCount() > 0) {
            pipeline = new VecSelectionOp(pipeline, dagReq.getConditionsList());
        }
        if (dagReq.getProjectionsCount() > 0) {
            pipeline = new VecProjectionOp(pipeline, dagReq.getProjectionsList());
        }
        return new PipelineResult(pipeline, scan);
    }

    // --- Response builders ---

    private Response drainToKvPairResponse(VecOperator pipeline) {
        var encoder = new TableScanCoprocessor.KvPairEncoder();
        CopChunk chunk;
        while ((chunk = pipeline.nextChunk(scanBatchSize)) != null) {
            for (int i = 0; i < chunk.size(); i++) {
                CopRecord record = chunk.get(i);
                encoder.add(record.key(), record.value());
            }
        }
        byte[] kvData = encoder.encode();
        Tipb.SelectResponse selectResp = Tipb.SelectResponse.newBuilder()
                .setKvPairData(ByteString.copyFrom(kvData))
                .setIsAgg(false)
                .build();
        return Response.newBuilder()
                .setData(ByteString.copyFrom(selectResp.toByteArray()))
                .build();
    }

    private void streamKvPairChunks(VecOperator pipeline, Consumer<StreamResponse> sink) {
        var encoder = new TableScanCoprocessor.KvPairEncoder();
        int recordsInBatch = 0;
        int chunksEmitted = 0;
        CopChunk chunk;
        while ((chunk = pipeline.nextChunk(streamChunkSize)) != null) {
            for (int i = 0; i < chunk.size(); i++) {
                CopRecord record = chunk.get(i);
                encoder.add(record.key(), record.value());
                recordsInBatch++;
                if (recordsInBatch >= streamChunkSize) {
                    emitChunk(encoder, sink);
                    encoder = new TableScanCoprocessor.KvPairEncoder();
                    recordsInBatch = 0;
                    chunksEmitted++;
                    if (chunksEmitted >= MAX_STREAM_CHUNKS) {
                        log.warn("stream truncated at {} chunks", MAX_STREAM_CHUNKS);
                        return;
                    }
                }
            }
        }
        if (recordsInBatch > 0) {
            emitChunk(encoder, sink);
        }
    }

    private void emitChunk(TableScanCoprocessor.KvPairEncoder encoder,
                            Consumer<StreamResponse> sink) {
        byte[] kvData = encoder.encode();
        Tipb.SelectResponse selectResp = Tipb.SelectResponse.newBuilder()
                .setKvPairData(ByteString.copyFrom(kvData))
                .setIsAgg(false)
                .build();
        sink.accept(StreamResponse.newBuilder()
                .setData(ByteString.copyFrom(selectResp.toByteArray()))
                .build());
    }

    // --- Aggregation ---

    private Response executeAgg(VecOperator pipeline, Tipb.DAGRequest dagReq,
                                CopMemTracker memTracker) {
        Map<GroupKey, CopAggFunction[]> groups = new HashMap<>();
        List<Tipb.Expr> groupByExprs = dagReq.getGroupByExprsList();
        List<Tipb.AggFuncDesc> aggDescs = dagReq.getAggFuncsList();
        int estGroupBytes = 256;

        CopChunk chunk;
        while ((chunk = pipeline.nextChunk(scanBatchSize)) != null) {
            for (int ci = 0; ci < chunk.size(); ci++) {
                CopRecord record = chunk.get(ci);
                CopRow row = record.row();
                GroupKey gk = computeGroupKey(row, groupByExprs);
                int prevSize = groups.size();
                CopAggFunction[] aggs = groups.computeIfAbsent(gk,
                        k -> createAggFunctions(aggDescs));

                if (groups.size() > prevSize) {
                    memTracker.track(estGroupBytes);
                }

                if (groups.size() > MAX_AGG_GROUPS) {
                    throw new IllegalStateException(
                            "aggregation exceeded max group count: " + MAX_AGG_GROUPS);
                }

                for (int i = 0; i < aggs.length; i++) {
                    Tipb.AggFuncDesc desc = aggDescs.get(i);
                    CopDatum value = desc.hasArg()
                            ? ExprEvaluator.eval(desc.getArg(), row)
                            : CopDatum.of(1L);
                    aggs[i].update(value);
                }
            }
        }

        return buildAggResponse(groups, groupByExprs, aggDescs);
    }

    private Response buildAggResponse(Map<GroupKey, CopAggFunction[]> groups,
                                       List<Tipb.Expr> groupByExprs,
                                       List<Tipb.AggFuncDesc> aggDescs) {
        Tipb.SelectResponse.Builder respBuilder = Tipb.SelectResponse.newBuilder().setIsAgg(true);

        if (groups.isEmpty() && groupByExprs.isEmpty()) {
            CopAggFunction[] emptyAggs = createAggFunctions(aggDescs);
            respBuilder.addAggGroups(buildAggGroupResult(List.of(), emptyAggs, aggDescs));
        } else {
            for (var entry : groups.entrySet()) {
                respBuilder.addAggGroups(
                        buildAggGroupResult(entry.getKey().keys, entry.getValue(), aggDescs));
            }
        }

        return Response.newBuilder()
                .setData(ByteString.copyFrom(respBuilder.build().toByteArray()))
                .build();
    }

    private CopAggFunction[] createAggFunctions(List<Tipb.AggFuncDesc> descs) {
        CopAggFunction[] result = new CopAggFunction[descs.size()];
        for (int i = 0; i < descs.size(); i++) {
            result[i] = CopAggFunction.create(descs.get(i).getAggType(), descs.get(i).getDistinct());
        }
        return result;
    }

    private Tipb.AggGroupResult buildAggGroupResult(List<CopDatum> groupKeys,
                                                     CopAggFunction[] aggs,
                                                     List<Tipb.AggFuncDesc> aggDescs) {
        Tipb.AggGroupResult.Builder b = Tipb.AggGroupResult.newBuilder();
        for (CopDatum key : groupKeys) {
            b.addGroupKeys(ExprEvaluator.toProto(key));
        }
        for (int i = 0; i < aggs.length; i++) {
            b.addPartialStates(extractPartialState(aggs[i], aggDescs.get(i)));
        }
        return b.build();
    }

    private Tipb.PartialAggState extractPartialState(CopAggFunction agg, Tipb.AggFuncDesc desc) {
        Tipb.PartialAggState.Builder b = Tipb.PartialAggState.newBuilder()
                .setAggType(desc.getAggType())
                .setDistinct(desc.getDistinct());

        int type = agg.type();
        switch (type) {
            case 0 -> b.addState(ExprEvaluator.toProto(CopDatum.of(agg.partialCount())));
            case 1 -> b.addState(ExprEvaluator.toProto(agg.result()));
            case 2 -> {
                b.addState(ExprEvaluator.toProto(CopDatum.of(agg.partialCount())));
                b.addState(agg.partialSum() != null
                        ? ExprEvaluator.toProto(CopDatum.of(agg.partialSum()))
                        : ExprEvaluator.toProto(CopDatum.nil()));
            }
            case 3, 4, 5 -> b.addState(ExprEvaluator.toProto(agg.result()));
        }
        return b.build();
    }

    private GroupKey computeGroupKey(CopRow row, List<Tipb.Expr> groupByExprs) {
        if (groupByExprs == null || groupByExprs.isEmpty()) {
            return GroupKey.EMPTY;
        }
        List<CopDatum> keys = new ArrayList<>(groupByExprs.size());
        for (Tipb.Expr expr : groupByExprs) {
            keys.add(ExprEvaluator.eval(expr, row));
        }
        return new GroupKey(keys);
    }

    // --- Deadline & exec details helpers ---

    static long extractDeadlineMs(Request req) {
        if (req.hasContext() && req.getContext().getMaxExecutionDurationMs() > 0) {
            return req.getContext().getMaxExecutionDurationMs();
        }
        return 0;
    }

    static VecOperator wrapDeadline(VecOperator pipeline, long deadlineMs) {
        if (deadlineMs <= 0) return pipeline;
        return new VecDeadlineOp(pipeline, deadlineMs);
    }

    private static Response withExecDetails(Response resp, VecOperator pipeline) {
        if (pipeline instanceof VecDeadlineOp dl) {
            return resp.toBuilder().setExecDetailsMs(dl.elapsedMs()).build();
        }
        return resp;
    }

    // --- Lock error helpers ---

    private static Response withLockError(Response resp, KeyLockedException lockErr) {
        if (lockErr == null) return resp;
        return resp.toBuilder()
                .setLocked(Kvrpcpb.KeyError.newBuilder()
                        .setLocked(toLockInfo(lockErr)))
                .build();
    }

    private static void emitLockErrorStream(KeyLockedException lockErr,
                                             Consumer<StreamResponse> sink) {
        if (lockErr == null) return;
        sink.accept(StreamResponse.newBuilder()
                .setLocked(Kvrpcpb.KeyError.newBuilder()
                        .setLocked(toLockInfo(lockErr)))
                .build());
    }

    static Kvrpcpb.LockInfo toLockInfo(KeyLockedException e) {
        var lock = e.lock();
        var b = Kvrpcpb.LockInfo.newBuilder()
                .setPrimaryLock(ByteString.copyFrom(lock.primary()))
                .setLockVersion(lock.startTs())
                .setKey(ByteString.copyFrom(e.key()))
                .setLockTtl(lock.ttlMs())
                .setTxnSize(lock.txnSize())
                .setMinCommitTs(lock.minCommitTs())
                .setUseAsyncCommit(lock.useAsyncCommit())
                .setLockForUpdateTs(lock.forUpdateTs());
        switch (lock.type()) {
            case PUT -> b.setLockType(Kvrpcpb.Op.Put);
            case DELETE -> b.setLockType(Kvrpcpb.Op.Del);
            case LOCK -> b.setLockType(Kvrpcpb.Op.Lock);
            case PESSIMISTIC -> b.setLockType(Kvrpcpb.Op.PessimisticLock);
        }
        return b.build();
    }

    // --- Inner classes ---

    static final class GroupKey {
        static final GroupKey EMPTY = new GroupKey(List.of());

        final List<CopDatum> keys;

        GroupKey(List<CopDatum> keys) {
            this.keys = keys;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (!(o instanceof GroupKey other)) return false;
            if (keys.size() != other.keys.size()) return false;
            for (int i = 0; i < keys.size(); i++) {
                CopDatum a = keys.get(i);
                CopDatum b = other.keys.get(i);
                if (a.isNull() && b.isNull()) continue;
                if (a.isNull() || b.isNull()) return false;
                if (CopDatumComparator.compare(a, b) != 0) return false;
            }
            return true;
        }

        @Override
        public int hashCode() {
            int h = 1;
            for (CopDatum d : keys) {
                h = 31 * h + (d.isNull() ? 0 : d.toStringValue().hashCode());
            }
            return h;
        }
    }
}
