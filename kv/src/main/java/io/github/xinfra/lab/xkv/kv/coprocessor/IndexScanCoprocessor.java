package io.github.xinfra.lab.xkv.kv.coprocessor;

import com.google.protobuf.ByteString;
import io.github.xinfra.lab.xkv.kv.coprocessor.dag.CopChunk;
import io.github.xinfra.lab.xkv.kv.coprocessor.dag.CopRecord;
import io.github.xinfra.lab.xkv.kv.coprocessor.dag.VecIndexLookupOp;
import io.github.xinfra.lab.xkv.kv.coprocessor.dag.VecIndexScanOp;
import io.github.xinfra.lab.xkv.kv.coprocessor.dag.VecLimitOp;
import io.github.xinfra.lab.xkv.kv.coprocessor.dag.VecOperator;
import io.github.xinfra.lab.xkv.kv.coprocessor.dag.VecSelectionOp;
import io.github.xinfra.lab.xkv.kv.coprocessor.dag.VecTopNOp;
import io.github.xinfra.lab.xkv.kv.engine.StorageEngine;
import io.github.xinfra.lab.xkv.kv.mvcc.MvccReader;
import io.github.xinfra.lab.xkv.proto.Coprocessor.Request;
import io.github.xinfra.lab.xkv.proto.Coprocessor.Response;
import io.github.xinfra.lab.xkv.proto.Coprocessor.StreamResponse;
import io.github.xinfra.lab.xkv.proto.Tipb;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.function.Consumer;

/**
 * Index scan coprocessor (tp=3).
 *
 * <p>Two modes:
 * <ul>
 *   <li><b>Covering index</b> ({@code index_lookup=false}): scans index entries
 *       and returns index column values directly. No double-read.</li>
 *   <li><b>Index lookup</b> ({@code index_lookup=true}): scans index entries,
 *       extracts handles, does point lookups for full row data.</li>
 * </ul>
 */
public final class IndexScanCoprocessor implements Coprocessor {
    private static final Logger log = LoggerFactory.getLogger(IndexScanCoprocessor.class);

    private static final int DEFAULT_SCAN_LIMIT = 10_000;
    private static final int DEFAULT_SCAN_BATCH_SIZE = 1024;
    private static final int STREAM_CHUNK_SIZE = 256;

    private final StorageEngine engine;
    private final int scanBatchSize;

    public IndexScanCoprocessor(StorageEngine engine) {
        this(engine, DEFAULT_SCAN_BATCH_SIZE);
    }

    IndexScanCoprocessor(StorageEngine engine, int scanBatchSize) {
        this.engine = engine;
        this.scanBatchSize = scanBatchSize;
    }

    @Override
    public int requestType() {
        return 3;
    }

    @Override
    public Response handle(Request req) {
        long startTs = req.getStartTs();
        try (var snapshot = engine.newSnapshot();
             var reader = new MvccReader(engine, snapshot, false)) {

            Tipb.DAGRequest dagReq = Tipb.DAGRequest.parseFrom(req.getData().toByteArray());
            int limit = req.getPagingSize() > 0 ? (int) req.getPagingSize() : DEFAULT_SCAN_LIMIT;
            boolean hasTopN = dagReq.getTopnLimit() > 0;

            VecOperator pipeline = buildVecPipeline(reader, dagReq, req, startTs);

            if (hasTopN) {
                pipeline = new VecTopNOp(pipeline, dagReq.getOrderByList(),
                        dagReq.getTopnLimit(), dagReq.getTopnOffset());
            } else {
                pipeline = new VecLimitOp(pipeline, limit);
            }

            pipeline.open();
            try {
                return drainToKvPairResponse(pipeline);
            } finally {
                pipeline.close();
            }
        } catch (Throwable t) {
            log.warn("IndexScanCoprocessor error", t);
            return Response.newBuilder().setOtherError(t.getMessage()).build();
        }
    }

    @Override
    public void handleStream(Request req, Consumer<StreamResponse> sink) {
        long startTs = req.getStartTs();
        try (var snapshot = engine.newSnapshot();
             var reader = new MvccReader(engine, snapshot, false)) {

            Tipb.DAGRequest dagReq = Tipb.DAGRequest.parseFrom(req.getData().toByteArray());
            boolean hasTopN = dagReq.getTopnLimit() > 0;
            int pagingSize = req.getPagingSize() > 0 ? (int) req.getPagingSize() : Integer.MAX_VALUE;

            VecOperator pipeline = buildVecPipeline(reader, dagReq, req, startTs);

            if (hasTopN) {
                pipeline = new VecTopNOp(pipeline, dagReq.getOrderByList(),
                        dagReq.getTopnLimit(), dagReq.getTopnOffset());
                pipeline.open();
                try {
                    Response resp = drainToKvPairResponse(pipeline);
                    sink.accept(StreamResponse.newBuilder().setData(resp.getData()).build());
                } finally {
                    pipeline.close();
                }
            } else {
                pipeline = new VecLimitOp(pipeline, pagingSize);
                pipeline.open();
                try {
                    streamKvPairChunks(pipeline, sink);
                } finally {
                    pipeline.close();
                }
            }
        } catch (Throwable t) {
            log.warn("IndexScanCoprocessor stream error", t);
            sink.accept(StreamResponse.newBuilder().setOtherError(t.getMessage()).build());
        }
    }

    private VecOperator buildVecPipeline(MvccReader reader, Tipb.DAGRequest dagReq,
                                          Request req, long startTs) {
        VecOperator pipeline = new VecIndexScanOp(reader, dagReq, req.getRangesList(), startTs);

        if (dagReq.getIndexLookup()) {
            pipeline = new VecIndexLookupOp(pipeline, reader, dagReq, startTs);
        }

        if (dagReq.getConditionsCount() > 0) {
            pipeline = new VecSelectionOp(pipeline, dagReq.getConditionsList());
        }

        return pipeline;
    }

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
        int chunkCount = 0;
        CopChunk chunk;
        while ((chunk = pipeline.nextChunk(STREAM_CHUNK_SIZE)) != null) {
            for (int i = 0; i < chunk.size(); i++) {
                CopRecord record = chunk.get(i);
                encoder.add(record.key(), record.value());
                chunkCount++;
                if (chunkCount >= STREAM_CHUNK_SIZE) {
                    emitChunk(encoder, sink);
                    encoder = new TableScanCoprocessor.KvPairEncoder();
                    chunkCount = 0;
                }
            }
        }
        if (chunkCount > 0) {
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
}
