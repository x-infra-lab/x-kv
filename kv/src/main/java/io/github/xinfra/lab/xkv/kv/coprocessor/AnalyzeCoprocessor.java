package io.github.xinfra.lab.xkv.kv.coprocessor;

import com.google.protobuf.ByteString;
import io.github.xinfra.lab.xkv.kv.coprocessor.eval.CopDatum;
import io.github.xinfra.lab.xkv.kv.coprocessor.eval.CopDatumComparator;
import io.github.xinfra.lab.xkv.kv.coprocessor.eval.CopRow;
import io.github.xinfra.lab.xkv.kv.coprocessor.eval.CopRowDecoder;
import io.github.xinfra.lab.xkv.kv.coprocessor.eval.ExprEvaluator;
import io.github.xinfra.lab.xkv.kv.engine.StorageEngine;
import io.github.xinfra.lab.xkv.kv.mvcc.MvccReader;
import io.github.xinfra.lab.xkv.proto.Coprocessor.Request;
import io.github.xinfra.lab.xkv.proto.Coprocessor.Response;
import io.github.xinfra.lab.xkv.proto.Coprocessor.StreamResponse;
import io.github.xinfra.lab.xkv.proto.Tipb;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Consumer;

public final class AnalyzeCoprocessor implements Coprocessor {
    private static final Logger log = LoggerFactory.getLogger(AnalyzeCoprocessor.class);

    private static final int DEFAULT_SAMPLE_SIZE = 10_000;
    private static final int SCAN_BATCH_SIZE = 1024;

    private final StorageEngine engine;

    public AnalyzeCoprocessor(StorageEngine engine) {
        this.engine = engine;
    }

    @Override
    public int requestType() {
        return 2;
    }

    @Override
    public Response handle(Request req) {
        long startTs = req.getStartTs();

        try (var snapshot = engine.newSnapshot();
             var reader = new MvccReader(engine, snapshot, false)) {

            Tipb.AnalyzeReq analyzeReq = Tipb.AnalyzeReq.parseFrom(req.getData().toByteArray());
            int sampleSize = analyzeReq.getSampleSize() > 0
                    ? analyzeReq.getSampleSize() : DEFAULT_SAMPLE_SIZE;

            Tipb.DAGRequest dagReq = buildDAGRequest(analyzeReq);
            int colCount = analyzeReq.getOutputColumnIndicesCount();

            ColumnAccumulator[] accumulators = new ColumnAccumulator[colCount];
            for (int i = 0; i < colCount; i++) {
                accumulators[i] = new ColumnAccumulator(sampleSize);
            }

            long rowCount = 0;
            for (var range : req.getRangesList()) {
                byte[] cursor = range.getStart().toByteArray();
                byte[] end = range.getEnd().toByteArray();

                while (cursor != null) {
                    MvccReader.ScanResult result = reader.scan(cursor, end, SCAN_BATCH_SIZE, startTs);

                    for (var pair : result.pairs()) {
                        CopRow row = CopRowDecoder.decode(pair.key(), pair.value(), dagReq);
                        rowCount++;

                        for (int c = 0; c < colCount; c++) {
                            accumulators[c].add(row.get(c), rowCount);
                        }
                    }

                    if (result.pairs().size() < SCAN_BATCH_SIZE) {
                        cursor = null;
                    } else {
                        byte[] lastKey = result.pairs().get(result.pairs().size() - 1).key();
                        cursor = nextKey(lastKey);
                    }
                }
            }

            Tipb.AnalyzeResult.Builder resultBuilder = Tipb.AnalyzeResult.newBuilder()
                    .setRowCount(rowCount);

            for (int c = 0; c < colCount; c++) {
                int colIdx = analyzeReq.getOutputColumnIndices(c);
                Tipb.ColumnInfo col = analyzeReq.getColumns(colIdx);
                resultBuilder.addColumnResults(accumulators[c].toProto(col.getColumnId()));
            }

            byte[] data = resultBuilder.build().toByteArray();
            return Response.newBuilder()
                    .setData(ByteString.copyFrom(data))
                    .build();

        } catch (Throwable t) {
            log.warn("AnalyzeCoprocessor error", t);
            return Response.newBuilder()
                    .setOtherError(t.getMessage())
                    .build();
        }
    }

    @Override
    public void handleStream(Request req, Consumer<StreamResponse> sink) {
        Response resp = handle(req);
        sink.accept(StreamResponse.newBuilder()
                .setData(resp.getData())
                .setOtherError(resp.getOtherError())
                .build());
    }

    private Tipb.DAGRequest buildDAGRequest(Tipb.AnalyzeReq analyzeReq) {
        return Tipb.DAGRequest.newBuilder()
                .setTableId(analyzeReq.getTableId())
                .addAllColumns(analyzeReq.getColumnsList())
                .addAllOutputColumnIndices(analyzeReq.getOutputColumnIndicesList())
                .build();
    }

    private static byte[] nextKey(byte[] key) {
        byte[] result = new byte[key.length + 1];
        System.arraycopy(key, 0, result, 0, key.length);
        return result;
    }

    static final class ColumnAccumulator {
        private final int sampleSize;
        private final Set<String> distinctValues = new HashSet<>();
        private final List<CopDatum> reservoir;
        private long nullCount;
        private long totalCount;
        private CopDatum min;
        private CopDatum max;

        ColumnAccumulator(int sampleSize) {
            this.sampleSize = sampleSize;
            this.reservoir = new ArrayList<>(Math.min(sampleSize, 1024));
        }

        void add(CopDatum value, long rowNum) {
            totalCount++;

            if (value.isNull()) {
                nullCount++;
                return;
            }

            distinctValues.add(value.toStringValue());

            if (min == null || CopDatumComparator.compare(value, min) < 0) {
                min = value;
            }
            if (max == null || CopDatumComparator.compare(value, max) > 0) {
                max = value;
            }

            // Reservoir sampling (Algorithm R)
            if (reservoir.size() < sampleSize) {
                reservoir.add(value);
            } else {
                long j = ThreadLocalRandom.current().nextLong(rowNum);
                if (j < sampleSize) {
                    reservoir.set((int) j, value);
                }
            }
        }

        Tipb.AnalyzeColumnResult toProto(long columnId) {
            Tipb.AnalyzeColumnResult.Builder b = Tipb.AnalyzeColumnResult.newBuilder()
                    .setColumnId(columnId)
                    .setNdv(distinctValues.size())
                    .setNullCount(nullCount)
                    .setTotalCount(totalCount);

            if (min != null) {
                b.setMinValue(ExprEvaluator.toProto(min));
            }
            if (max != null) {
                b.setMaxValue(ExprEvaluator.toProto(max));
            }

            reservoir.sort(CopDatumComparator::compare);
            for (CopDatum v : reservoir) {
                b.addSampleValues(ExprEvaluator.toProto(v));
            }

            return b.build();
        }
    }
}
