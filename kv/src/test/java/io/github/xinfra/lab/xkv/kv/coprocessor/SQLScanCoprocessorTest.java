package io.github.xinfra.lab.xkv.kv.coprocessor;

import com.google.protobuf.ByteString;
import io.github.xinfra.lab.xkv.kv.coprocessor.eval.CopDatum;
import io.github.xinfra.lab.xkv.kv.coprocessor.eval.CopKvPairDecoder;
import io.github.xinfra.lab.xkv.kv.coprocessor.eval.CopRowDecoder;
import io.github.xinfra.lab.xkv.kv.config.KvConfig;
import io.github.xinfra.lab.xkv.kv.engine.RocksStorageEngine;
import io.github.xinfra.lab.xkv.kv.engine.StorageEngine;
import io.github.xinfra.lab.xkv.kv.mvcc.MvccKey;
import io.github.xinfra.lab.xkv.kv.mvcc.Write;
import io.github.xinfra.lab.xkv.proto.Coprocessor;
import io.github.xinfra.lab.xkv.proto.Tipb;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class SQLScanCoprocessorTest {

    private static final long TABLE_ID = 100;
    private static final long START_TS = 10;
    private static final long COMMIT_TS = 20;
    private static final long READ_TS = 100;

    private static final int DT_BIGINT = 3;
    private static final int DT_VARCHAR = 8;

    @TempDir Path dataDir;
    private RocksStorageEngine engine;
    private SQLScanCoprocessor cop;

    private final List<Tipb.ColumnInfo> columns = List.of(
            Tipb.ColumnInfo.newBuilder().setColumnId(1).setDataType(DT_BIGINT).setAutoIncrement(true).setOffset(0).build(),
            Tipb.ColumnInfo.newBuilder().setColumnId(2).setDataType(DT_VARCHAR).setAutoIncrement(false).setOffset(1).build(),
            Tipb.ColumnInfo.newBuilder().setColumnId(3).setDataType(DT_BIGINT).setAutoIncrement(false).setOffset(2).build());

    private final List<Integer> outputIndices = List.of(0, 1, 2);

    @BeforeEach
    void setUp() throws Exception {
        engine = RocksStorageEngine.open(dataDir, KvConfig.EngineConfig.defaults());
        cop = new SQLScanCoprocessor(engine);
    }

    @AfterEach
    void tearDown() {
        if (engine != null) engine.close();
    }

    private void insertRow(long handle, String name, long age) {
        byte[] userKey = encodeRowKey(TABLE_ID, handle);
        byte[] rowValue = encodeRowValue(
                new long[]{2L, 3L},
                new Object[]{name, age});
        try (var batch = engine.newWriteBatch()) {
            batch.put(StorageEngine.Cf.DEFAULT,
                    MvccKey.encode(userKey, START_TS), rowValue);
            batch.put(StorageEngine.Cf.WRITE,
                    MvccKey.encode(userKey, COMMIT_TS),
                    Write.put(START_TS).encode());
            engine.write(batch, false);
        }
    }

    private Coprocessor.Request buildRequest(Tipb.DAGRequest dagReq) {
        byte[] data = dagReq.toByteArray();
        byte[] startKey = tableRecordPrefix(TABLE_ID);
        byte[] endKey = encodeRowKey(TABLE_ID, Long.MAX_VALUE);
        return Coprocessor.Request.newBuilder()
                .setTp(1)
                .setData(ByteString.copyFrom(data))
                .setStartTs(READ_TS)
                .addRanges(Coprocessor.KeyRange.newBuilder()
                        .setStart(ByteString.copyFrom(startKey))
                        .setEnd(ByteString.copyFrom(endKey)))
                .build();
    }

    private Tipb.DAGRequest.Builder baseDagBuilder() {
        var b = Tipb.DAGRequest.newBuilder().setTableId(TABLE_ID);
        for (Tipb.ColumnInfo col : columns) b.addColumns(col);
        for (int idx : outputIndices) b.addOutputColumnIndices(idx);
        return b;
    }

    // --- Selection tests ---

    @Test
    void selectionNoConditionsReturnsAllRows() throws Exception {
        insertRow(1, "Alice", 30);
        insertRow(2, "Bob", 25);
        insertRow(3, "Charlie", 35);

        Tipb.DAGRequest dagReq = baseDagBuilder().build();
        Coprocessor.Response resp = cop.handle(buildRequest(dagReq));

        assertThat(resp.getOtherError()).isEmpty();
        Tipb.SelectResponse selectResp = Tipb.SelectResponse.parseFrom(resp.getData());
        assertThat(selectResp.getIsAgg()).isFalse();
        List<CopKvPairDecoder.KvPair> pairs = CopKvPairDecoder.decode(
                selectResp.getKvPairData().toByteArray());
        assertThat(pairs).hasSize(3);
    }

    @Test
    void selectionWithWhereFilter() throws Exception {
        insertRow(1, "Alice", 30);
        insertRow(2, "Bob", 25);
        insertRow(3, "Charlie", 35);
        insertRow(4, "Diana", 20);

        // WHERE age > 28
        Tipb.Expr condition = Tipb.Expr.newBuilder()
                .setTp(Tipb.ExprType.BINARY_OP)
                .setOp(10) // GT
                .addChildren(Tipb.Expr.newBuilder()
                        .setTp(Tipb.ExprType.COLUMN_REF)
                        .setColumnName("age")
                        .setColumnIndex(2)
                        .setDataType(DT_BIGINT))
                .addChildren(Tipb.Expr.newBuilder()
                        .setTp(Tipb.ExprType.CONSTANT)
                        .setVal(Tipb.Datum.newBuilder().setIntVal(28L))
                        .setDataType(DT_BIGINT))
                .build();

        Tipb.DAGRequest dagReq = baseDagBuilder()
                .addConditions(condition)
                .build();
        Coprocessor.Response resp = cop.handle(buildRequest(dagReq));

        assertThat(resp.getOtherError()).isEmpty();
        Tipb.SelectResponse selectResp = Tipb.SelectResponse.parseFrom(resp.getData());
        List<CopKvPairDecoder.KvPair> pairs = CopKvPairDecoder.decode(
                selectResp.getKvPairData().toByteArray());
        assertThat(pairs).hasSize(2);
    }

    @Test
    void selectionEmptyTable() throws Exception {
        Tipb.DAGRequest dagReq = baseDagBuilder().build();
        Coprocessor.Response resp = cop.handle(buildRequest(dagReq));

        assertThat(resp.getOtherError()).isEmpty();
        Tipb.SelectResponse selectResp = Tipb.SelectResponse.parseFrom(resp.getData());
        List<CopKvPairDecoder.KvPair> pairs = CopKvPairDecoder.decode(
                selectResp.getKvPairData().toByteArray());
        assertThat(pairs).isEmpty();
    }

    // --- Aggregation tests ---

    @Test
    void aggregationCount() throws Exception {
        insertRow(1, "Alice", 30);
        insertRow(2, "Bob", 25);
        insertRow(3, "Charlie", 35);

        Tipb.DAGRequest dagReq = baseDagBuilder()
                .addAggFuncs(Tipb.AggFuncDesc.newBuilder()
                        .setAggType(0) // COUNT
                        .setDistinct(false))
                .build();
        Coprocessor.Response resp = cop.handle(buildRequest(dagReq));

        assertThat(resp.getOtherError()).isEmpty();
        Tipb.SelectResponse selectResp = Tipb.SelectResponse.parseFrom(resp.getData());
        assertThat(selectResp.getIsAgg()).isTrue();
        assertThat(selectResp.getAggGroupsCount()).isEqualTo(1);

        Tipb.AggGroupResult group = selectResp.getAggGroups(0);
        assertThat(group.getGroupKeysCount()).isEqualTo(0);
        Tipb.PartialAggState state = group.getPartialStates(0);
        assertThat(state.getAggType()).isEqualTo(0);
        assertThat(state.getState(0).getIntVal()).isEqualTo(3L);
    }

    @Test
    void aggregationSum() throws Exception {
        insertRow(1, "Alice", 30);
        insertRow(2, "Bob", 25);
        insertRow(3, "Charlie", 35);

        Tipb.Expr argExpr = Tipb.Expr.newBuilder()
                .setTp(Tipb.ExprType.COLUMN_REF)
                .setColumnName("age")
                .setColumnIndex(2)
                .setDataType(DT_BIGINT)
                .build();

        Tipb.DAGRequest dagReq = baseDagBuilder()
                .addAggFuncs(Tipb.AggFuncDesc.newBuilder()
                        .setAggType(1) // SUM
                        .setDistinct(false)
                        .setArg(argExpr))
                .build();
        Coprocessor.Response resp = cop.handle(buildRequest(dagReq));

        assertThat(resp.getOtherError()).isEmpty();
        Tipb.SelectResponse selectResp = Tipb.SelectResponse.parseFrom(resp.getData());
        assertThat(selectResp.getAggGroupsCount()).isEqualTo(1);

        Tipb.PartialAggState state = selectResp.getAggGroups(0).getPartialStates(0);
        assertThat(state.getAggType()).isEqualTo(1);
        assertThat(state.getState(0).getDecimalVal()).isEqualTo("90");
    }

    @Test
    void aggregationGroupBy() throws Exception {
        insertRow(1, "Alice", 30);
        insertRow(2, "Alice", 25);
        insertRow(3, "Bob", 35);
        insertRow(4, "Bob", 40);
        insertRow(5, "Bob", 20);

        Tipb.Expr groupExpr = Tipb.Expr.newBuilder()
                .setTp(Tipb.ExprType.COLUMN_REF)
                .setColumnName("name")
                .setColumnIndex(1)
                .setDataType(DT_VARCHAR)
                .build();

        Tipb.DAGRequest dagReq = baseDagBuilder()
                .addGroupByExprs(groupExpr)
                .addAggFuncs(Tipb.AggFuncDesc.newBuilder()
                        .setAggType(0) // COUNT
                        .setDistinct(false))
                .build();
        Coprocessor.Response resp = cop.handle(buildRequest(dagReq));

        assertThat(resp.getOtherError()).isEmpty();
        Tipb.SelectResponse selectResp = Tipb.SelectResponse.parseFrom(resp.getData());
        assertThat(selectResp.getAggGroupsCount()).isEqualTo(2);

        boolean foundAlice = false, foundBob = false;
        for (Tipb.AggGroupResult group : selectResp.getAggGroupsList()) {
            String name = group.getGroupKeys(0).getStringVal();
            long count = group.getPartialStates(0).getState(0).getIntVal();
            if ("Alice".equals(name)) { assertThat(count).isEqualTo(2); foundAlice = true; }
            if ("Bob".equals(name)) { assertThat(count).isEqualTo(3); foundBob = true; }
        }
        assertThat(foundAlice).isTrue();
        assertThat(foundBob).isTrue();
    }

    @Test
    void aggregationWithFilter() throws Exception {
        insertRow(1, "Alice", 30);
        insertRow(2, "Bob", 25);
        insertRow(3, "Charlie", 35);

        Tipb.Expr condition = Tipb.Expr.newBuilder()
                .setTp(Tipb.ExprType.BINARY_OP)
                .setOp(10) // GT
                .addChildren(Tipb.Expr.newBuilder()
                        .setTp(Tipb.ExprType.COLUMN_REF)
                        .setColumnName("age")
                        .setColumnIndex(2)
                        .setDataType(DT_BIGINT))
                .addChildren(Tipb.Expr.newBuilder()
                        .setTp(Tipb.ExprType.CONSTANT)
                        .setVal(Tipb.Datum.newBuilder().setIntVal(28L))
                        .setDataType(DT_BIGINT))
                .build();

        Tipb.DAGRequest dagReq = baseDagBuilder()
                .addConditions(condition)
                .addAggFuncs(Tipb.AggFuncDesc.newBuilder()
                        .setAggType(0) // COUNT
                        .setDistinct(false))
                .build();
        Coprocessor.Response resp = cop.handle(buildRequest(dagReq));

        Tipb.SelectResponse selectResp = Tipb.SelectResponse.parseFrom(resp.getData());
        assertThat(selectResp.getAggGroupsCount()).isEqualTo(1);
        assertThat(selectResp.getAggGroups(0).getPartialStates(0).getState(0).getIntVal()).isEqualTo(2L);
    }

    @Test
    void aggregationEmptyTable() throws Exception {
        Tipb.DAGRequest dagReq = baseDagBuilder()
                .addAggFuncs(Tipb.AggFuncDesc.newBuilder()
                        .setAggType(0) // COUNT
                        .setDistinct(false))
                .build();
        Coprocessor.Response resp = cop.handle(buildRequest(dagReq));

        Tipb.SelectResponse selectResp = Tipb.SelectResponse.parseFrom(resp.getData());
        assertThat(selectResp.getAggGroupsCount()).isEqualTo(1);
        assertThat(selectResp.getAggGroups(0).getPartialStates(0).getState(0).getIntVal()).isEqualTo(0L);
    }

    // --- TopN tests ---

    @Test
    void topNAscLimit() throws Exception {
        insertRow(1, "Alice", 30);
        insertRow(2, "Bob", 10);
        insertRow(3, "Charlie", 50);
        insertRow(4, "Diana", 20);
        insertRow(5, "Eve", 40);

        Tipb.Expr orderExpr = Tipb.Expr.newBuilder()
                .setTp(Tipb.ExprType.COLUMN_REF)
                .setColumnName("age")
                .setColumnIndex(2)
                .setDataType(DT_BIGINT)
                .build();

        Tipb.DAGRequest dagReq = baseDagBuilder()
                .setTopnLimit(3)
                .addOrderBy(Tipb.ByItem.newBuilder().setExpr(orderExpr).setDesc(false))
                .build();
        Coprocessor.Response resp = cop.handle(buildRequest(dagReq));

        assertThat(resp.getOtherError()).isEmpty();
        Tipb.SelectResponse selectResp = Tipb.SelectResponse.parseFrom(resp.getData());
        List<CopKvPairDecoder.KvPair> pairs = CopKvPairDecoder.decode(
                selectResp.getKvPairData().toByteArray());
        assertThat(pairs).hasSize(3);

        long[] ages = decodedAges(pairs);
        assertThat(ages[0]).isEqualTo(10L);
        assertThat(ages[1]).isEqualTo(20L);
        assertThat(ages[2]).isEqualTo(30L);
    }

    @Test
    void topNDescLimit() throws Exception {
        insertRow(1, "Alice", 30);
        insertRow(2, "Bob", 10);
        insertRow(3, "Charlie", 50);
        insertRow(4, "Diana", 20);

        Tipb.Expr orderExpr = Tipb.Expr.newBuilder()
                .setTp(Tipb.ExprType.COLUMN_REF)
                .setColumnName("age")
                .setColumnIndex(2)
                .setDataType(DT_BIGINT)
                .build();

        Tipb.DAGRequest dagReq = baseDagBuilder()
                .setTopnLimit(2)
                .addOrderBy(Tipb.ByItem.newBuilder().setExpr(orderExpr).setDesc(true))
                .build();
        Coprocessor.Response resp = cop.handle(buildRequest(dagReq));

        Tipb.SelectResponse selectResp = Tipb.SelectResponse.parseFrom(resp.getData());
        List<CopKvPairDecoder.KvPair> pairs = CopKvPairDecoder.decode(
                selectResp.getKvPairData().toByteArray());
        assertThat(pairs).hasSize(2);

        long[] ages = decodedAges(pairs);
        assertThat(ages[0]).isEqualTo(50L);
        assertThat(ages[1]).isEqualTo(30L);
    }

    @Test
    void topNWithOffset() throws Exception {
        insertRow(1, "A", 10);
        insertRow(2, "B", 20);
        insertRow(3, "C", 30);
        insertRow(4, "D", 40);
        insertRow(5, "E", 50);

        Tipb.Expr orderExpr = Tipb.Expr.newBuilder()
                .setTp(Tipb.ExprType.COLUMN_REF)
                .setColumnName("age")
                .setColumnIndex(2)
                .setDataType(DT_BIGINT)
                .build();

        Tipb.DAGRequest dagReq = baseDagBuilder()
                .setTopnLimit(2)
                .setTopnOffset(1)
                .addOrderBy(Tipb.ByItem.newBuilder().setExpr(orderExpr).setDesc(false))
                .build();
        Coprocessor.Response resp = cop.handle(buildRequest(dagReq));

        Tipb.SelectResponse selectResp = Tipb.SelectResponse.parseFrom(resp.getData());
        List<CopKvPairDecoder.KvPair> pairs = CopKvPairDecoder.decode(
                selectResp.getKvPairData().toByteArray());
        assertThat(pairs).hasSize(2);

        long[] ages = decodedAges(pairs);
        assertThat(ages[0]).isEqualTo(20L);
        assertThat(ages[1]).isEqualTo(30L);
    }

    @Test
    void topNEmptyTable() throws Exception {
        Tipb.Expr orderExpr = Tipb.Expr.newBuilder()
                .setTp(Tipb.ExprType.COLUMN_REF)
                .setColumnName("age")
                .setColumnIndex(2)
                .setDataType(DT_BIGINT)
                .build();

        Tipb.DAGRequest dagReq = baseDagBuilder()
                .setTopnLimit(10)
                .addOrderBy(Tipb.ByItem.newBuilder().setExpr(orderExpr).setDesc(false))
                .build();
        Coprocessor.Response resp = cop.handle(buildRequest(dagReq));

        Tipb.SelectResponse selectResp = Tipb.SelectResponse.parseFrom(resp.getData());
        List<CopKvPairDecoder.KvPair> pairs = CopKvPairDecoder.decode(
                selectResp.getKvPairData().toByteArray());
        assertThat(pairs).isEmpty();
    }

    @Test
    void invalidRequestDataReturnsError() {
        Coprocessor.Request req = Coprocessor.Request.newBuilder()
                .setTp(1)
                .setData(ByteString.copyFrom(new byte[]{0x00, 0x01}))
                .setStartTs(READ_TS)
                .addRanges(Coprocessor.KeyRange.newBuilder()
                        .setStart(ByteString.copyFrom(tableRecordPrefix(TABLE_ID)))
                        .setEnd(ByteString.copyFrom(encodeRowKey(TABLE_ID, Long.MAX_VALUE))))
                .build();

        Coprocessor.Response resp = cop.handle(req);
        assertThat(resp.getOtherError()).isNotEmpty();
    }

    @Test
    void requestTypeIsOne() {
        assertThat(cop.requestType()).isEqualTo(1);
    }

    // --- Chunked scanning tests ---

    @Test
    void aggregationScansAllRowsAcrossBatches() throws Exception {
        var smallBatchCop = new SQLScanCoprocessor(engine, 3);

        for (int i = 1; i <= 10; i++) {
            insertRow(i, "User" + i, i * 10);
        }

        Tipb.DAGRequest dagReq = baseDagBuilder()
                .addAggFuncs(Tipb.AggFuncDesc.newBuilder()
                        .setAggType(0)
                        .setDistinct(false))
                .build();
        Coprocessor.Response resp = smallBatchCop.handle(buildRequest(dagReq));

        Tipb.SelectResponse selectResp = Tipb.SelectResponse.parseFrom(resp.getData());
        assertThat(selectResp.getAggGroups(0).getPartialStates(0).getState(0).getIntVal())
                .isEqualTo(10L);
    }

    @Test
    void aggregationSumScansAllBatches() throws Exception {
        var smallBatchCop = new SQLScanCoprocessor(engine, 3);

        for (int i = 1; i <= 10; i++) {
            insertRow(i, "User" + i, i * 10);
        }

        Tipb.Expr argExpr = Tipb.Expr.newBuilder()
                .setTp(Tipb.ExprType.COLUMN_REF)
                .setColumnName("age")
                .setColumnIndex(2)
                .setDataType(DT_BIGINT)
                .build();

        Tipb.DAGRequest dagReq = baseDagBuilder()
                .addAggFuncs(Tipb.AggFuncDesc.newBuilder()
                        .setAggType(1)
                        .setDistinct(false)
                        .setArg(argExpr))
                .build();
        Coprocessor.Response resp = smallBatchCop.handle(buildRequest(dagReq));

        Tipb.SelectResponse selectResp = Tipb.SelectResponse.parseFrom(resp.getData());
        assertThat(selectResp.getAggGroups(0).getPartialStates(0).getState(0).getDecimalVal())
                .isEqualTo("550");
    }

    @Test
    void selectionContinuesScanningAfterFilteredRows() throws Exception {
        var smallBatchCop = new SQLScanCoprocessor(engine, 3);

        for (int i = 1; i <= 10; i++) {
            insertRow(i, "User" + i, i * 10);
        }

        Tipb.Expr condition = Tipb.Expr.newBuilder()
                .setTp(Tipb.ExprType.BINARY_OP)
                .setOp(10) // GT
                .addChildren(Tipb.Expr.newBuilder()
                        .setTp(Tipb.ExprType.COLUMN_REF)
                        .setColumnName("age")
                        .setColumnIndex(2)
                        .setDataType(DT_BIGINT))
                .addChildren(Tipb.Expr.newBuilder()
                        .setTp(Tipb.ExprType.CONSTANT)
                        .setVal(Tipb.Datum.newBuilder().setIntVal(25L))
                        .setDataType(DT_BIGINT))
                .build();

        Tipb.DAGRequest dagReq = baseDagBuilder()
                .addConditions(condition)
                .build();

        byte[] startKey = tableRecordPrefix(TABLE_ID);
        byte[] endKey = encodeRowKey(TABLE_ID, Long.MAX_VALUE);
        Coprocessor.Request req = Coprocessor.Request.newBuilder()
                .setTp(1)
                .setData(ByteString.copyFrom(dagReq.toByteArray()))
                .setStartTs(READ_TS)
                .setPagingSize(5)
                .addRanges(Coprocessor.KeyRange.newBuilder()
                        .setStart(ByteString.copyFrom(startKey))
                        .setEnd(ByteString.copyFrom(endKey)))
                .build();

        Coprocessor.Response resp = smallBatchCop.handle(req);

        Tipb.SelectResponse selectResp = Tipb.SelectResponse.parseFrom(resp.getData());
        List<CopKvPairDecoder.KvPair> pairs = CopKvPairDecoder.decode(
                selectResp.getKvPairData().toByteArray());
        assertThat(pairs).hasSize(5);

        long[] ages = decodedAges(pairs);
        assertThat(ages).containsExactly(30L, 40L, 50L, 60L, 70L);
    }

    @Test
    void topNScansAllBatchesCorrectly() throws Exception {
        var smallBatchCop = new SQLScanCoprocessor(engine, 3);

        insertRow(1, "A", 50);
        insertRow(2, "B", 30);
        insertRow(3, "C", 90);
        insertRow(4, "D", 10);
        insertRow(5, "E", 70);
        insertRow(6, "F", 20);
        insertRow(7, "G", 80);
        insertRow(8, "H", 40);
        insertRow(9, "I", 60);
        insertRow(10, "J", 100);

        Tipb.Expr orderExpr = Tipb.Expr.newBuilder()
                .setTp(Tipb.ExprType.COLUMN_REF)
                .setColumnName("age")
                .setColumnIndex(2)
                .setDataType(DT_BIGINT)
                .build();

        Tipb.DAGRequest dagReq = baseDagBuilder()
                .setTopnLimit(3)
                .addOrderBy(Tipb.ByItem.newBuilder().setExpr(orderExpr).setDesc(false))
                .build();
        Coprocessor.Response resp = smallBatchCop.handle(buildRequest(dagReq));

        Tipb.SelectResponse selectResp = Tipb.SelectResponse.parseFrom(resp.getData());
        List<CopKvPairDecoder.KvPair> pairs = CopKvPairDecoder.decode(
                selectResp.getKvPairData().toByteArray());
        assertThat(pairs).hasSize(3);

        long[] ages = decodedAges(pairs);
        assertThat(ages).containsExactly(10L, 20L, 30L);
    }

    // --- Streaming tests ---

    @Test
    void handleStreamSelectionProducesMultipleChunks() throws Exception {
        var streamCop = new SQLScanCoprocessor(engine, 3, 2);

        for (int i = 1; i <= 7; i++) {
            insertRow(i, "User" + i, i * 10);
        }

        Tipb.DAGRequest dagReq = baseDagBuilder().build();
        Coprocessor.Request req = buildRequest(dagReq);

        List<Coprocessor.StreamResponse> chunks = new ArrayList<>();
        streamCop.handleStream(req, chunks::add);

        assertThat(chunks).hasSizeGreaterThanOrEqualTo(3);

        int totalRows = 0;
        for (var chunk : chunks) {
            assertThat(chunk.getOtherError()).isEmpty();
            Tipb.SelectResponse selectResp = Tipb.SelectResponse.parseFrom(chunk.getData());
            List<CopKvPairDecoder.KvPair> pairs = CopKvPairDecoder.decode(
                    selectResp.getKvPairData().toByteArray());
            assertThat(pairs.size()).isLessThanOrEqualTo(2);
            totalRows += pairs.size();
        }
        assertThat(totalRows).isEqualTo(7);
    }

    @Test
    void handleStreamAggregationScansAllBatches() throws Exception {
        var smallBatchCop = new SQLScanCoprocessor(engine, 3);

        for (int i = 1; i <= 10; i++) {
            insertRow(i, "User" + i, i * 10);
        }

        Tipb.DAGRequest dagReq = baseDagBuilder()
                .addAggFuncs(Tipb.AggFuncDesc.newBuilder()
                        .setAggType(0)
                        .setDistinct(false))
                .build();

        List<Coprocessor.StreamResponse> chunks = new ArrayList<>();
        smallBatchCop.handleStream(buildRequest(dagReq), chunks::add);

        assertThat(chunks).hasSize(1);
        assertThat(chunks.get(0).getOtherError()).isEmpty();

        Tipb.SelectResponse selectResp = Tipb.SelectResponse.parseFrom(chunks.get(0).getData());
        assertThat(selectResp.getIsAgg()).isTrue();
        assertThat(selectResp.getAggGroups(0).getPartialStates(0).getState(0).getIntVal())
                .isEqualTo(10L);
    }

    @Test
    void handleStreamTopNScansAllBatches() throws Exception {
        var smallBatchCop = new SQLScanCoprocessor(engine, 3);

        insertRow(1, "A", 50);
        insertRow(2, "B", 30);
        insertRow(3, "C", 90);
        insertRow(4, "D", 10);
        insertRow(5, "E", 70);

        Tipb.Expr orderExpr = Tipb.Expr.newBuilder()
                .setTp(Tipb.ExprType.COLUMN_REF)
                .setColumnName("age")
                .setColumnIndex(2)
                .setDataType(DT_BIGINT)
                .build();

        Tipb.DAGRequest dagReq = baseDagBuilder()
                .setTopnLimit(2)
                .addOrderBy(Tipb.ByItem.newBuilder().setExpr(orderExpr).setDesc(true))
                .build();

        List<Coprocessor.StreamResponse> chunks = new ArrayList<>();
        smallBatchCop.handleStream(buildRequest(dagReq), chunks::add);

        assertThat(chunks).hasSize(1);

        Tipb.SelectResponse selectResp = Tipb.SelectResponse.parseFrom(chunks.get(0).getData());
        List<CopKvPairDecoder.KvPair> pairs = CopKvPairDecoder.decode(
                selectResp.getKvPairData().toByteArray());
        assertThat(pairs).hasSize(2);

        long[] ages = decodedAges(pairs);
        assertThat(ages).containsExactly(90L, 70L);
    }

    // --- encoding helpers (replicate x-db format without x-db dependency) ---

    private static byte[] encodeInt64(long v) {
        byte[] b = new byte[8];
        long u = v ^ Long.MIN_VALUE;
        for (int i = 7; i >= 0; i--) {
            b[i] = (byte) (u & 0xFF);
            u >>>= 8;
        }
        return b;
    }

    private static byte[] encodeRowKey(long tableId, long handle) {
        byte[] key = new byte[19];
        key[0] = 0x74; // 't'
        System.arraycopy(encodeInt64(tableId), 0, key, 1, 8);
        key[9] = 0x5F; // '_'
        key[10] = 0x72; // 'r'
        System.arraycopy(encodeInt64(handle), 0, key, 11, 8);
        return key;
    }

    private static byte[] tableRecordPrefix(long tableId) {
        byte[] prefix = new byte[11];
        prefix[0] = 0x74;
        System.arraycopy(encodeInt64(tableId), 0, prefix, 1, 8);
        prefix[9] = 0x5F;
        prefix[10] = 0x72;
        return prefix;
    }

    private static byte[] encodeUvarint(long v) {
        ByteArrayOutputStream out = new ByteArrayOutputStream(10);
        while ((v & ~0x7FL) != 0) {
            out.write((int) ((v & 0x7F) | 0x80));
            v >>>= 7;
        }
        out.write((int) (v & 0x7F));
        return out.toByteArray();
    }

    private static byte[] encodeVarint(long v) {
        long uv = (v << 1) ^ (v >> 63);
        return encodeUvarint(uv);
    }

    private static byte[] encodeRowValue(long[] colIds, Object[] values) {
        ByteArrayOutputStream out = new ByteArrayOutputStream(128);
        out.write(0x80); // ROW_FLAG
        for (int i = 0; i < colIds.length; i++) {
            byte[] colIdBytes = encodeUvarint(colIds[i]);
            out.write(colIdBytes, 0, colIdBytes.length);
            byte[] encoded = encodeValueBytes(values[i]);
            out.write(encoded, 0, encoded.length);
        }
        return out.toByteArray();
    }

    private static byte[] encodeValueBytes(Object value) {
        ByteArrayOutputStream out = new ByteArrayOutputStream(16);
        if (value instanceof Long l) {
            out.write(0x03); // INT_FLAG
            byte[] b = encodeVarint(l);
            out.write(b, 0, b.length);
        } else if (value instanceof String s) {
            out.write(0x02); // COMPACT_BYTES_FLAG
            byte[] raw = s.getBytes(StandardCharsets.UTF_8);
            byte[] lenB = encodeVarint(raw.length);
            out.write(lenB, 0, lenB.length);
            out.write(raw, 0, raw.length);
        }
        return out.toByteArray();
    }

    private long[] decodedAges(List<CopKvPairDecoder.KvPair> pairs) {
        long[] ages = new long[pairs.size()];
        for (int i = 0; i < pairs.size(); i++) {
            Map<Long, CopDatum> colValues = CopRowDecoder.decodeRowValue(pairs.get(i).value());
            CopDatum ageDatum = colValues.get(3L);
            ages[i] = ageDatum != null ? ageDatum.toLong() : 0;
        }
        return ages;
    }
}
