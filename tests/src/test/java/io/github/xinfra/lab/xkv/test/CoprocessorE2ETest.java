package io.github.xinfra.lab.xkv.test;

import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import io.github.xinfra.lab.raft.Peer;
import io.github.xinfra.lab.xkv.kv.coprocessor.TableScanCoprocessor;
import io.github.xinfra.lab.xkv.kv.coprocessor.eval.CopDatum;
import io.github.xinfra.lab.xkv.kv.coprocessor.eval.TidbKeyCodec;
import io.github.xinfra.lab.xkv.kv.config.KvConfig;
import io.github.xinfra.lab.xkv.kv.engine.PerRegionRaftEngine;
import io.github.xinfra.lab.xkv.kv.engine.RocksStorageEngine;
import io.github.xinfra.lab.xkv.kv.mvcc.ConcurrencyManager;
import io.github.xinfra.lab.xkv.kv.mvcc.MaxTsTracker;
import io.github.xinfra.lab.xkv.kv.raft.CompositeApplyHandler;
import io.github.xinfra.lab.xkv.kv.raft.LoopbackTransport;
import io.github.xinfra.lab.xkv.kv.raft.RegionPeerImpl;
import io.github.xinfra.lab.xkv.kv.server.CoprocessorService;
import io.github.xinfra.lab.xkv.kv.server.RawKvService;
import io.github.xinfra.lab.xkv.kv.server.TikvServiceImpl;
import io.github.xinfra.lab.xkv.kv.server.TransactionService;
import io.github.xinfra.lab.xkv.proto.Coprocessor;
import io.github.xinfra.lab.xkv.proto.Kvrpcpb;
import io.github.xinfra.lab.xkv.proto.Metapb;
import io.github.xinfra.lab.xkv.proto.Tipb;
import io.github.xinfra.lab.xkv.proto.TikvGrpc;
import io.grpc.ManagedChannel;
import io.grpc.Server;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * E2E test: coprocessor table-scan over MVCC data.
 *
 * <p>Writes data through the txn path (Prewrite + Commit), then uses the
 * coprocessor RPC to scan it back. Verifies MVCC snapshot isolation and
 * paging.
 */
final class CoprocessorE2ETest {

    @TempDir Path dataDir;
    private RocksStorageEngine engine;
    private RegionPeerImpl peer;
    private Server grpcServer;
    private ManagedChannel channel;
    private TikvGrpc.TikvBlockingStub tikv;

    @BeforeEach
    void start() throws Exception {
        engine = RocksStorageEngine.open(dataDir, KvConfig.EngineConfig.defaults());
        var raftEngine = new PerRegionRaftEngine(engine, 1);
        var region = Metapb.Region.newBuilder()
                .setId(1)
                .setRegionEpoch(Metapb.RegionEpoch.newBuilder().setConfVer(1).setVersion(1))
                .addPeers(Metapb.Peer.newBuilder().setId(1).setStoreId(1).setRole(Metapb.PeerRole.Voter))
                .build();
        var cm = new ConcurrencyManager(new MaxTsTracker(raftEngine.persistedMaxTs()));
        peer = new RegionPeerImpl(
                engine, raftEngine, region, region.getPeers(0),
                List.of(new Peer(1)),
                new LoopbackTransport(),
                CompositeApplyHandler.defaultFor(engine, cm).withAdmin(raftEngine),
                new RegionPeerImpl.Settings(10, 1, 30),
                cm);
        Awaitility.await().atMost(Duration.ofSeconds(10)).until(peer::isLeader);

        var rawKv = new RawKvService(engine, key -> peer, 5_000);
        var txn = new TransactionService(engine, key -> peer, 5_000, cm);

        var copService = new CoprocessorService();
        copService.register(new TableScanCoprocessor(engine));
        copService.register(new io.github.xinfra.lab.xkv.kv.coprocessor.IndexScanCoprocessor(engine));

        var name = "cop-test-" + UUID.randomUUID();
        grpcServer = InProcessServerBuilder.forName(name)
                .directExecutor()
                .addService(new TikvServiceImpl(rawKv, txn, copService, null, null))
                .build()
                .start();
        channel = InProcessChannelBuilder.forName(name).directExecutor().build();
        tikv = TikvGrpc.newBlockingStub(channel);
    }

    @AfterEach
    void teardown() {
        if (channel != null) channel.shutdownNow();
        if (grpcServer != null) {
            grpcServer.shutdownNow();
            try { grpcServer.awaitTermination(2, TimeUnit.SECONDS); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
        }
        if (peer != null) peer.shutdown();
        if (engine != null) engine.close();
    }

    @Test
    void coprocessorScanReturnsMvccData() throws Exception {
        long startTs = 10;
        long commitTs = 20;

        // Write 5 keys via 2PC.
        var mutations = new ArrayList<Kvrpcpb.Mutation>();
        for (int i = 0; i < 5; i++) {
            mutations.add(Kvrpcpb.Mutation.newBuilder()
                    .setOp(Kvrpcpb.Op.Put)
                    .setKey(ByteString.copyFromUtf8(String.format("ck%03d", i)))
                    .setValue(ByteString.copyFromUtf8("cv" + i))
                    .build());
        }

        var prewriteResp = tikv.kvPrewrite(Kvrpcpb.PrewriteRequest.newBuilder()
                .setStartVersion(startTs)
                .setPrimaryLock(ByteString.copyFromUtf8("ck000"))
                .addAllMutations(mutations)
                .setLockTtl(5000)
                .build());
        assertThat(prewriteResp.getErrorsCount()).isZero();

        var commitResp = tikv.kvCommit(Kvrpcpb.CommitRequest.newBuilder()
                .setStartVersion(startTs)
                .setCommitVersion(commitTs)
                .addAllKeys(mutations.stream()
                        .map(Kvrpcpb.Mutation::getKey).toList())
                .build());
        assertThat(commitResp.hasError()).isFalse();

        // Coprocessor scan at commitTs should see all 5 keys.
        var copResp = tikv.coprocessor(Coprocessor.Request.newBuilder()
                .setTp(0)
                .setStartTs(commitTs)
                .addRanges(Coprocessor.KeyRange.newBuilder()
                        .setStart(ByteString.copyFromUtf8("ck000"))
                        .setEnd(ByteString.copyFromUtf8("ck999")))
                .build());
        assertThat(copResp.getOtherError()).isEmpty();
        assertThat(copResp.hasLocked()).isFalse();

        var pairs = decodeKvPairs(copResp.getData().toByteArray());
        assertThat(pairs).hasSize(5);
        for (int i = 0; i < 5; i++) {
            assertThat(new String(pairs.get(i).key)).isEqualTo(String.format("ck%03d", i));
            assertThat(new String(pairs.get(i).value)).isEqualTo("cv" + i);
        }
    }

    @Test
    void coprocessorPagingLimitsResults() throws Exception {
        long startTs = 100;
        long commitTs = 200;

        var mutations = new ArrayList<Kvrpcpb.Mutation>();
        for (int i = 0; i < 10; i++) {
            mutations.add(Kvrpcpb.Mutation.newBuilder()
                    .setOp(Kvrpcpb.Op.Put)
                    .setKey(ByteString.copyFromUtf8(String.format("pk%03d", i)))
                    .setValue(ByteString.copyFromUtf8("pv" + i))
                    .build());
        }

        tikv.kvPrewrite(Kvrpcpb.PrewriteRequest.newBuilder()
                .setStartVersion(startTs)
                .setPrimaryLock(ByteString.copyFromUtf8("pk000"))
                .addAllMutations(mutations)
                .setLockTtl(5000).build());
        tikv.kvCommit(Kvrpcpb.CommitRequest.newBuilder()
                .setStartVersion(startTs).setCommitVersion(commitTs)
                .addAllKeys(mutations.stream().map(Kvrpcpb.Mutation::getKey).toList())
                .build());

        // Request with paging_size = 3: should return at most 3 rows.
        var copResp = tikv.coprocessor(Coprocessor.Request.newBuilder()
                .setTp(0).setStartTs(commitTs).setPagingSize(3)
                .addRanges(Coprocessor.KeyRange.newBuilder()
                        .setStart(ByteString.copyFromUtf8("pk000"))
                        .setEnd(ByteString.copyFromUtf8("pk999")))
                .build());
        assertThat(copResp.getOtherError()).isEmpty();

        var pairs = decodeKvPairs(copResp.getData().toByteArray());
        assertThat(pairs).hasSize(3);
        assertThat(new String(pairs.get(0).key)).isEqualTo("pk000");
        assertThat(new String(pairs.get(2).key)).isEqualTo("pk002");
    }

    @Test
    void coprocessorScanAtOlderTimestampSeesNothing() throws Exception {
        long startTs = 300;
        long commitTs = 400;

        tikv.kvPrewrite(Kvrpcpb.PrewriteRequest.newBuilder()
                .setStartVersion(startTs)
                .setPrimaryLock(ByteString.copyFromUtf8("tk001"))
                .addMutations(Kvrpcpb.Mutation.newBuilder()
                        .setOp(Kvrpcpb.Op.Put)
                        .setKey(ByteString.copyFromUtf8("tk001"))
                        .setValue(ByteString.copyFromUtf8("tv1")))
                .setLockTtl(5000).build());
        tikv.kvCommit(Kvrpcpb.CommitRequest.newBuilder()
                .setStartVersion(startTs).setCommitVersion(commitTs)
                .addKeys(ByteString.copyFromUtf8("tk001")).build());

        // Scan at ts=200 (before commitTs=400) should see nothing.
        var copResp = tikv.coprocessor(Coprocessor.Request.newBuilder()
                .setTp(0).setStartTs(200)
                .addRanges(Coprocessor.KeyRange.newBuilder()
                        .setStart(ByteString.copyFromUtf8("tk000"))
                        .setEnd(ByteString.copyFromUtf8("tk999")))
                .build());
        assertThat(copResp.getOtherError()).isEmpty();
        var pairs = decodeKvPairs(copResp.getData().toByteArray());
        assertThat(pairs).isEmpty();
    }

    @Test
    void indexScanCoveringReturnsIndexEntries() throws Exception {
        long tableId = 1, indexId = 1;
        long startTs = 500, commitTs = 600;
        int rowCount = 5;

        var mutations = new ArrayList<Kvrpcpb.Mutation>();
        byte[] primaryKey = TidbKeyCodec.encodeRecordKey(tableId, 1);

        for (int i = 1; i <= rowCount; i++) {
            byte[] recordKey = TidbKeyCodec.encodeRecordKey(tableId, i);
            byte[] recordValue = encodeIntRowValue(2, i * 10L);
            mutations.add(mutation(Kvrpcpb.Op.Put, recordKey, recordValue));

            byte[] indexKey = TidbKeyCodec.encodeIndexKey(tableId, indexId,
                    new CopDatum[]{CopDatum.of((long) (i * 10))});
            byte[] indexValue = new byte[8];
            TidbKeyCodec.encodeInt64(indexValue, 0, i);
            mutations.add(mutation(Kvrpcpb.Op.Put, indexKey, indexValue));
        }

        commit2PC(startTs, commitTs, primaryKey, mutations);

        Tipb.DAGRequest dagReq = Tipb.DAGRequest.newBuilder()
                .setTableId(tableId)
                .setIndexId(indexId)
                .addIndexColumns(Tipb.ColumnInfo.newBuilder()
                        .setColumnId(2).setDataType(3))
                .build();

        byte[] rangeStart = TidbKeyCodec.encodeIndexKeyPrefix(tableId, indexId);
        byte[] rangeEnd = TidbKeyCodec.encodeIndexKeyPrefix(tableId, indexId + 1);

        var copResp = tikv.coprocessor(Coprocessor.Request.newBuilder()
                .setTp(3)
                .setStartTs(commitTs)
                .setData(ByteString.copyFrom(dagReq.toByteArray()))
                .addRanges(Coprocessor.KeyRange.newBuilder()
                        .setStart(ByteString.copyFrom(rangeStart))
                        .setEnd(ByteString.copyFrom(rangeEnd)))
                .build());

        assertThat(copResp.getOtherError()).isEmpty();

        var pairs = decodeSelectResponseKvPairs(copResp.getData().toByteArray());
        assertThat(pairs).hasSize(rowCount);

        for (int i = 0; i < rowCount; i++) {
            byte[] expectedKey = TidbKeyCodec.encodeIndexKey(tableId, indexId,
                    new CopDatum[]{CopDatum.of((long) ((i + 1) * 10))});
            assertThat(pairs.get(i).key).isEqualTo(expectedKey);
        }
    }

    @Test
    void indexLookupReturnsFullRows() throws Exception {
        long tableId = 2, indexId = 1;
        long startTs = 700, commitTs = 800;
        int rowCount = 3;

        var mutations = new ArrayList<Kvrpcpb.Mutation>();
        byte[] primaryKey = TidbKeyCodec.encodeRecordKey(tableId, 1);

        for (int i = 1; i <= rowCount; i++) {
            byte[] recordKey = TidbKeyCodec.encodeRecordKey(tableId, i);
            byte[] recordValue = encodeIntRowValue(2, i * 100L);
            mutations.add(mutation(Kvrpcpb.Op.Put, recordKey, recordValue));

            byte[] indexKey = TidbKeyCodec.encodeIndexKey(tableId, indexId,
                    new CopDatum[]{CopDatum.of((long) (i * 100))});
            byte[] indexValue = new byte[8];
            TidbKeyCodec.encodeInt64(indexValue, 0, i);
            mutations.add(mutation(Kvrpcpb.Op.Put, indexKey, indexValue));
        }

        commit2PC(startTs, commitTs, primaryKey, mutations);

        Tipb.DAGRequest dagReq = Tipb.DAGRequest.newBuilder()
                .setTableId(tableId)
                .setIndexId(indexId)
                .addIndexColumns(Tipb.ColumnInfo.newBuilder()
                        .setColumnId(2).setDataType(3))
                .setIndexLookup(true)
                .addColumns(Tipb.ColumnInfo.newBuilder()
                        .setColumnId(1).setDataType(3).setAutoIncrement(true))
                .addColumns(Tipb.ColumnInfo.newBuilder()
                        .setColumnId(2).setDataType(3))
                .addOutputColumnIndices(0)
                .addOutputColumnIndices(1)
                .build();

        byte[] rangeStart = TidbKeyCodec.encodeIndexKeyPrefix(tableId, indexId);
        byte[] rangeEnd = TidbKeyCodec.encodeIndexKeyPrefix(tableId, indexId + 1);

        var copResp = tikv.coprocessor(Coprocessor.Request.newBuilder()
                .setTp(3)
                .setStartTs(commitTs)
                .setData(ByteString.copyFrom(dagReq.toByteArray()))
                .addRanges(Coprocessor.KeyRange.newBuilder()
                        .setStart(ByteString.copyFrom(rangeStart))
                        .setEnd(ByteString.copyFrom(rangeEnd)))
                .build());

        assertThat(copResp.getOtherError()).isEmpty();

        var pairs = decodeSelectResponseKvPairs(copResp.getData().toByteArray());
        assertThat(pairs).hasSize(rowCount);

        for (int i = 0; i < rowCount; i++) {
            byte[] expectedRecordKey = TidbKeyCodec.encodeRecordKey(tableId, i + 1);
            assertThat(pairs.get(i).key).isEqualTo(expectedRecordKey);
        }
    }

    @Test
    void indexScanAtOlderTimestampSeesNothing() throws Exception {
        long tableId = 3, indexId = 1;
        long startTs = 900, commitTs = 1000;

        byte[] recordKey = TidbKeyCodec.encodeRecordKey(tableId, 1);
        byte[] recordValue = encodeIntRowValue(2, 42L);
        byte[] indexKey = TidbKeyCodec.encodeIndexKey(tableId, indexId,
                new CopDatum[]{CopDatum.of(42L)});
        byte[] indexValue = new byte[8];
        TidbKeyCodec.encodeInt64(indexValue, 0, 1);

        commit2PC(startTs, commitTs, recordKey,
                List.of(mutation(Kvrpcpb.Op.Put, recordKey, recordValue),
                        mutation(Kvrpcpb.Op.Put, indexKey, indexValue)));

        Tipb.DAGRequest dagReq = Tipb.DAGRequest.newBuilder()
                .setTableId(tableId)
                .setIndexId(indexId)
                .addIndexColumns(Tipb.ColumnInfo.newBuilder()
                        .setColumnId(2).setDataType(3))
                .build();

        byte[] rangeStart = TidbKeyCodec.encodeIndexKeyPrefix(tableId, indexId);
        byte[] rangeEnd = TidbKeyCodec.encodeIndexKeyPrefix(tableId, indexId + 1);

        var copResp = tikv.coprocessor(Coprocessor.Request.newBuilder()
                .setTp(3)
                .setStartTs(500)
                .setData(ByteString.copyFrom(dagReq.toByteArray()))
                .addRanges(Coprocessor.KeyRange.newBuilder()
                        .setStart(ByteString.copyFrom(rangeStart))
                        .setEnd(ByteString.copyFrom(rangeEnd)))
                .build());

        assertThat(copResp.getOtherError()).isEmpty();
        var pairs = decodeSelectResponseKvPairs(copResp.getData().toByteArray());
        assertThat(pairs).isEmpty();
    }

    // --- Helpers ---

    private void commit2PC(long startTs, long commitTs, byte[] primaryKey,
                           List<Kvrpcpb.Mutation> mutations) {
        var prewriteResp = tikv.kvPrewrite(Kvrpcpb.PrewriteRequest.newBuilder()
                .setStartVersion(startTs)
                .setPrimaryLock(ByteString.copyFrom(primaryKey))
                .addAllMutations(mutations)
                .setLockTtl(5000)
                .build());
        assertThat(prewriteResp.getErrorsCount()).isZero();

        var commitResp = tikv.kvCommit(Kvrpcpb.CommitRequest.newBuilder()
                .setStartVersion(startTs)
                .setCommitVersion(commitTs)
                .addAllKeys(mutations.stream()
                        .map(Kvrpcpb.Mutation::getKey).toList())
                .build());
        assertThat(commitResp.hasError()).isFalse();
    }

    private static Kvrpcpb.Mutation mutation(Kvrpcpb.Op op, byte[] key, byte[] value) {
        return Kvrpcpb.Mutation.newBuilder()
                .setOp(op)
                .setKey(ByteString.copyFrom(key))
                .setValue(ByteString.copyFrom(value))
                .build();
    }

    private static byte[] encodeIntRowValue(long colId, long value) {
        byte[] colIdBytes = encodeUvarint(colId);
        byte[] valueBytes = encodeVarint(value);
        byte[] result = new byte[colIdBytes.length + 1 + valueBytes.length];
        System.arraycopy(colIdBytes, 0, result, 0, colIdBytes.length);
        result[colIdBytes.length] = 0x03; // INT_FLAG
        System.arraycopy(valueBytes, 0, result, colIdBytes.length + 1, valueBytes.length);
        return result;
    }

    private static byte[] encodeUvarint(long v) {
        byte[] buf = new byte[10];
        int i = 0;
        while (v >= 0x80) {
            buf[i++] = (byte) (v | 0x80);
            v >>>= 7;
        }
        buf[i++] = (byte) v;
        byte[] result = new byte[i];
        System.arraycopy(buf, 0, result, 0, i);
        return result;
    }

    private static byte[] encodeVarint(long v) {
        return encodeUvarint((v << 1) ^ (v >> 63));
    }

    private record KvPair(byte[] key, byte[] value) {}

    private static List<KvPair> decodeKvPairs(byte[] data) {
        if (data == null || data.length < 4) return List.of();
        var bb = ByteBuffer.wrap(data);
        int count = bb.getInt();
        var out = new ArrayList<KvPair>(count);
        for (int i = 0; i < count; i++) {
            int kLen = bb.getInt();
            byte[] k = new byte[kLen];
            bb.get(k);
            int vLen = bb.getInt();
            byte[] v = new byte[vLen];
            bb.get(v);
            out.add(new KvPair(k, v));
        }
        return out;
    }

    private static List<KvPair> decodeSelectResponseKvPairs(byte[] data)
            throws InvalidProtocolBufferException {
        Tipb.SelectResponse selectResp = Tipb.SelectResponse.parseFrom(data);
        return decodeKvPairs(selectResp.getKvPairData().toByteArray());
    }
}
