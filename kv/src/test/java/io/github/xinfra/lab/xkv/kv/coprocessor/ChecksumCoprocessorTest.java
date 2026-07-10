package io.github.xinfra.lab.xkv.kv.coprocessor;

import com.google.protobuf.ByteString;
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
import java.util.zip.CRC32;

import static org.assertj.core.api.Assertions.assertThat;

class ChecksumCoprocessorTest {

    private static final long TABLE_ID = 200;
    private static final long START_TS = 10;
    private static final long COMMIT_TS = 20;
    private static final long READ_TS = 100;

    @TempDir Path dataDir;
    private RocksStorageEngine engine;
    private ChecksumCoprocessor cop;

    @BeforeEach
    void setUp() throws Exception {
        engine = RocksStorageEngine.open(dataDir, KvConfig.EngineConfig.defaults());
        cop = new ChecksumCoprocessor(engine);
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

    private Coprocessor.Request buildRequest() {
        byte[] startKey = tableRecordPrefix(TABLE_ID);
        byte[] endKey = encodeRowKey(TABLE_ID, Long.MAX_VALUE);
        return Coprocessor.Request.newBuilder()
                .setTp(5)
                .setStartTs(READ_TS)
                .addRanges(Coprocessor.KeyRange.newBuilder()
                        .setStart(ByteString.copyFrom(startKey))
                        .setEnd(ByteString.copyFrom(endKey)))
                .build();
    }

    @Test
    void checksumEmptyTable() throws Exception {
        Coprocessor.Response resp = cop.handle(buildRequest());

        assertThat(resp.getOtherError()).isEmpty();
        Tipb.ChecksumResponse checksumResp = Tipb.ChecksumResponse.parseFrom(resp.getData());
        assertThat(checksumResp.getTotalKvs()).isZero();
        assertThat(checksumResp.getTotalBytes()).isZero();
        assertThat(checksumResp.getChecksum()).isEqualTo(0L);
    }

    @Test
    void checksumSingleRow() throws Exception {
        insertRow(1, "Alice", 30);

        Coprocessor.Response resp = cop.handle(buildRequest());
        assertThat(resp.getOtherError()).isEmpty();

        Tipb.ChecksumResponse checksumResp = Tipb.ChecksumResponse.parseFrom(resp.getData());
        assertThat(checksumResp.getTotalKvs()).isEqualTo(1);
        assertThat(checksumResp.getTotalBytes()).isGreaterThan(0);
        assertThat(checksumResp.getChecksum()).isNotEqualTo(0L);
    }

    @Test
    void checksumMultipleRows() throws Exception {
        insertRow(1, "Alice", 30);
        insertRow(2, "Bob", 25);
        insertRow(3, "Charlie", 35);

        Coprocessor.Response resp = cop.handle(buildRequest());
        assertThat(resp.getOtherError()).isEmpty();

        Tipb.ChecksumResponse checksumResp = Tipb.ChecksumResponse.parseFrom(resp.getData());
        assertThat(checksumResp.getTotalKvs()).isEqualTo(3);
        assertThat(checksumResp.getTotalBytes()).isGreaterThan(0);
    }

    @Test
    void checksumDeterministic() throws Exception {
        insertRow(1, "Alice", 30);
        insertRow(2, "Bob", 25);

        Coprocessor.Response resp1 = cop.handle(buildRequest());
        Coprocessor.Response resp2 = cop.handle(buildRequest());

        Tipb.ChecksumResponse cr1 = Tipb.ChecksumResponse.parseFrom(resp1.getData());
        Tipb.ChecksumResponse cr2 = Tipb.ChecksumResponse.parseFrom(resp2.getData());

        assertThat(cr1.getChecksum()).isEqualTo(cr2.getChecksum());
        assertThat(cr1.getTotalKvs()).isEqualTo(cr2.getTotalKvs());
        assertThat(cr1.getTotalBytes()).isEqualTo(cr2.getTotalBytes());
    }

    @Test
    void checksumChangesWhenDataChanges() throws Exception {
        insertRow(1, "Alice", 30);
        Coprocessor.Response resp1 = cop.handle(buildRequest());
        Tipb.ChecksumResponse cr1 = Tipb.ChecksumResponse.parseFrom(resp1.getData());

        insertRow(2, "Bob", 25);
        Coprocessor.Response resp2 = cop.handle(buildRequest());
        Tipb.ChecksumResponse cr2 = Tipb.ChecksumResponse.parseFrom(resp2.getData());

        assertThat(cr2.getChecksum()).isNotEqualTo(cr1.getChecksum());
        assertThat(cr2.getTotalKvs()).isEqualTo(cr1.getTotalKvs() + 1);
        assertThat(cr2.getTotalBytes()).isGreaterThan(cr1.getTotalBytes());
    }

    @Test
    void streamReturnsChecksumInSingleChunk() throws Exception {
        insertRow(1, "Alice", 30);
        insertRow(2, "Bob", 25);

        List<Coprocessor.StreamResponse> chunks = new ArrayList<>();
        cop.handleStream(buildRequest(), chunks::add);

        assertThat(chunks).hasSize(1);
        Tipb.ChecksumResponse checksumResp = Tipb.ChecksumResponse.parseFrom(chunks.get(0).getData());
        assertThat(checksumResp.getTotalKvs()).isEqualTo(2);
    }

    // --- Key encoding helpers (same as SQLScanCoprocessorTest) ---

    private static byte[] encodeInt64(long v) {
        long u = v ^ (1L << 63);
        byte[] b = new byte[8];
        for (int i = 7; i >= 0; i--) {
            b[i] = (byte) (u & 0xFF);
            u >>>= 8;
        }
        return b;
    }

    private static byte[] encodeRowKey(long tableId, long handle) {
        byte[] key = new byte[19];
        key[0] = 0x74;
        System.arraycopy(encodeInt64(tableId), 0, key, 1, 8);
        key[9] = 0x5F;
        key[10] = 0x72;
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
        out.write(0x80);
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
            out.write(0x03);
            byte[] b = encodeVarint(l);
            out.write(b, 0, b.length);
        } else if (value instanceof String s) {
            out.write(0x02);
            byte[] raw = s.getBytes(StandardCharsets.UTF_8);
            byte[] lenB = encodeVarint(raw.length);
            out.write(lenB, 0, lenB.length);
            out.write(raw, 0, raw.length);
        }
        return out.toByteArray();
    }
}
