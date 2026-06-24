package io.github.xinfra.lab.xkv.kv.coprocessor;

import com.google.protobuf.ByteString;
import io.github.xinfra.lab.xkv.kv.config.KvConfig;
import io.github.xinfra.lab.xkv.kv.engine.RocksStorageEngine;
import io.github.xinfra.lab.xkv.kv.engine.StorageEngine;
import io.github.xinfra.lab.xkv.kv.mvcc.MvccKey;
import io.github.xinfra.lab.xkv.kv.mvcc.Write;
import io.github.xinfra.lab.xkv.proto.Coprocessor;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class SplitKeysCoprocessorTest {

    @TempDir Path dataDir;
    private RocksStorageEngine engine;
    private SplitKeysCoprocessor cop;

    @BeforeEach
    void setUp() throws Exception {
        engine = RocksStorageEngine.open(dataDir, KvConfig.EngineConfig.defaults());
        cop = new SplitKeysCoprocessor(engine);
    }

    @AfterEach
    void tearDown() {
        if (engine != null) engine.close();
    }

    private void putWrite(String userKey, long commitTs, long startTs) {
        byte[] uk = userKey.getBytes(StandardCharsets.UTF_8);
        try (var batch = engine.newWriteBatch()) {
            batch.put(StorageEngine.Cf.DEFAULT,
                    MvccKey.encode(uk, startTs), new byte[64]);
            batch.put(StorageEngine.Cf.WRITE,
                    MvccKey.encode(uk, commitTs),
                    Write.put(startTs).encode());
            engine.write(batch, false);
        }
    }

    @Test
    void requestType() {
        assertThat(cop.requestType()).isEqualTo(4);
    }

    @Test
    void emptyRegionReturnsNoKeys() {
        Coprocessor.Request req = Coprocessor.Request.newBuilder()
                .setTp(4)
                .setPagingSize(3)
                .build();
        Coprocessor.Response resp = cop.handle(req);
        assertThat(resp.getOtherError()).isEmpty();
        List<byte[]> keys = SplitKeysCoprocessor.decodeSplitKeys(resp.getData().toByteArray());
        assertThat(keys).isEmpty();
    }

    @Test
    void singleKeyRegionReturnsNoSplitKeys() {
        putWrite("key-a", 20, 10);
        List<byte[]> keys = cop.findSplitKeys(null, null, 2);
        assertThat(keys).hasSizeLessThanOrEqualTo(1);
    }

    @Test
    void findsSplitKeysForMultipleEntries() {
        for (int i = 0; i < 100; i++) {
            putWrite(String.format("key-%03d", i), 20, 10);
        }
        List<byte[]> keys = cop.findSplitKeys(null, null, 4);
        assertThat(keys).hasSizeBetween(1, 3);
        for (byte[] k : keys) {
            assertThat(k).isNotEmpty();
        }
    }

    @Test
    void splitCountOneReturnsEmpty() {
        putWrite("key-a", 20, 10);
        putWrite("key-b", 20, 10);
        List<byte[]> keys = cop.findSplitKeys(null, null, 1);
        assertThat(keys).isEmpty();
    }

    @Test
    void respectsStartAndEndKeyBounds() {
        for (int i = 0; i < 50; i++) {
            putWrite(String.format("key-%03d", i), 20, 10);
        }
        byte[] start = "key-010".getBytes(StandardCharsets.UTF_8);
        byte[] end = "key-040".getBytes(StandardCharsets.UTF_8);
        List<byte[]> keys = cop.findSplitKeys(start, end, 3);
        for (byte[] k : keys) {
            String s = new String(k, StandardCharsets.UTF_8);
            assertThat(s).isGreaterThan("key-010").isLessThan("key-040");
        }
    }

    @Test
    void encodeDecodeSplitKeysRoundTrip() {
        List<byte[]> original = List.of(
                "abc".getBytes(StandardCharsets.UTF_8),
                "def".getBytes(StandardCharsets.UTF_8),
                new byte[0]);
        byte[] encoded = SplitKeysCoprocessor.encodeSplitKeys(original);
        List<byte[]> decoded = SplitKeysCoprocessor.decodeSplitKeys(encoded);
        assertThat(decoded).hasSize(3);
        assertThat(decoded.get(0)).isEqualTo("abc".getBytes(StandardCharsets.UTF_8));
        assertThat(decoded.get(1)).isEqualTo("def".getBytes(StandardCharsets.UTF_8));
        assertThat(decoded.get(2)).isEmpty();
    }

    @Test
    void handleViaGrpcRequest() {
        for (int i = 0; i < 50; i++) {
            putWrite(String.format("row-%03d", i), 20, 10);
        }
        byte[] start = "row-000".getBytes(StandardCharsets.UTF_8);
        byte[] end = "row-050".getBytes(StandardCharsets.UTF_8);

        Coprocessor.Request req = Coprocessor.Request.newBuilder()
                .setTp(4)
                .setPagingSize(3)
                .addRanges(Coprocessor.KeyRange.newBuilder()
                        .setStart(ByteString.copyFrom(start))
                        .setEnd(ByteString.copyFrom(end)))
                .build();

        Coprocessor.Response resp = cop.handle(req);
        assertThat(resp.getOtherError()).isEmpty();
        List<byte[]> keys = SplitKeysCoprocessor.decodeSplitKeys(resp.getData().toByteArray());
        assertThat(keys).hasSizeBetween(1, 2);
    }

    @Test
    void deduplicatesStartAndEndKeys() {
        putWrite("aaa", 20, 10);
        putWrite("zzz", 20, 10);
        byte[] start = "aaa".getBytes(StandardCharsets.UTF_8);
        byte[] end = "zzz".getBytes(StandardCharsets.UTF_8);
        List<byte[]> keys = cop.findSplitKeys(start, end, 3);
        for (byte[] k : keys) {
            assertThat(k).isNotEqualTo(start);
            assertThat(k).isNotEqualTo(end);
        }
    }

    @Test
    void handleStreamReturnsSameAsHandle() {
        for (int i = 0; i < 30; i++) {
            putWrite(String.format("s-%03d", i), 20, 10);
        }

        Coprocessor.Request req = Coprocessor.Request.newBuilder()
                .setTp(4)
                .setPagingSize(2)
                .build();

        Coprocessor.Response directResp = cop.handle(req);
        var streamResponses = new java.util.ArrayList<Coprocessor.StreamResponse>();
        cop.handleStream(req, streamResponses::add);

        assertThat(streamResponses).hasSize(1);
        assertThat(streamResponses.get(0).getData()).isEqualTo(directResp.getData());
    }
}
