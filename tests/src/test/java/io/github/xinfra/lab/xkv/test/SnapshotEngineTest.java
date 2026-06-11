package io.github.xinfra.lab.xkv.test;

import io.github.xinfra.lab.xkv.kv.config.KvConfig;
import io.github.xinfra.lab.xkv.kv.engine.RocksStorageEngine;
import io.github.xinfra.lab.xkv.kv.engine.SnapshotEngineImpl;
import io.github.xinfra.lab.xkv.kv.engine.StorageEngine;
import io.github.xinfra.lab.xkv.proto.KvServerpb;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.ArrayList;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Verifies {@link SnapshotEngineImpl} buildAndStream / receiveAndInstall
 * is atomic and consistent across all three CFs.
 *
 * <p>This is the engine-layer test — wired-through gRPC streaming would
 * land alongside the multi-region work that triggers it.
 */
final class SnapshotEngineTest {

    @TempDir Path src;
    @TempDir Path dst;
    private RocksStorageEngine srcEngine;
    private RocksStorageEngine dstEngine;
    private SnapshotEngineImpl srcSnap;
    private SnapshotEngineImpl dstSnap;

    @BeforeEach
    void open() throws Exception {
        srcEngine = RocksStorageEngine.open(src, KvConfig.EngineConfig.defaults());
        dstEngine = RocksStorageEngine.open(dst, KvConfig.EngineConfig.defaults());
        srcSnap = new SnapshotEngineImpl(srcEngine, src.resolve("snap"));
        dstSnap = new SnapshotEngineImpl(dstEngine, dst.resolve("snap"));
    }

    @AfterEach
    void close() {
        if (srcEngine != null) srcEngine.close();
        if (dstEngine != null) dstEngine.close();
    }

    @Test
    void snapshotRoundTripCarriesAllCfData() {
        // Populate src across all three CFs.
        try (var b = srcEngine.newWriteBatch()) {
            b.put(StorageEngine.Cf.DEFAULT, "k1".getBytes(), "vd1".getBytes());
            b.put(StorageEngine.Cf.DEFAULT, "k2".getBytes(), "vd2".getBytes());
            b.put(StorageEngine.Cf.LOCK, "k1".getBytes(), "vl1".getBytes());
            b.put(StorageEngine.Cf.WRITE, "k1".getBytes(), "vw1".getBytes());
            srcEngine.write(b, true);
        }

        // Drop unrelated stale data into dst so the install must wipe it.
        try (var b = dstEngine.newWriteBatch()) {
            b.put(StorageEngine.Cf.DEFAULT, "stale".getBytes(), "x".getBytes());
            b.put(StorageEngine.Cf.LOCK, "ghost".getBytes(), "x".getBytes());
            dstEngine.write(b, true);
        }

        // Build snapshot on src; pipe to dst.
        var chunks = new ArrayList<KvServerpb.SnapshotChunk>();
        srcSnap.buildAndStream(/* regionId= */ 1, /* term= */ 5, /* index= */ 100,
                new byte[]{0}, /* endKey= */ null, chunks::add);
        dstSnap.receiveAndInstall(1, chunks);

        // Every CF on dst now matches src exactly within range.
        assertThat(dstEngine.get(StorageEngine.Cf.DEFAULT, "k1".getBytes()))
                .containsExactly("vd1".getBytes());
        assertThat(dstEngine.get(StorageEngine.Cf.DEFAULT, "k2".getBytes()))
                .containsExactly("vd2".getBytes());
        assertThat(dstEngine.get(StorageEngine.Cf.LOCK, "k1".getBytes()))
                .containsExactly("vl1".getBytes());
        assertThat(dstEngine.get(StorageEngine.Cf.WRITE, "k1".getBytes()))
                .containsExactly("vw1".getBytes());

        // Stale data was wiped by the install's deleteRange.
        assertThat(dstEngine.get(StorageEngine.Cf.DEFAULT, "stale".getBytes())).isNull();
        assertThat(dstEngine.get(StorageEngine.Cf.LOCK, "ghost".getBytes())).isNull();
    }

    @Test
    void corruptChunkRejected() {
        try (var b = srcEngine.newWriteBatch()) {
            b.put(StorageEngine.Cf.DEFAULT, "k".getBytes(), "v".getBytes());
            srcEngine.write(b, true);
        }
        var chunks = new ArrayList<KvServerpb.SnapshotChunk>();
        srcSnap.buildAndStream(1, 1, 1, new byte[]{0}, null, chunks::add);

        // Tamper one chunk's data without updating its CRC.
        var tampered = new ArrayList<KvServerpb.SnapshotChunk>(chunks.size());
        for (int i = 0; i < chunks.size(); i++) {
            var c = chunks.get(i);
            if (i == 0 && c.getData().size() > 0) {
                var bad = c.toBuilder().setData(
                        com.google.protobuf.ByteString.copyFromUtf8("CORRUPTED")).build();
                tampered.add(bad);
            } else {
                tampered.add(c);
            }
        }
        assertThatThrownBy(() -> dstSnap.receiveAndInstall(1, tampered))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("CRC mismatch");
    }
}
