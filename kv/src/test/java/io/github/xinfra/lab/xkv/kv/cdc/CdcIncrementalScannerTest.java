package io.github.xinfra.lab.xkv.kv.cdc;

import io.github.xinfra.lab.xkv.kv.config.KvConfig;
import io.github.xinfra.lab.xkv.kv.engine.RocksStorageEngine;
import io.github.xinfra.lab.xkv.kv.engine.StorageEngine;
import io.github.xinfra.lab.xkv.kv.mvcc.MvccKey;
import io.github.xinfra.lab.xkv.kv.mvcc.MvccReader;
import io.github.xinfra.lab.xkv.kv.mvcc.MvccTxn;
import io.github.xinfra.lab.xkv.kv.mvcc.Write;
import io.github.xinfra.lab.xkv.proto.Cdcpb;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

final class CdcIncrementalScannerTest {

    @TempDir Path dataDir;
    private RocksStorageEngine engine;

    @BeforeEach
    void open() throws Exception {
        engine = RocksStorageEngine.open(dataDir, KvConfig.EngineConfig.defaults());
    }

    @AfterEach
    void close() { if (engine != null) engine.close(); }

    @Test
    void scanFindsCommittedPutsInWindow() {
        // Write 3 keys committed at ts 100, 200, 300.
        commit("a", "v1", 10, 100);
        commit("b", "v2", 20, 200);
        commit("c", "v3", 30, 300);

        // Scan (50, 250] should find "a" and "b" but not "c".
        try (var snap = engine.newSnapshot()) {
            var events = CdcIncrementalScanner.scan(
                    engine, snap, 1, null, null, 50, 250);
            assertThat(events).hasSize(2);
            assertThat(events.get(0).type()).isEqualTo(Cdcpb.Row.OpType.PUT);
            assertThat(new String(events.get(0).key())).isEqualTo("a");
            assertThat(new String(events.get(0).value())).isEqualTo("v1");
            assertThat(events.get(0).commitTs()).isEqualTo(100);
            assertThat(new String(events.get(1).key())).isEqualTo("b");
            assertThat(events.get(1).commitTs()).isEqualTo(200);
        }
    }

    @Test
    void scanFindsDeleteEvents() {
        commit("x", "val", 10, 100);
        commitDelete("x", 50, 200);

        // Scan (0, 250] should find the DELETE at commitTs=200.
        try (var snap = engine.newSnapshot()) {
            var events = CdcIncrementalScanner.scan(
                    engine, snap, 1, null, null, 0, 250);
            assertThat(events).hasSize(1);
            assertThat(events.get(0).type()).isEqualTo(Cdcpb.Row.OpType.DELETE);
            assertThat(new String(events.get(0).key())).isEqualTo("x");
            assertThat(events.get(0).commitTs()).isEqualTo(200);
        }
    }

    @Test
    void scanRespectsKeyRange() {
        commit("a", "v1", 10, 100);
        commit("b", "v2", 20, 100);
        commit("c", "v3", 30, 100);

        // Scan with key range [b, c) — should only find "b".
        try (var snap = engine.newSnapshot()) {
            var events = CdcIncrementalScanner.scan(
                    engine, snap, 1,
                    "b".getBytes(), "c".getBytes(),
                    0, 200);
            assertThat(events).hasSize(1);
            assertThat(new String(events.get(0).key())).isEqualTo("b");
        }
    }

    @Test
    void scanSkipsRollbackRecords() {
        // Write a PUT then a ROLLBACK for the same key at a later ts.
        commit("k", "val", 10, 100);
        writeRollback("k", 50);

        try (var snap = engine.newSnapshot()) {
            var events = CdcIncrementalScanner.scan(
                    engine, snap, 1, null, null, 0, 200);
            assertThat(events).hasSize(1);
            assertThat(events.get(0).type()).isEqualTo(Cdcpb.Row.OpType.PUT);
            assertThat(events.get(0).commitTs()).isEqualTo(100);
        }
    }

    @Test
    void scanReturnsOnlyLatestVersionPerKey() {
        commit("k", "old", 10, 100);
        commit("k", "new", 50, 200);

        // Scan (0, 300] — should only return the latest version (commitTs=200).
        try (var snap = engine.newSnapshot()) {
            var events = CdcIncrementalScanner.scan(
                    engine, snap, 1, null, null, 0, 300);
            assertThat(events).hasSize(1);
            assertThat(new String(events.get(0).value())).isEqualTo("new");
            assertThat(events.get(0).commitTs()).isEqualTo(200);
        }
    }

    @Test
    void scanEmptyRangeReturnsNothing() {
        commit("a", "v", 10, 100);

        try (var snap = engine.newSnapshot()) {
            var events = CdcIncrementalScanner.scan(
                    engine, snap, 1, null, null, 100, 200);
            assertThat(events).isEmpty();
        }
    }

    @Test
    void scanHandlesShortValues() {
        // Use a short value (< 64 bytes) that gets inlined in the Write record.
        byte[] key = "k".getBytes();
        byte[] val = "short".getBytes();
        var b = engine.newWriteBatch();
        b.put(StorageEngine.Cf.WRITE, MvccKey.encode(key, 100),
                Write.put(10, val).encode());
        engine.write(b, false);
        b.close();

        try (var snap = engine.newSnapshot()) {
            var events = CdcIncrementalScanner.scan(
                    engine, snap, 1, null, null, 0, 200);
            assertThat(events).hasSize(1);
            assertThat(new String(events.get(0).value())).isEqualTo("short");
        }
    }

    // ---- helpers ----

    private void commit(String key, String value, long startTs, long commitTs) {
        byte[] k = key.getBytes(StandardCharsets.UTF_8);
        byte[] v = value.getBytes(StandardCharsets.UTF_8);
        var b = engine.newWriteBatch();
        b.put(StorageEngine.Cf.DEFAULT, MvccKey.encode(k, startTs), v);
        b.put(StorageEngine.Cf.WRITE, MvccKey.encode(k, commitTs),
                Write.put(startTs).encode());
        engine.write(b, false);
        b.close();
    }

    private void commitDelete(String key, long startTs, long commitTs) {
        byte[] k = key.getBytes(StandardCharsets.UTF_8);
        var b = engine.newWriteBatch();
        b.put(StorageEngine.Cf.WRITE, MvccKey.encode(k, commitTs),
                Write.delete(startTs).encode());
        engine.write(b, false);
        b.close();
    }

    private void writeRollback(String key, long startTs) {
        byte[] k = key.getBytes(StandardCharsets.UTF_8);
        var b = engine.newWriteBatch();
        b.put(StorageEngine.Cf.WRITE, MvccKey.encode(k, startTs),
                Write.rollback(startTs).encode());
        engine.write(b, false);
        b.close();
    }
}
