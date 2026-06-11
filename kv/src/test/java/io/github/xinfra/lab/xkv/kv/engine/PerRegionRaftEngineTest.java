package io.github.xinfra.lab.xkv.kv.engine;

import io.github.xinfra.lab.xkv.kv.config.KvConfig;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Phase 1 verification: per-region Raft state lives in the shared engine,
 * and Inv-1 (single-batch atomicity) is preserved.
 */
final class PerRegionRaftEngineTest {

    @TempDir Path dataDir;
    private RocksStorageEngine engine;

    @BeforeEach
    void open() throws Exception {
        engine = RocksStorageEngine.open(dataDir, KvConfig.EngineConfig.defaults());
    }

    @AfterEach
    void close() {
        if (engine != null) engine.close();
    }

    @Test
    void appendAndReadEntries() {
        var r = new PerRegionRaftEngine(engine, 7);
        try (var b = engine.newWriteBatch()) {
            r.appendEntries(b, new byte[][] { "e1".getBytes(), "e2".getBytes(), "e3".getBytes() });
            engine.write(b, true);
        }
        assertThat(r.firstIndex()).isEqualTo(1);
        assertThat(r.lastIndex()).isEqualTo(3);
        assertThat(r.entryAt(1)).isEqualTo("e1".getBytes());
        assertThat(r.entryAt(3)).isEqualTo("e3".getBytes());
        assertThat(r.entryAt(4)).isNull();
    }

    @Test
    void hardStateAndAppliedAreAtomicWithEntries() {
        // Inv-1 in action: log + hard state + applied + dedup + business in
        // one batch.
        var r = new PerRegionRaftEngine(engine, 1);
        try (var b = engine.newWriteBatch()) {
            r.appendEntries(b, new byte[][] { "raft-payload".getBytes() });
            r.saveHardState(5, 99, 1, b);
            r.saveAppliedIndex(1, b);
            r.recordDedup(123, 7, b);
            // pretend the entry is a Put on the default CF
            b.put(StorageEngine.Cf.DEFAULT, "user:1".getBytes(), "alice".getBytes());
            engine.write(b, true);
        }
        assertThat(r.lastIndex()).isEqualTo(1);
        assertThat(r.appliedIndex()).isEqualTo(1);
        assertThat(r.currentTerm()).isEqualTo(5);
        assertThat(r.votedFor()).isEqualTo(99);
        assertThat(r.lastDedupReqId(123)).isEqualTo(7);
        assertThat(engine.get(StorageEngine.Cf.DEFAULT, "user:1".getBytes()))
                .isEqualTo("alice".getBytes());
    }

    @Test
    void compactDropsEarlyEntries() {
        var r = new PerRegionRaftEngine(engine, 1);
        try (var b = engine.newWriteBatch()) {
            byte[][] entries = new byte[10][];
            for (int i = 0; i < 10; i++) entries[i] = ("e" + (i + 1)).getBytes();
            r.appendEntries(b, entries);
            engine.write(b, true);
        }
        try (var b = engine.newWriteBatch()) {
            r.compactLog(5, b);
            engine.write(b, true);
        }
        assertThat(r.firstIndex()).isEqualTo(6);
        assertThat(r.entryAt(5)).isNull();
        assertThat(r.entryAt(6)).isEqualTo("e6".getBytes());
        assertThat(r.lastIndex()).isEqualTo(10);
    }

    @Test
    void truncateAfterDropsTail() {
        var r = new PerRegionRaftEngine(engine, 1);
        try (var b = engine.newWriteBatch()) {
            byte[][] entries = new byte[5][];
            for (int i = 0; i < 5; i++) entries[i] = ("e" + (i + 1)).getBytes();
            r.appendEntries(b, entries);
            engine.write(b, true);
        }
        try (var b = engine.newWriteBatch()) {
            r.truncateAfter(3, b);
            engine.write(b, true);
        }
        assertThat(r.lastIndex()).isEqualTo(3);
        assertThat(r.entryAt(3)).isEqualTo("e3".getBytes());
        assertThat(r.entryAt(4)).isNull();
    }

    @Test
    void multipleRegionsAreIsolated() {
        var r1 = new PerRegionRaftEngine(engine, 1);
        var r2 = new PerRegionRaftEngine(engine, 2);

        try (var b = engine.newWriteBatch()) {
            r1.appendEntries(b, new byte[][] { "r1-e1".getBytes(), "r1-e2".getBytes() });
            r2.appendEntries(b, new byte[][] { "r2-e1".getBytes() });
            r1.saveAppliedIndex(2, b);
            r2.saveAppliedIndex(1, b);
            engine.write(b, true);
        }
        assertThat(r1.lastIndex()).isEqualTo(2);
        assertThat(r1.appliedIndex()).isEqualTo(2);
        assertThat(r2.lastIndex()).isEqualTo(1);
        assertThat(r2.appliedIndex()).isEqualTo(1);
        assertThat(r1.entryAt(1)).isEqualTo("r1-e1".getBytes());
        assertThat(r2.entryAt(1)).isEqualTo("r2-e1".getBytes());
    }

    @Test
    void reloadRestoresFullState() throws Exception {
        // Persist some state.
        var r = new PerRegionRaftEngine(engine, 42);
        try (var b = engine.newWriteBatch()) {
            r.appendEntries(b, new byte[][] { "x1".getBytes(), "x2".getBytes(), "x3".getBytes() });
            r.saveHardState(11, 22, 3, b);
            r.saveAppliedIndex(2, b);
            r.recordDedup(1L, 100L, b);
            r.recordDedup(2L, 200L, b);
            engine.write(b, true);
        }
        // Reopen the engine entirely.
        engine.close();
        engine = RocksStorageEngine.open(dataDir, KvConfig.EngineConfig.defaults());

        var r2 = new PerRegionRaftEngine(engine, 42);
        assertThat(r2.lastIndex()).isEqualTo(3);
        assertThat(r2.firstIndex()).isEqualTo(1);
        assertThat(r2.appliedIndex()).isEqualTo(2);
        assertThat(r2.currentTerm()).isEqualTo(11);
        assertThat(r2.votedFor()).isEqualTo(22);
        assertThat(r2.commitIndex()).isEqualTo(3);
        assertThat(r2.lastDedupReqId(1L)).isEqualTo(100L);
        assertThat(r2.lastDedupReqId(2L)).isEqualTo(200L);
        assertThat(r2.lastDedupReqId(999L)).isEqualTo(0L);
    }

    @Test
    void destroyWipesEverythingForOneRegion() {
        var r = new PerRegionRaftEngine(engine, 1);
        var keep = new PerRegionRaftEngine(engine, 2);
        try (var b = engine.newWriteBatch()) {
            r.appendEntries(b, new byte[][] { "to-destroy".getBytes() });
            keep.appendEntries(b, new byte[][] { "to-keep".getBytes() });
            engine.write(b, true);
        }
        r.destroy();
        var rReopen = new PerRegionRaftEngine(engine, 1);
        assertThat(rReopen.lastIndex()).isEqualTo(0);
        assertThat(rReopen.entryAt(1)).isNull();
        var keepReopen = new PerRegionRaftEngine(engine, 2);
        assertThat(keepReopen.lastIndex()).isEqualTo(1);
        assertThat(keepReopen.entryAt(1)).isEqualTo("to-keep".getBytes());
    }
}
