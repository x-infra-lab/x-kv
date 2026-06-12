package io.github.xinfra.lab.xkv.kv.mvcc;

import io.github.xinfra.lab.xkv.kv.config.KvConfig;
import io.github.xinfra.lab.xkv.kv.engine.RocksStorageEngine;
import io.github.xinfra.lab.xkv.kv.engine.StorageEngine;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Phase 2 verification: MvccReader implements snapshot read correctly,
 * including the v1 fixes (ROLLBACK skip, LOCK lock not blocking reads).
 */
final class MvccReaderTest {

    @TempDir Path dataDir;
    private RocksStorageEngine engine;

    @BeforeEach
    void open() throws Exception {
        engine = RocksStorageEngine.open(dataDir, KvConfig.EngineConfig.defaults());
    }

    @AfterEach
    void close() { if (engine != null) engine.close(); }

    @Test
    void getReturnsLatestVisibleVersion() {
        // Three commits at 100, 200, 300. Read at 250 sees the 200-version.
        commitPut("k", "v100", 50, 100);
        commitPut("k", "v200", 150, 200);
        commitPut("k", "v300", 250, 300);

        var r = new MvccReader(engine, null, false);
        assertThat(r.get("k".getBytes(), 250).map(String::new)).contains("v200");
        assertThat(r.get("k".getBytes(), 99).map(String::new)).isEmpty();
        assertThat(r.get("k".getBytes(), 300).map(String::new)).contains("v300");
    }

    @Test
    void getRespectsDelete() {
        commitPut("k", "v", 10, 20);
        commitDelete("k", 30, 40);
        var r = new MvccReader(engine, null, false);
        assertThat(r.get("k".getBytes(), 50).map(String::new)).isEmpty();
        assertThat(r.get("k".getBytes(), 25).map(String::new)).contains("v");
    }

    @Test
    void rollbackBetweenRealCommitsIsSkipped() {
        // PUT @ 100, ROLLBACK @ 150, then... reading at 200 should see PUT @ 100.
        commitPut("k", "real", 50, 100);
        writeRollback("k", 150);
        var r = new MvccReader(engine, null, false);
        assertThat(r.get("k".getBytes(), 200).map(String::new)).contains("real");
    }

    @Test
    void putLockBlocksRead() {
        commitPut("k", "old", 10, 20);
        writeLock("k", Lock.Type.PUT, 50, "p");

        var r = new MvccReader(engine, null, false);
        // Reading at 100 sees the lock (lock.startTs=50 <= 100) → blocked.
        assertThatThrownBy(() -> r.get("k".getBytes(), 100))
                .isInstanceOf(KeyLockedException.class);
        // Reading at 30 (before lock.startTs) returns the old value.
        assertThat(r.get("k".getBytes(), 30).map(String::new)).contains("old");
    }

    @Test
    void lockTypeLockDoesNotBlockReads() {
        // SELECT ... FOR SHARE-style LOCK: type=LOCK, must not block snapshot reads.
        commitPut("k", "real", 10, 20);
        writeLock("k", Lock.Type.LOCK, 50, "p");
        var r = new MvccReader(engine, null, false);
        assertThat(r.get("k".getBytes(), 100).map(String::new)).contains("real");
    }

    @Test
    void scanReturnsKeysInRange() {
        commitPut("a", "va", 10, 20);
        commitPut("b", "vb", 10, 20);
        commitPut("c", "vc", 10, 20);
        commitPut("d", "vd", 10, 20);

        var r = new MvccReader(engine, null, false);
        var pairs = r.scan("b".getBytes(), "d".getBytes(), 100, 100);
        assertThat(pairs).hasSize(2);
        assertThat(new String(pairs.get(0).key())).isEqualTo("b");
        assertThat(new String(pairs.get(1).key())).isEqualTo("c");
    }

    @Test
    void scanRespectsLimit() {
        for (int i = 0; i < 10; i++) commitPut("k" + i, "v" + i, 10, 20);
        var r = new MvccReader(engine, null, false);
        var pairs = r.scan("k0".getBytes(), null, 3, 100);
        assertThat(pairs).hasSize(3);
    }

    @Test
    void findWriteByStartTsLocatesExactVersion() {
        commitPut("k", "v100", 50, 100);
        commitPut("k", "v200", 150, 200);
        var r = new MvccReader(engine, null, false);
        var w = r.findWriteByStartTs("k".getBytes(), 50);
        assertThat(w).isPresent();
        assertThat(w.get().type()).isEqualTo(Write.Type.PUT);
    }

    // ---- helpers ----

    private void commitPut(String userKey, String value, long startTs, long commitTs) {
        try (var b = engine.newWriteBatch()) {
            b.put(StorageEngine.Cf.DEFAULT, MvccKey.encode(userKey.getBytes(), startTs), value.getBytes());
            b.put(StorageEngine.Cf.WRITE, MvccKey.encode(userKey.getBytes(), commitTs),
                    Write.put(startTs).encode());
            engine.write(b, false);
        }
    }

    private void commitDelete(String userKey, long startTs, long commitTs) {
        try (var b = engine.newWriteBatch()) {
            b.put(StorageEngine.Cf.WRITE, MvccKey.encode(userKey.getBytes(), commitTs),
                    Write.delete(startTs).encode());
            engine.write(b, false);
        }
    }

    private void writeRollback(String userKey, long startTs) {
        try (var b = engine.newWriteBatch()) {
            b.put(StorageEngine.Cf.WRITE, MvccKey.encode(userKey.getBytes(), startTs),
                    Write.rollback(startTs).encode());
            engine.write(b, false);
        }
    }

    private void writeLock(String userKey, Lock.Type type, long startTs, String primary) {
        var lock = Lock.builder()
                .type(type)
                .primary(primary.getBytes())
                .startTs(startTs)
                .ttlMs(3_000)
                .build();
        try (var b = engine.newWriteBatch()) {
            b.put(StorageEngine.Cf.LOCK, MvccKey.lockKey(userKey.getBytes()), lock.encode());
            engine.write(b, false);
        }
    }
}
