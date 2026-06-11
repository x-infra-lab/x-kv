package io.github.xinfra.lab.xkv.kv.engine;

import io.github.xinfra.lab.xkv.kv.config.KvConfig;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.charset.StandardCharsets;
import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Phase 1 verification: the v2 atomicity contract holds at the engine layer.
 */
final class RocksStorageEngineTest {

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
    void fourCfsAreIsolated() {
        try (var batch = engine.newWriteBatch()) {
            batch.put(StorageEngine.Cf.DEFAULT, "k".getBytes(), "default-v".getBytes());
            batch.put(StorageEngine.Cf.LOCK,    "k".getBytes(), "lock-v".getBytes());
            batch.put(StorageEngine.Cf.WRITE,   "k".getBytes(), "write-v".getBytes());
            batch.put(StorageEngine.Cf.RAFT,    "k".getBytes(), "raft-v".getBytes());
            engine.write(batch, true);
        }
        assertThat(engine.get(StorageEngine.Cf.DEFAULT, "k".getBytes())).isEqualTo("default-v".getBytes());
        assertThat(engine.get(StorageEngine.Cf.LOCK,    "k".getBytes())).isEqualTo("lock-v".getBytes());
        assertThat(engine.get(StorageEngine.Cf.WRITE,   "k".getBytes())).isEqualTo("write-v".getBytes());
        assertThat(engine.get(StorageEngine.Cf.RAFT,    "k".getBytes())).isEqualTo("raft-v".getBytes());
    }

    @Test
    void writeBatchIsAtomicAcrossCfs() {
        // Inv-1: business mutation + raft applied-index together.
        try (var batch = engine.newWriteBatch()) {
            batch.put(StorageEngine.Cf.DEFAULT, "user:1".getBytes(), "alice".getBytes());
            batch.put(StorageEngine.Cf.RAFT,    "applied".getBytes(), "42".getBytes());
            engine.write(batch, true);
        }
        assertThat(engine.get(StorageEngine.Cf.DEFAULT, "user:1".getBytes()))
                .as("the kv mutation").isEqualTo("alice".getBytes());
        assertThat(engine.get(StorageEngine.Cf.RAFT, "applied".getBytes()))
                .as("the raft applied marker").isEqualTo("42".getBytes());
    }

    @Test
    void snapshotProvidesCrossCfConsistentView() throws Exception {
        // Inv-2: a snapshot taken at one instant gives a consistent view across
        // all CFs. After we open the snapshot, subsequent writes to ANY cf
        // must be invisible to iterators bound to it.
        try (var b = engine.newWriteBatch()) {
            b.put(StorageEngine.Cf.DEFAULT, "k1".getBytes(), "v1".getBytes());
            b.put(StorageEngine.Cf.LOCK,    "k1".getBytes(), "lock1".getBytes());
            b.put(StorageEngine.Cf.WRITE,   "k1".getBytes(), "write1".getBytes());
            engine.write(b, false);
        }
        try (var snap = engine.newSnapshot()) {
            // After the snapshot we mutate every CF.
            try (var b = engine.newWriteBatch()) {
                b.put(StorageEngine.Cf.DEFAULT, "k1".getBytes(), "v2".getBytes());
                b.put(StorageEngine.Cf.LOCK,    "k1".getBytes(), "lock2".getBytes());
                b.put(StorageEngine.Cf.WRITE,   "k1".getBytes(), "write2".getBytes());
                engine.write(b, false);
            }
            var ro = engine.newReadOptions().snapshot(snap);
            try (var it = engine.newIterator(StorageEngine.Cf.DEFAULT, ro)) {
                it.seek("k1".getBytes());
                assertThat(it.isValid()).isTrue();
                assertThat(it.value()).isEqualTo("v1".getBytes());
            }
            try (var it = engine.newIterator(StorageEngine.Cf.LOCK, ro)) {
                it.seek("k1".getBytes());
                assertThat(it.value()).isEqualTo("lock1".getBytes());
            }
            try (var it = engine.newIterator(StorageEngine.Cf.WRITE, ro)) {
                it.seek("k1".getBytes());
                assertThat(it.value()).isEqualTo("write1".getBytes());
            }
        }
        // Outside the snapshot we see the new values.
        assertThat(engine.get(StorageEngine.Cf.DEFAULT, "k1".getBytes()))
                .isEqualTo("v2".getBytes());
    }

    @Test
    void deleteRangeDropsContiguousKeys() {
        try (var b = engine.newWriteBatch()) {
            for (int i = 0; i < 100; i++) {
                b.put(StorageEngine.Cf.DEFAULT,
                        String.format("k%03d", i).getBytes(StandardCharsets.UTF_8),
                        "v".getBytes());
            }
            engine.write(b, false);
        }
        try (var b = engine.newWriteBatch()) {
            engine.deleteRange(b, StorageEngine.Cf.DEFAULT, "k010".getBytes(), "k090".getBytes());
            engine.write(b, true);
        }
        assertThat(engine.get(StorageEngine.Cf.DEFAULT, "k009".getBytes())).isNotNull();
        assertThat(engine.get(StorageEngine.Cf.DEFAULT, "k010".getBytes())).isNull();
        assertThat(engine.get(StorageEngine.Cf.DEFAULT, "k050".getBytes())).isNull();
        assertThat(engine.get(StorageEngine.Cf.DEFAULT, "k089".getBytes())).isNull();
        assertThat(engine.get(StorageEngine.Cf.DEFAULT, "k090".getBytes())).isNotNull();
    }

    @Test
    void multiGetReturnsValuesInOrder() {
        try (var b = engine.newWriteBatch()) {
            b.put(StorageEngine.Cf.DEFAULT, "a".getBytes(), "1".getBytes());
            b.put(StorageEngine.Cf.DEFAULT, "b".getBytes(), "2".getBytes());
            b.put(StorageEngine.Cf.DEFAULT, "c".getBytes(), "3".getBytes());
            engine.write(b, false);
        }
        var values = engine.multiGet(StorageEngine.Cf.DEFAULT,
                java.util.List.of("c".getBytes(), "a".getBytes(), "missing".getBytes(), "b".getBytes()));
        assertThat(values).hasSize(4);
        assertThat(values.get(0)).isEqualTo("3".getBytes());
        assertThat(values.get(1)).isEqualTo("1".getBytes());
        assertThat(values.get(2)).isNull();
        assertThat(values.get(3)).isEqualTo("2".getBytes());
    }

    @Test
    void reopenPersistsData() throws Exception {
        try (var b = engine.newWriteBatch()) {
            b.put(StorageEngine.Cf.DEFAULT, "persist".getBytes(), "yes".getBytes());
            engine.write(b, true);   // sync = true
        }
        engine.close();
        engine = RocksStorageEngine.open(dataDir, KvConfig.EngineConfig.defaults());
        assertThat(engine.get(StorageEngine.Cf.DEFAULT, "persist".getBytes()))
                .isEqualTo("yes".getBytes());
    }
}
