package io.github.xinfra.lab.xkv.kv.mvcc;

import io.github.xinfra.lab.xkv.kv.config.KvConfig;
import io.github.xinfra.lab.xkv.kv.engine.RocksStorageEngine;
import io.github.xinfra.lab.xkv.kv.engine.StorageEngine;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Phase 2 verification: server-side Percolator state machine semantics.
 * Tests run synchronously on a single engine — no raft involved — so we
 * exercise pure conflict-detection / lock-handling logic.
 */
final class MvccTxnTest {

    @TempDir Path dataDir;
    private RocksStorageEngine engine;

    @BeforeEach
    void open() throws Exception {
        engine = RocksStorageEngine.open(dataDir, KvConfig.EngineConfig.defaults());
    }

    @AfterEach
    void close() { if (engine != null) engine.close(); }

    @Test
    void prewriteHappyPath() {
        try (var b = engine.newWriteBatch()) {
            var txn = newTxn(b);
            var check = txn.checkPrewrite("k".getBytes(), 100, MvccTxn.Op.PUT);
            assertThat(check).isInstanceOf(MvccTxn.PrewriteOk.class);
            txn.writePrewrite("k".getBytes(), "v".getBytes(),
                    MvccTxn.Op.PUT, "k".getBytes(),
                    100, 3000, 1, 0, 0, false, List.of());
            engine.write(b, true);
        }
        // Lock CF should now hold one entry.
        assertThat(engine.get(StorageEngine.Cf.LOCK, MvccKey.lockKey("k".getBytes()))).isNotNull();
    }

    @Test
    void prewriteWriteConflictBlocksLaterTxn() {
        // Earlier txn commits at ts=50.
        commitDirect("k", "v50", 30, 50);
        try (var b = engine.newWriteBatch()) {
            var txn = newTxn(b);
            // Later txn with startTs=40 sees a commit at ts=50 — write conflict.
            var check = txn.checkPrewrite("k".getBytes(), 40, MvccTxn.Op.PUT);
            assertThat(check).isInstanceOf(MvccTxn.PrewriteWriteConflict.class);
            engine.write(b, true);
        }
    }

    @Test
    void prewriteIdempotentOnSameTxnLock() {
        // First prewrite.
        try (var b = engine.newWriteBatch()) {
            var txn = newTxn(b);
            txn.writePrewrite("k".getBytes(), "v".getBytes(),
                    MvccTxn.Op.PUT, "k".getBytes(), 100, 3000, 1, 0, 0, false, List.of());
            engine.write(b, true);
        }
        // Re-prewrite at same startTs is idempotent.
        try (var b = engine.newWriteBatch()) {
            var txn = newTxn(b);
            var check = txn.checkPrewrite("k".getBytes(), 100, MvccTxn.Op.PUT);
            assertThat(check).isInstanceOf(MvccTxn.PrewriteAlready.class);
            engine.write(b, true);
        }
    }

    @Test
    void prewriteFailsIfDifferentTxnHoldsLock() {
        // Other txn writes a lock at startTs=100.
        try (var b = engine.newWriteBatch()) {
            newTxn(b).writePrewrite("k".getBytes(), "v".getBytes(),
                    MvccTxn.Op.PUT, "k".getBytes(), 100, 3000, 1, 0, 0, false, List.of());
            engine.write(b, true);
        }
        // Different startTs → KeyLocked.
        try (var b = engine.newWriteBatch()) {
            var check = newTxn(b).checkPrewrite("k".getBytes(), 200, MvccTxn.Op.PUT);
            assertThat(check).isInstanceOf(MvccTxn.PrewriteKeyLocked.class);
        }
    }

    @Test
    void commitWritesWriteRecordAndDeletesLock() {
        // Prewrite.
        try (var b = engine.newWriteBatch()) {
            newTxn(b).writePrewrite("k".getBytes(), "v".getBytes(),
                    MvccTxn.Op.PUT, "k".getBytes(), 100, 3000, 1, 0, 0, false, List.of());
            engine.write(b, true);
        }
        // Commit.
        try (var b = engine.newWriteBatch()) {
            var txn = newTxn(b);
            var oc = txn.commit("k".getBytes(), 100, 150);
            assertThat(oc).isInstanceOf(MvccTxn.CommitCommitted.class);
            engine.write(b, true);
        }
        // Lock gone, write CF has commit record at ts=150.
        assertThat(engine.get(StorageEngine.Cf.LOCK, MvccKey.lockKey("k".getBytes()))).isNull();
        var r = new MvccReader(engine, null, false);
        assertThat(r.get("k".getBytes(), 200).map(String::new)).contains("v");
    }

    @Test
    void commitRejectsCommitTsLeStartTs() {
        try (var b = engine.newWriteBatch()) {
            newTxn(b).writePrewrite("k".getBytes(), "v".getBytes(),
                    MvccTxn.Op.PUT, "k".getBytes(), 100, 3000, 1, 0, 0, false, List.of());
            engine.write(b, true);
        }
        try (var b = engine.newWriteBatch()) {
            var oc = newTxn(b).commit("k".getBytes(), 100, 100);
            assertThat(oc).isInstanceOf(MvccTxn.CommitInvalidCommitTs.class);
        }
    }

    @Test
    void commitRejectsCommitTsBelowMinCommitTs() {
        // Lock has minCommitTs = 250. commit at 200 should fail.
        try (var b = engine.newWriteBatch()) {
            newTxn(b).writePrewrite("k".getBytes(), "v".getBytes(),
                    MvccTxn.Op.PUT, "k".getBytes(), 100, 3000, 1,
                    /* minCommitTs= */ 250, 0, true, List.of());
            engine.write(b, true);
        }
        try (var b = engine.newWriteBatch()) {
            var oc = newTxn(b).commit("k".getBytes(), 100, 200);
            assertThat(oc).isInstanceOf(MvccTxn.CommitInvalidCommitTs.class);
        }
    }

    @Test
    void commitIdempotentReplay() {
        try (var b = engine.newWriteBatch()) {
            newTxn(b).writePrewrite("k".getBytes(), "v".getBytes(),
                    MvccTxn.Op.PUT, "k".getBytes(), 100, 3000, 1, 0, 0, false, List.of());
            engine.write(b, true);
        }
        try (var b = engine.newWriteBatch()) {
            newTxn(b).commit("k".getBytes(), 100, 150);
            engine.write(b, true);
        }
        // Replay commit — already committed, idempotent OK.
        try (var b = engine.newWriteBatch()) {
            var oc = newTxn(b).commit("k".getBytes(), 100, 150);
            assertThat(oc).isInstanceOf(MvccTxn.CommitAlreadyCommitted.class);
        }
    }

    @Test
    void rollbackWritesRollbackRecordAndCleansLock() {
        try (var b = engine.newWriteBatch()) {
            newTxn(b).writePrewrite("k".getBytes(), "v".getBytes(),
                    MvccTxn.Op.PUT, "k".getBytes(), 100, 3000, 1, 0, 0, false, List.of());
            engine.write(b, true);
        }
        try (var b = engine.newWriteBatch()) {
            var oc = newTxn(b).rollback("k".getBytes(), 100);
            assertThat(oc).isInstanceOf(MvccTxn.RollbackOk.class);
            engine.write(b, true);
        }
        // Lock gone; default CF entry gone; ROLLBACK record present.
        assertThat(engine.get(StorageEngine.Cf.LOCK, MvccKey.lockKey("k".getBytes()))).isNull();
        assertThat(engine.get(StorageEngine.Cf.DEFAULT,
                MvccKey.encode("k".getBytes(), 100))).isNull();

        var r = new MvccReader(engine, null, false);
        var w = r.findWriteByStartTs("k".getBytes(), 100);
        assertThat(w).isPresent();
        assertThat(w.get().type()).isEqualTo(Write.Type.ROLLBACK);
    }

    @Test
    void rollbackOfAlreadyCommittedReturnsAlreadyCommitted() {
        try (var b = engine.newWriteBatch()) {
            newTxn(b).writePrewrite("k".getBytes(), "v".getBytes(),
                    MvccTxn.Op.PUT, "k".getBytes(), 100, 3000, 1, 0, 0, false, List.of());
            engine.write(b, true);
        }
        try (var b = engine.newWriteBatch()) {
            newTxn(b).commit("k".getBytes(), 100, 150);
            engine.write(b, true);
        }
        try (var b = engine.newWriteBatch()) {
            var oc = newTxn(b).rollback("k".getBytes(), 100);
            assertThat(oc).isInstanceOf(MvccTxn.RollbackAlreadyCommitted.class);
            assertThat(((MvccTxn.RollbackAlreadyCommitted) oc).commitTs()).isEqualTo(150);
        }
    }

    @Test
    void prewriteAfterSameTxnRollbackFails() {
        // Txn 100 was rolled back externally.
        try (var b = engine.newWriteBatch()) {
            b.put(StorageEngine.Cf.WRITE,
                    MvccKey.encode("k".getBytes(), 100),
                    Write.rollback(100).encode());
            engine.write(b, true);
        }
        try (var b = engine.newWriteBatch()) {
            var oc = newTxn(b).checkPrewrite("k".getBytes(), 100, MvccTxn.Op.PUT);
            assertThat(oc).isInstanceOf(MvccTxn.PrewriteSelfRolledBack.class);
        }
    }

    @Test
    void shortValueIsInlinedAtCommit() {
        var sv = "tiny".getBytes();
        try (var b = engine.newWriteBatch()) {
            newTxn(b).writePrewrite("k".getBytes(), sv,
                    MvccTxn.Op.PUT, "k".getBytes(), 100, 3000, 1, 0, 0, false, List.of());
            engine.write(b, true);
        }
        try (var b = engine.newWriteBatch()) {
            newTxn(b).commit("k".getBytes(), 100, 150);
            engine.write(b, true);
        }
        // After commit the value should be inlined; default CF entry deleted.
        assertThat(engine.get(StorageEngine.Cf.DEFAULT,
                MvccKey.encode("k".getBytes(), 100))).isNull();
        // Read still works (because Write record carries the inlined value).
        var r = new MvccReader(engine, null, false);
        assertThat(r.get("k".getBytes(), 200).map(String::new)).contains("tiny");
    }

    @Test
    void pessimisticLockAcquiresAndReLockRefreshes() {
        try (var b = engine.newWriteBatch()) {
            var oc = newTxn(b).acquirePessimisticLock("k".getBytes(),
                    "k".getBytes(), 100, 110, 3000);
            assertThat(oc).isInstanceOf(MvccTxn.PessimisticAcquired.class);
            engine.write(b, true);
        }
        // Re-lock by same txn refreshes (no error).
        try (var b = engine.newWriteBatch()) {
            var oc = newTxn(b).acquirePessimisticLock("k".getBytes(),
                    "k".getBytes(), 100, 120, 3000);
            assertThat(oc).isInstanceOf(MvccTxn.PessimisticAcquired.class);
        }
        // Different txn → KeyLocked.
        try (var b = engine.newWriteBatch()) {
            var oc = newTxn(b).acquirePessimisticLock("k".getBytes(),
                    "k".getBytes(), 200, 210, 3000);
            assertThat(oc).isInstanceOf(MvccTxn.PessimisticKeyLocked.class);
        }
    }

    // ---- helpers ----

    private MvccTxn newTxn(StorageEngine.WriteBatch batch) {
        return new MvccTxn(batch, new MvccReader(engine, null, false));
    }

    private void commitDirect(String userKey, String value, long startTs, long commitTs) {
        try (var b = engine.newWriteBatch()) {
            b.put(StorageEngine.Cf.DEFAULT,
                    MvccKey.encode(userKey.getBytes(), startTs), value.getBytes());
            b.put(StorageEngine.Cf.WRITE,
                    MvccKey.encode(userKey.getBytes(), commitTs),
                    Write.put(startTs).encode());
            engine.write(b, false);
        }
    }
}
