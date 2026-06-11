package io.github.xinfra.lab.xkv.test;

import io.github.xinfra.lab.raft.proto.Eraftpb;
import io.github.xinfra.lab.xkv.kv.config.KvConfig;
import io.github.xinfra.lab.xkv.kv.engine.PerRegionRaftEngine;
import io.github.xinfra.lab.xkv.kv.engine.RocksStorageEngine;
import io.github.xinfra.lab.xkv.kv.engine.SnapshotEngineImpl;
import io.github.xinfra.lab.xkv.kv.engine.StorageEngine;
import io.github.xinfra.lab.xkv.kv.raft.RegionRaftStorage;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Verifies the {@code createSnapshot}/{@code applySnapshot} wiring on
 * {@link RegionRaftStorage} carries user-data CF content end-to-end via
 * the snapshot's {@code data} envelope.
 *
 * <p>Without this wiring, a follower that recovers via raft snapshot has
 * an empty business state — the v1 gap closed here.
 */
final class RaftSnapshotWiringTest {

    @TempDir Path leaderDir;
    @TempDir Path followerDir;
    private RocksStorageEngine leaderEngine;
    private RocksStorageEngine followerEngine;
    private PerRegionRaftEngine leaderRaft;
    private PerRegionRaftEngine followerRaft;
    private SnapshotEngineImpl leaderSnap;
    private SnapshotEngineImpl followerSnap;
    private RegionRaftStorage leaderStorage;
    private RegionRaftStorage followerStorage;

    @BeforeEach
    void open() throws Exception {
        leaderEngine = RocksStorageEngine.open(leaderDir, KvConfig.EngineConfig.defaults());
        followerEngine = RocksStorageEngine.open(followerDir, KvConfig.EngineConfig.defaults());
        leaderRaft = new PerRegionRaftEngine(leaderEngine, 1);
        followerRaft = new PerRegionRaftEngine(followerEngine, 1);
        leaderSnap = new SnapshotEngineImpl(leaderEngine, leaderDir.resolve("snap"));
        followerSnap = new SnapshotEngineImpl(followerEngine, followerDir.resolve("snap"));
        leaderStorage = new RegionRaftStorage(leaderEngine, leaderRaft, leaderSnap);
        followerStorage = new RegionRaftStorage(followerEngine, followerRaft, followerSnap);
    }

    @AfterEach
    void close() {
        if (leaderEngine != null) leaderEngine.close();
        if (followerEngine != null) followerEngine.close();
    }

    @Test
    void createSnapshotCarriesUserDataIntoApplySnapshot() throws Exception {
        // Seed leader with some MVCC-ish data across all three CFs.
        try (var b = leaderEngine.newWriteBatch()) {
            b.put(StorageEngine.Cf.DEFAULT, "alice".getBytes(), "100".getBytes());
            b.put(StorageEngine.Cf.DEFAULT, "bob".getBytes(), "200".getBytes());
            b.put(StorageEngine.Cf.WRITE, "alice".getBytes(), "wv".getBytes());
            b.put(StorageEngine.Cf.LOCK, "carol".getBytes(), "lk".getBytes());
            leaderEngine.write(b, true);
        }
        // The snapshot path reads a term from the log at appliedIndex —
        // append a real entry at index 1 with term 2 so createSnapshot has
        // something to look at.
        var entry = Eraftpb.Entry.newBuilder().setIndex(1).setTerm(2).build();
        try (var b = leaderEngine.newWriteBatch()) {
            leaderRaft.appendEntries(b, new byte[][]{entry.toByteArray()});
            leaderRaft.saveAppliedIndex(1, b);
            leaderEngine.write(b, true);
        }

        // Leader generates a snapshot via the storage path. The wire here is
        // the same one raft library uses internally.
        var snap = leaderStorage.createSnapshot(1,
                Eraftpb.ConfState.newBuilder().addVoters(1).build(),
                /* business data callback = */ null);
        assertThat(snap.getData().size())
                .as("createSnapshot now dumps user-data CFs into Snapshot.data")
                .isPositive();

        // Follower applies the snapshot — same call shape as raft library
        // calls during MsgSnapshot processing.
        followerStorage.applySnapshot(snap);

        // Follower must now see the leader's data.
        assertThat(followerEngine.get(StorageEngine.Cf.DEFAULT, "alice".getBytes()))
                .containsExactly("100".getBytes());
        assertThat(followerEngine.get(StorageEngine.Cf.DEFAULT, "bob".getBytes()))
                .containsExactly("200".getBytes());
        assertThat(followerEngine.get(StorageEngine.Cf.WRITE, "alice".getBytes()))
                .containsExactly("wv".getBytes());
        assertThat(followerEngine.get(StorageEngine.Cf.LOCK, "carol".getBytes()))
                .containsExactly("lk".getBytes());

        // Follower's raft meta advanced to the snapshot's index.
        assertThat(followerRaft.appliedIndex()).isEqualTo(1);
    }
}
