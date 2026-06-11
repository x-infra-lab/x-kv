package io.github.xinfra.lab.xkv.pd.state;

import com.google.protobuf.ByteString;
import io.github.xinfra.lab.xkv.proto.Metapb;
import io.github.xinfra.lab.xkv.proto.PdInternalpb;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThat;

final class RocksDbPdStateMachineTest {

    @TempDir Path tempDir;
    private RocksDbPdStateMachine sm;

    @AfterEach
    void teardown() {
        if (sm != null) sm.close();
    }

    private RocksDbPdStateMachine open() {
        return new RocksDbPdStateMachine(tempDir.resolve("pd-state"));
    }

    @Test
    void bootstrapIsIdempotent() {
        sm = open();
        assertThat(sm.isBootstrapped()).isFalse();
        sm.bootstrap(store(1), region(1, "", ""));
        assertThat(sm.isBootstrapped()).isTrue();
        sm.bootstrap(store(99), region(99, "x", "y"));
        assertThat(sm.isBootstrapped()).isTrue();
        assertThat(sm.storeCount()).isEqualTo(1);
        assertThat(sm.regionCount()).isEqualTo(1);
    }

    @Test
    void getRegionByKeyIsLogN() {
        sm = open();
        sm.bootstrap(store(1), region(1, "", ""));
        sm.updateRegion(region(1, "", "k"));
        sm.updateRegion(region(2, "k", "p"));
        sm.updateRegion(region(3, "p", ""));

        assertThat(sm.getRegionByKey("a".getBytes())).hasValueSatisfying(r ->
                assertThat(r.getId()).isEqualTo(1));
        assertThat(sm.getRegionByKey("k".getBytes())).hasValueSatisfying(r ->
                assertThat(r.getId()).isEqualTo(2));
        assertThat(sm.getRegionByKey("o".getBytes())).hasValueSatisfying(r ->
                assertThat(r.getId()).isEqualTo(2));
        assertThat(sm.getRegionByKey("p".getBytes())).hasValueSatisfying(r ->
                assertThat(r.getId()).isEqualTo(3));
        assertThat(sm.getRegionByKey("zz".getBytes())).hasValueSatisfying(r ->
                assertThat(r.getId()).isEqualTo(3));
    }

    @Test
    void staleEpochUpdateIsDropped() {
        sm = open();
        sm.bootstrap(store(1), region(1, "", "").toBuilder()
                .setRegionEpoch(Metapb.RegionEpoch.newBuilder().setConfVer(2).setVersion(5)).build());
        var stale = region(1, "", "").toBuilder()
                .setRegionEpoch(Metapb.RegionEpoch.newBuilder().setConfVer(2).setVersion(3))
                .build();
        sm.updateRegion(stale);
        assertThat(sm.getRegion(1).orElseThrow().getRegionEpoch().getVersion())
                .as("stale update dropped").isEqualTo(5);
    }

    @Test
    void allocIdReturnsSequence() {
        sm = open();
        long a = sm.allocId(1);
        long b = sm.allocId(5);
        long c = sm.allocId(1);
        assertThat(b).isEqualTo(a + 1);
        assertThat(c).isEqualTo(b + 5);
    }

    @Test
    void scanRegionsReturnsRangeInOrder() {
        sm = open();
        sm.bootstrap(store(1), region(1, "", ""));
        sm.updateRegion(region(1, "", "b"));
        sm.updateRegion(region(2, "b", "d"));
        sm.updateRegion(region(3, "d", "f"));
        sm.updateRegion(region(4, "f", "h"));

        var regions = new java.util.ArrayList<Metapb.Region>();
        sm.scanRegions("c".getBytes(), "g".getBytes(), 100).forEach(regions::add);
        assertThat(regions).hasSize(3);
        assertThat(regions.get(0).getId()).isEqualTo(2);
        assertThat(regions.get(1).getId()).isEqualTo(3);
        assertThat(regions.get(2).getId()).isEqualTo(4);
    }

    @Test
    void splitReplacesParentRange() {
        sm = open();
        sm.bootstrap(store(1), region(1, "", ""));
        sm.updateRegion(region(1, "", "m"));
        sm.updateRegion(region(7, "m", ""));
        assertThat(sm.getRegionByKey("a".getBytes()).orElseThrow().getId()).isEqualTo(1);
        assertThat(sm.getRegionByKey("z".getBytes()).orElseThrow().getId()).isEqualTo(7);
    }

    @Test
    void statePersistedAcrossReopen() {
        sm = open();
        sm.bootstrap(store(1), region(1, "", ""));
        sm.putStore(store(2));
        sm.updateRegion(region(1, "", "m"));
        sm.updateRegion(region(7, "m", ""));
        long allocBase = sm.allocId(50);
        sm.close();

        sm = open();
        assertThat(sm.isBootstrapped()).isTrue();
        assertThat(sm.storeCount()).isEqualTo(2);
        assertThat(sm.getStore(1)).isPresent();
        assertThat(sm.getStore(2)).isPresent();
        assertThat(sm.regionCount()).isEqualTo(2);
        assertThat(sm.getRegion(1)).isPresent();
        assertThat(sm.getRegion(7)).isPresent();
        assertThat(sm.getRegionByKey("a".getBytes()).orElseThrow().getId()).isEqualTo(1);
        assertThat(sm.getRegionByKey("z".getBytes()).orElseThrow().getId()).isEqualTo(7);
        long nextAlloc = sm.allocId(1);
        assertThat(nextAlloc).isGreaterThanOrEqualTo(allocBase + 50);
    }

    @Test
    void hasPersistedStateReflectsBootstrap() {
        sm = open();
        assertThat(sm.hasPersistedState()).isFalse();
        sm.bootstrap(store(1), region(1, "", ""));
        assertThat(sm.hasPersistedState()).isTrue();
        sm.close();

        sm = open();
        assertThat(sm.hasPersistedState()).isTrue();
    }

    @Test
    void snapshotDumpAndInstall() {
        sm = open();
        sm.bootstrap(store(1), region(1, "", ""));
        sm.putStore(store(2));
        sm.updateRegion(region(1, "", "m"));
        sm.updateRegion(region(7, "m", ""));

        byte[] snapshot = sm.dumpSnapshot();
        sm.close();

        // Open a fresh state machine and install the snapshot.
        sm = new RocksDbPdStateMachine(tempDir.resolve("pd-state-2"));
        assertThat(sm.isBootstrapped()).isFalse();
        sm.installSnapshot(snapshot);

        assertThat(sm.isBootstrapped()).isTrue();
        assertThat(sm.storeCount()).isEqualTo(2);
        assertThat(sm.regionCount()).isEqualTo(2);
        assertThat(sm.getRegionByKey("a".getBytes()).orElseThrow().getId()).isEqualTo(1);
        assertThat(sm.getRegionByKey("z".getBytes()).orElseThrow().getId()).isEqualTo(7);
    }

    @Test
    void applyCommandIdempotent() {
        sm = open();
        var bootstrapCmd = PdInternalpb.PdCommand.newBuilder()
                .setType(PdInternalpb.CommandType.CMD_BOOTSTRAP)
                .setBootstrap(PdInternalpb.BootstrapPayload.newBuilder()
                        .setStore(store(1))
                        .setRegion(region(1, "", "")))
                .build();
        sm.applyCommand(bootstrapCmd.toByteArray());
        assertThat(sm.isBootstrapped()).isTrue();

        // Apply again — idempotent.
        sm.applyCommand(bootstrapCmd.toByteArray());
        assertThat(sm.storeCount()).isEqualTo(1);

        var putStoreCmd = PdInternalpb.PdCommand.newBuilder()
                .setType(PdInternalpb.CommandType.CMD_PUT_STORE)
                .setStore(store(2))
                .build();
        sm.applyCommand(putStoreCmd.toByteArray());
        assertThat(sm.getStore(2)).isPresent();
    }

    // ---- helpers ----

    private static Metapb.Store store(long id) {
        return Metapb.Store.newBuilder().setId(id).setAddress("127.0.0.1:" + (10000 + id)).build();
    }

    private static Metapb.Region region(long id, String start, String end) {
        return Metapb.Region.newBuilder()
                .setId(id)
                .setStartKey(ByteString.copyFromUtf8(start))
                .setEndKey(ByteString.copyFromUtf8(end))
                .setRegionEpoch(Metapb.RegionEpoch.newBuilder().setConfVer(1).setVersion(1))
                .build();
    }
}
