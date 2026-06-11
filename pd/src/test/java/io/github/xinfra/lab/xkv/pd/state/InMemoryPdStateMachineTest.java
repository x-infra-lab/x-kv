package io.github.xinfra.lab.xkv.pd.state;

import com.google.protobuf.ByteString;
import io.github.xinfra.lab.xkv.proto.Metapb;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

final class InMemoryPdStateMachineTest {

    @Test
    void bootstrapIsIdempotent() {
        var sm = new InMemoryPdStateMachine();
        assertThat(sm.isBootstrapped()).isFalse();
        sm.bootstrap(store(1), region(1, "", ""));
        assertThat(sm.isBootstrapped()).isTrue();
        // Idempotent — second call doesn't blow up.
        sm.bootstrap(store(99), region(99, "x", "y"));
        assertThat(sm.isBootstrapped()).isTrue();
        assertThat(sm.storeCount()).isEqualTo(1);
        assertThat(sm.regionCount()).isEqualTo(1);
    }

    @Test
    void getRegionByKeyIsLogN() {
        var sm = new InMemoryPdStateMachine();
        sm.bootstrap(store(1), region(1, "", ""));
        // Replace the bootstrap region with multiple regions.
        sm.updateRegion(region(1, "", "k"));
        sm.updateRegion(region(2, "k", "p"));
        sm.updateRegion(region(3, "p", ""));   // tail = +∞

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
        var sm = new InMemoryPdStateMachine();
        sm.bootstrap(store(1), region(1, "", "").toBuilder()
                .setRegionEpoch(Metapb.RegionEpoch.newBuilder().setConfVer(2).setVersion(5)).build());
        // Try to apply a stale update with smaller epoch.
        var stale = region(1, "", "").toBuilder()
                .setRegionEpoch(Metapb.RegionEpoch.newBuilder().setConfVer(2).setVersion(3))
                .build();
        sm.updateRegion(stale);
        assertThat(sm.getRegion(1).orElseThrow().getRegionEpoch().getVersion())
                .as("stale update dropped").isEqualTo(5);
    }

    @Test
    void allocIdReturnsSequence() {
        var sm = new InMemoryPdStateMachine();
        long a = sm.allocId(1);
        long b = sm.allocId(5);
        long c = sm.allocId(1);
        assertThat(b).isEqualTo(a + 1);
        assertThat(c).isEqualTo(b + 5);
    }

    @Test
    void scanRegionsReturnsRangeInOrder() {
        var sm = new InMemoryPdStateMachine();
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
        var sm = new InMemoryPdStateMachine();
        sm.bootstrap(store(1), region(1, "", ""));
        sm.updateRegion(region(1, "", "m"));
        sm.updateRegion(region(7, "m", ""));     // right side of split
        // Region 1 still in id index, range [, m); region 7 is [m, +∞).
        assertThat(sm.getRegionByKey("a".getBytes()).orElseThrow().getId()).isEqualTo(1);
        assertThat(sm.getRegionByKey("z".getBytes()).orElseThrow().getId()).isEqualTo(7);
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
