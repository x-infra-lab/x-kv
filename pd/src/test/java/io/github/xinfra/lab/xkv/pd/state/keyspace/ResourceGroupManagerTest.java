package io.github.xinfra.lab.xkv.pd.state.keyspace;

import io.github.xinfra.lab.xkv.proto.Pdpb;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

final class ResourceGroupManagerTest {

    private static Pdpb.ResourceGroup group(String name, long fillRate, long burstLimit) {
        return Pdpb.ResourceGroup.newBuilder()
                .setName(name)
                .setRuSettings(Pdpb.RUSettings.newBuilder()
                        .setFillRate(fillRate)
                        .setBurstLimit(burstLimit))
                .build();
    }

    @Test
    void defaultGroupExistsOnCreation() {
        var mgr = new ResourceGroupManager();
        assertThat(mgr.size()).isEqualTo(1);
        var def = mgr.getGroup(ResourceGroupManager.DEFAULT_GROUP_NAME);
        assertThat(def).isNotNull();
        assertThat(def.getRuSettings().getFillRate()).isZero();
    }

    @Test
    void addAndGetGroup() {
        var mgr = new ResourceGroupManager();
        assertThat(mgr.addGroup(group("batch", 100, 200))).isTrue();
        var g = mgr.getGroup("batch");
        assertThat(g).isNotNull();
        assertThat(g.getRuSettings().getFillRate()).isEqualTo(100);
        assertThat(g.getRuSettings().getBurstLimit()).isEqualTo(200);
    }

    @Test
    void duplicateAddRejected() {
        var mgr = new ResourceGroupManager();
        assertThat(mgr.addGroup(group("dup", 10, 20))).isTrue();
        assertThat(mgr.addGroup(group("dup", 30, 40))).isFalse();
    }

    @Test
    void emptyNameRejected() {
        var mgr = new ResourceGroupManager();
        assertThat(mgr.addGroup(group("", 10, 20))).isFalse();
    }

    @Test
    void modifyExistingGroup() {
        var mgr = new ResourceGroupManager();
        mgr.addGroup(group("oltp", 100, 200));
        assertThat(mgr.modifyGroup(group("oltp", 500, 1000))).isTrue();
        assertThat(mgr.getGroup("oltp").getRuSettings().getFillRate()).isEqualTo(500);
    }

    @Test
    void modifyNonExistentReturnsFlse() {
        var mgr = new ResourceGroupManager();
        assertThat(mgr.modifyGroup(group("ghost", 10, 20))).isFalse();
    }

    @Test
    void deleteGroupButNotDefault() {
        var mgr = new ResourceGroupManager();
        mgr.addGroup(group("temp", 10, 20));
        assertThat(mgr.deleteGroup("temp")).isTrue();
        assertThat(mgr.getGroup("temp")).isNull();

        assertThat(mgr.deleteGroup(ResourceGroupManager.DEFAULT_GROUP_NAME)).isFalse();
        assertThat(mgr.getGroup(ResourceGroupManager.DEFAULT_GROUP_NAME)).isNotNull();
    }

    @Test
    void listGroupsSortedByName() {
        var mgr = new ResourceGroupManager();
        mgr.addGroup(group("z-group", 10, 20));
        mgr.addGroup(group("a-group", 30, 40));
        var list = mgr.listGroups();
        assertThat(list).hasSize(3);
        assertThat(list.get(0).getName()).isEqualTo("a-group");
        assertThat(list.get(1).getName()).isEqualTo(ResourceGroupManager.DEFAULT_GROUP_NAME);
        assertThat(list.get(2).getName()).isEqualTo("z-group");
    }

    @Test
    void encodeDecodeRoundTrip() {
        var mgr = new ResourceGroupManager();
        mgr.addGroup(group("rg1", 100, 200));
        mgr.addGroup(group("rg2", 300, 400));
        var encoded = mgr.encode();

        var mgr2 = new ResourceGroupManager();
        mgr2.decode(encoded);
        assertThat(mgr2.size()).isEqualTo(mgr.size());
        assertThat(mgr2.getGroup("rg1")).isNotNull();
        assertThat(mgr2.getGroup("rg2")).isNotNull();
        assertThat(mgr2.getGroup(ResourceGroupManager.DEFAULT_GROUP_NAME)).isNotNull();
    }
}
