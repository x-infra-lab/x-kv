package io.github.xinfra.lab.xkv.pd.state.keyspace;

import io.github.xinfra.lab.xkv.proto.Pdpb;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

final class KeyspaceManagerTest {

    @Test
    void defaultKeyspaceExistsOnCreation() {
        var mgr = new KeyspaceManager();
        assertThat(mgr.size()).isEqualTo(1);
        var def = mgr.loadById(KeyspaceManager.DEFAULT_KEYSPACE_ID);
        assertThat(def).isNotNull();
        assertThat(def.getName()).isEqualTo(KeyspaceManager.DEFAULT_KEYSPACE_NAME);
        assertThat(def.getState()).isEqualTo(Pdpb.KeyspaceState.ENABLED);
    }

    @Test
    void createAndLoadByName() {
        var mgr = new KeyspaceManager();
        var ks = mgr.createKeyspace("tenant-a", Map.of("k", "v"));
        assertThat(ks).isNotNull();
        assertThat(ks.getId()).isGreaterThan(0);
        assertThat(ks.getName()).isEqualTo("tenant-a");
        assertThat(ks.getState()).isEqualTo(Pdpb.KeyspaceState.ENABLED);

        var loaded = mgr.loadByName("tenant-a");
        assertThat(loaded).isNotNull();
        assertThat(loaded.getId()).isEqualTo(ks.getId());
    }

    @Test
    void duplicateNameRejected() {
        var mgr = new KeyspaceManager();
        assertThat(mgr.createKeyspace("dup", null)).isNotNull();
        assertThat(mgr.createKeyspace("dup", null)).isNull();
    }

    @Test
    void stateTransitions() {
        var mgr = new KeyspaceManager();
        var ks = mgr.createKeyspace("transitions", null);
        int id = ks.getId();

        // ENABLED -> DISABLED
        var updated = mgr.updateState(id, Pdpb.KeyspaceState.DISABLED);
        assertThat(updated).isNotNull();
        assertThat(updated.getState()).isEqualTo(Pdpb.KeyspaceState.DISABLED);

        // DISABLED -> ENABLED (revert)
        updated = mgr.updateState(id, Pdpb.KeyspaceState.ENABLED);
        assertThat(updated).isNotNull();
        assertThat(updated.getState()).isEqualTo(Pdpb.KeyspaceState.ENABLED);

        // ENABLED -> ARCHIVED is invalid (must go through DISABLED first)
        assertThat(mgr.updateState(id, Pdpb.KeyspaceState.ARCHIVED)).isNull();

        // ENABLED -> DISABLED -> ARCHIVED
        mgr.updateState(id, Pdpb.KeyspaceState.DISABLED);
        updated = mgr.updateState(id, Pdpb.KeyspaceState.ARCHIVED);
        assertThat(updated).isNotNull();
        assertThat(updated.getState()).isEqualTo(Pdpb.KeyspaceState.ARCHIVED);

        // ARCHIVED -> TOMBSTONE
        updated = mgr.updateState(id, Pdpb.KeyspaceState.TOMBSTONE);
        assertThat(updated).isNotNull();
        assertThat(updated.getState()).isEqualTo(Pdpb.KeyspaceState.TOMBSTONE);
    }

    @Test
    void listKeyspacesFilterByState() {
        var mgr = new KeyspaceManager();
        mgr.createKeyspace("ks1", null);
        var ks2 = mgr.createKeyspace("ks2", null);
        mgr.updateState(ks2.getId(), Pdpb.KeyspaceState.DISABLED);

        var all = mgr.listAll();
        assertThat(all).hasSize(3); // default + ks1 + ks2

        var enabled = mgr.listKeyspaces(Pdpb.KeyspaceState.ENABLED);
        assertThat(enabled).hasSize(2); // default + ks1

        var disabled = mgr.listKeyspaces(Pdpb.KeyspaceState.DISABLED);
        assertThat(disabled).hasSize(1);
        assertThat(disabled.get(0).getName()).isEqualTo("ks2");
    }

    @Test
    void encodeDecodeRoundTrip() {
        var mgr = new KeyspaceManager();
        mgr.createKeyspace("alpha", Map.of("env", "prod"));
        mgr.createKeyspace("beta", null);
        var encoded = mgr.encode();

        var mgr2 = new KeyspaceManager();
        mgr2.decode(encoded);
        assertThat(mgr2.size()).isEqualTo(mgr.size());
        assertThat(mgr2.loadByName("alpha")).isNotNull();
        assertThat(mgr2.loadByName("beta")).isNotNull();
        assertThat(mgr2.loadById(KeyspaceManager.DEFAULT_KEYSPACE_ID)).isNotNull();
    }

    @Test
    void updateNonExistentReturnNull() {
        var mgr = new KeyspaceManager();
        assertThat(mgr.updateState(999, Pdpb.KeyspaceState.DISABLED)).isNull();
    }
}
