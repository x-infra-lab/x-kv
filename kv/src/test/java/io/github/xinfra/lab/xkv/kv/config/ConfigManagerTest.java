package io.github.xinfra.lab.xkv.kv.config;

import org.junit.jupiter.api.Test;

import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;

final class ConfigManagerTest {

    private KvConfig testConfig() {
        return KvConfig.builder()
                .storeId(1)
                .clientAddress("127.0.0.1:20160")
                .raftAddress("127.0.0.1:20161")
                .pdEndpoints(List.of("127.0.0.1:2379"))
                .dataDir(Path.of("/tmp/test"))
                .build();
    }

    @Test
    void seedsFromKvConfig() {
        var cm = new ConfigManager(testConfig());
        var all = cm.getAll();
        assertThat(all).containsKey("raftstore.raft-heartbeat-tick-ms");
        assertThat(all).containsKey("region.split-region-bytes");
        assertThat(all).containsKey("worker.gc-interval-ms");
        assertThat(all).containsKey("server.max-concurrent-requests");
    }

    @Test
    void getReturnsSeededValue() {
        var cm = new ConfigManager(testConfig());
        assertThat(cm.get("raftstore.raft-heartbeat-tick-ms")).isEqualTo("100");
    }

    @Test
    void getLongParsesCorrectly() {
        var cm = new ConfigManager(testConfig());
        assertThat(cm.getLong("raftstore.raft-heartbeat-tick-ms", -1)).isEqualTo(100);
        assertThat(cm.getLong("nonexistent", 42)).isEqualTo(42);
    }

    @Test
    void getBooleanParsesCorrectly() {
        var cm = new ConfigManager(testConfig());
        assertThat(cm.getBoolean("raftstore.lease-based-read", false)).isTrue();
        assertThat(cm.getBoolean("nonexistent", true)).isTrue();
    }

    @Test
    void setUpdatesValue() {
        var cm = new ConfigManager(testConfig());
        String err = cm.set("raftstore.raft-heartbeat-tick-ms", "200");
        assertThat(err).isNull();
        assertThat(cm.get("raftstore.raft-heartbeat-tick-ms")).isEqualTo("200");
    }

    @Test
    void setRejectsUnknownKey() {
        var cm = new ConfigManager(testConfig());
        String err = cm.set("unknown.key", "value");
        assertThat(err).contains("unknown config key");
    }

    @Test
    void listenerNotifiedOnChange() {
        var cm = new ConfigManager(testConfig());
        var received = new AtomicReference<String>();
        cm.onUpdate("raftstore.raft-heartbeat-tick-ms", received::set);

        cm.set("raftstore.raft-heartbeat-tick-ms", "300");
        assertThat(received.get()).isEqualTo("300");
    }

    @Test
    void listenerNotCalledForSameValue() {
        var cm = new ConfigManager(testConfig());
        var callCount = new java.util.concurrent.atomic.AtomicInteger();
        cm.onUpdate("raftstore.raft-heartbeat-tick-ms", v -> callCount.incrementAndGet());

        cm.set("raftstore.raft-heartbeat-tick-ms", "100");
        assertThat(callCount.get()).isZero();
    }

    @Test
    void emptyManagerHasNoEntries() {
        var cm = new ConfigManager();
        assertThat(cm.getAll()).isEmpty();
    }
}
