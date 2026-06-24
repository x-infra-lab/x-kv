package io.github.xinfra.lab.xkv.pd.config;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;

class PdScheduleConfigManagerTest {

    private PdScheduleConfigManager mgr;

    @BeforeEach
    void setUp() {
        mgr = new PdScheduleConfigManager();
    }

    @Test
    void seedsFromDefaults() {
        var defaults = PdConfig.SchedulerConfig.defaults();
        assertThat(mgr.get("schedule.max-operators-per-store"))
                .isEqualTo(String.valueOf(defaults.maxOperatorsPerStore()));
        assertThat(mgr.get("schedule.region-split-bytes"))
                .isEqualTo(String.valueOf(defaults.regionSplitBytes()));
        assertThat(mgr.get("schedule.leader-schedule-limit"))
                .isEqualTo(String.valueOf(defaults.leaderScheduleLimit()));
    }

    @Test
    void setAndGet() {
        assertThat(mgr.set("schedule.max-operators-per-store", "10")).isNull();
        assertThat(mgr.get("schedule.max-operators-per-store")).isEqualTo("10");
        assertThat(mgr.getInt("schedule.max-operators-per-store", 5)).isEqualTo(10);
    }

    @Test
    void unknownKeyReturnsError() {
        String err = mgr.set("nonexistent.key", "value");
        assertThat(err).contains("unknown config key");
    }

    @Test
    void listenerNotifiedOnChange() {
        var ref = new AtomicReference<String>();
        mgr.onUpdate("schedule.region-split-bytes", ref::set);

        mgr.set("schedule.region-split-bytes", "128000000");
        assertThat(ref.get()).isEqualTo("128000000");
    }

    @Test
    void sameValueSkipsListener() {
        var defaults = PdConfig.SchedulerConfig.defaults();
        var ref = new AtomicReference<String>();
        mgr.onUpdate("schedule.max-operators-per-store", ref::set);

        mgr.set("schedule.max-operators-per-store",
                String.valueOf(defaults.maxOperatorsPerStore()));
        assertThat(ref.get()).isNull();
    }

    @Test
    void getAllReturnsSnapshot() {
        var all = mgr.getAll();
        assertThat(all).containsKey("schedule.max-operators-per-store");
        assertThat(all).containsKey("schedule.leader-schedule-limit");
        assertThat(all).containsKey("schedule.region-schedule-limit");
        assertThat(all).containsKey("schedule.hot-region-schedule-limit");
        assertThat(all).containsKey("schedule.region-split-bytes");
        assertThat(all).containsKey("schedule.store-state-timeout-ms");
        assertThat(all).containsKey("schedule.heartbeat-interval-ms");
    }

    @Test
    void getLongWithDefault() {
        assertThat(mgr.getLong("nonexistent", 42L)).isEqualTo(42L);
        assertThat(mgr.getLong("schedule.region-split-bytes", 0L))
                .isEqualTo(PdConfig.SchedulerConfig.defaults().regionSplitBytes());
    }

    @Test
    void seedsFromCustomConfig() {
        var custom = new PdConfig.SchedulerConfig(5000, 15000, 10, 16, 8, 2, 32 * 1024 * 1024);
        var customMgr = new PdScheduleConfigManager(custom);
        assertThat(customMgr.getInt("schedule.max-operators-per-store", 0)).isEqualTo(10);
        assertThat(customMgr.getInt("schedule.leader-schedule-limit", 0)).isEqualTo(8);
    }
}
