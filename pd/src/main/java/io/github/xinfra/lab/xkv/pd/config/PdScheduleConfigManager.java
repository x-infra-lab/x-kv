package io.github.xinfra.lab.xkv.pd.config;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Consumer;

public final class PdScheduleConfigManager {

    private static final Logger log = LoggerFactory.getLogger(PdScheduleConfigManager.class);

    private final ConcurrentHashMap<String, String> entries = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, List<Consumer<String>>> listeners = new ConcurrentHashMap<>();

    public PdScheduleConfigManager() {
        seedDefaults(PdConfig.SchedulerConfig.defaults());
    }

    public PdScheduleConfigManager(PdConfig.SchedulerConfig config) {
        seedDefaults(config);
    }

    private void seedDefaults(PdConfig.SchedulerConfig config) {
        entries.put("schedule.max-operators-per-store", String.valueOf(config.maxOperatorsPerStore()));
        entries.put("schedule.leader-schedule-limit", String.valueOf(config.leaderScheduleLimit()));
        entries.put("schedule.region-schedule-limit", String.valueOf(config.regionScheduleLimit()));
        entries.put("schedule.hot-region-schedule-limit", String.valueOf(config.hotRegionScheduleLimit()));
        entries.put("schedule.region-split-bytes", String.valueOf(config.regionSplitBytes()));
        entries.put("schedule.store-state-timeout-ms", String.valueOf(config.storeStateTimeoutMs()));
        entries.put("schedule.heartbeat-interval-ms", String.valueOf(config.heartbeatIntervalMs()));
    }

    public String get(String key) {
        return entries.get(key);
    }

    public long getLong(String key, long defaultValue) {
        String v = entries.get(key);
        if (v == null) return defaultValue;
        try {
            return Long.parseLong(v);
        } catch (NumberFormatException e) {
            return defaultValue;
        }
    }

    public int getInt(String key, int defaultValue) {
        String v = entries.get(key);
        if (v == null) return defaultValue;
        try {
            return Integer.parseInt(v);
        } catch (NumberFormatException e) {
            return defaultValue;
        }
    }

    public Map<String, String> getAll() {
        return Collections.unmodifiableMap(new LinkedHashMap<>(entries));
    }

    public String set(String key, String value) {
        if (!entries.containsKey(key)) {
            return "unknown config key: " + key;
        }
        String old = entries.put(key, value);
        if (old != null && old.equals(value)) return null;

        log.info("schedule config changed: {} = {} (was {})", key, value, old);
        var keyListeners = listeners.get(key);
        if (keyListeners != null) {
            for (var listener : keyListeners) {
                try {
                    listener.accept(value);
                } catch (Throwable t) {
                    log.warn("schedule config listener error for key={}: {}", key, t.getMessage());
                }
            }
        }
        return null;
    }

    public void onUpdate(String key, Consumer<String> listener) {
        listeners.computeIfAbsent(key, k -> new CopyOnWriteArrayList<>()).add(listener);
    }
}
