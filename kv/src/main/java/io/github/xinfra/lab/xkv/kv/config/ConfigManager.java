package io.github.xinfra.lab.xkv.kv.config;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Consumer;

/**
 * Runtime-mutable configuration registry. Holds a map of dot-separated
 * config keys (e.g., {@code "raftstore.raft-heartbeat-tick-ms"}) to their
 * current string-encoded values.
 *
 * <p>On startup, the manager is seeded with the initial {@link KvConfig}'s
 * mutable parameters. At runtime, operators can modify entries via the
 * Debug gRPC service. Listeners are notified on change so that subsystems
 * can pick up new values without a restart.
 *
 * <p>Immutable parameters (store ID, data dir, addresses) are excluded —
 * changing them requires a restart.
 */
public final class ConfigManager {

    private static final Logger log = LoggerFactory.getLogger(ConfigManager.class);

    private final ConcurrentHashMap<String, String> entries = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, List<Consumer<String>>> listeners = new ConcurrentHashMap<>();

    public ConfigManager() {}

    public ConfigManager(KvConfig config) {
        seed(config);
    }

    private void seed(KvConfig config) {
        // Raft
        var raft = config.raft();
        entries.put("raftstore.raft-heartbeat-tick-ms", String.valueOf(raft.heartbeatTickMs()));
        entries.put("raftstore.raft-election-tick-ms", String.valueOf(raft.electionTickMs()));
        entries.put("raftstore.max-size-per-msg", String.valueOf(raft.maxSizePerMsg()));
        entries.put("raftstore.max-inflight-msgs", String.valueOf(raft.maxInflightMsgs()));
        entries.put("raftstore.snapshot-interval-entries", String.valueOf(raft.snapshotIntervalEntries()));
        entries.put("raftstore.apply-batch-entries", String.valueOf(raft.applyBatchEntries()));
        entries.put("raftstore.lease-based-read", String.valueOf(raft.leaseBasedRead()));

        // Region
        var region = config.region();
        entries.put("region.max-region-bytes", String.valueOf(region.maxRegionBytes()));
        entries.put("region.split-region-bytes", String.valueOf(region.splitRegionBytes()));
        entries.put("region.merge-region-bytes", String.valueOf(region.mergeRegionBytes()));
        entries.put("region.region-max-keys", String.valueOf(region.regionMaxKeys()));

        // Workers
        var worker = config.worker();
        entries.put("worker.log-compaction-interval-ms", String.valueOf(worker.logCompactionIntervalMs()));
        entries.put("worker.log-compaction-gap-threshold", String.valueOf(worker.logCompactionGapThreshold()));
        entries.put("worker.gc-interval-ms", String.valueOf(worker.gcIntervalMs()));

        // Server
        entries.put("server.max-concurrent-requests", String.valueOf(config.maxConcurrentRequests()));
        entries.put("server.slow-log-threshold-ms", String.valueOf(config.slowLogThresholdMs()));
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

    public boolean getBoolean(String key, boolean defaultValue) {
        String v = entries.get(key);
        if (v == null) return defaultValue;
        return Boolean.parseBoolean(v);
    }

    public Map<String, String> getAll() {
        return Collections.unmodifiableMap(new LinkedHashMap<>(entries));
    }

    /**
     * Update a config entry. Returns null on success, error message on failure.
     */
    public String set(String key, String value) {
        if (!entries.containsKey(key)) {
            return "unknown config key: " + key;
        }
        String old = entries.put(key, value);
        if (old != null && old.equals(value)) return null;

        log.info("config changed: {} = {} (was {})", key, value, old);
        var keyListeners = listeners.get(key);
        if (keyListeners != null) {
            for (var listener : keyListeners) {
                try {
                    listener.accept(value);
                } catch (Throwable t) {
                    log.warn("config listener error for key={}: {}", key, t.getMessage());
                }
            }
        }
        return null;
    }

    public void onUpdate(String key, Consumer<String> listener) {
        listeners.computeIfAbsent(key, k -> new CopyOnWriteArrayList<>()).add(listener);
    }
}
