package io.github.xinfra.lab.xkv.kv.config;

import io.github.xinfra.lab.xkv.common.config.YamlConfigLoader;
import io.github.xinfra.lab.xkv.common.tls.TlsConfig;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;

import static io.github.xinfra.lab.xkv.common.config.YamlConfigLoader.*;

/**
 * Builds a {@link KvConfig} from YAML file + env vars + CLI args.
 *
 * <p>Priority: CLI args > env vars ({@code X_KV_*}) > YAML file > defaults.
 */
public final class KvConfigLoader {

    private KvConfigLoader() {}

    public static KvConfig load(String[] args) throws IOException {
        var m = loadAll(args, "X_KV");

        var builder = KvConfig.builder()
                .storeId(getLong(m, "store-id", 1))
                .pdEndpoints(getStringList(m, "pd-endpoints",
                        List.of(getString(m, "pd", "127.0.0.1:2379"))))
                .clientAddress(getString(m, "client-address",
                        getString(m, "addr", "127.0.0.1:20160")))
                .raftAddress(getString(m, "raft-address",
                        getString(m, "raft-addr", "127.0.0.1:20170")))
                .dataDir(Path.of(getString(m, "data-dir",
                        System.getProperty("java.io.tmpdir") + "/x-kv-1")))
                .metricsPort(getInt(m, "metrics-port", 0))
                .maxConcurrentRequests(getInt(m, "max-concurrent-requests", 10_000))
                .slowLogThresholdMs(getLong(m, "slow-log-threshold-ms", 1000))
                .drainTimeoutMs(getLong(m, "drain-timeout-ms", 10_000));

        String authToken = getString(m, "security.auth-token",
                getString(m, "auth-token", null));
        if (authToken != null) builder.authToken(authToken);

        var clientTls = loadTlsConfig(m, "security.client-tls");
        if (clientTls != null) builder.clientTls(clientTls);
        var raftTls = loadTlsConfig(m, "security.raft-tls");
        if (raftTls != null) builder.raftTls(raftTls);

        builder.engine(new KvConfig.EngineConfig(
                getLong(m, "engine.block-cache-bytes", 256L * 1024 * 1024),
                getLong(m, "engine.write-buffer-bytes", 64L * 1024 * 1024),
                getInt(m, "engine.max-background-jobs", 4),
                getBool(m, "engine.enable-statistics", false)));

        builder.raft(new KvConfig.RaftConfig(
                getLong(m, "raft.election-tick-ms", 1000),
                getLong(m, "raft.heartbeat-tick-ms", 100),
                getInt(m, "raft.max-size-per-msg", 1024 * 1024),
                getInt(m, "raft.max-inflight-msgs", 256),
                getLong(m, "raft.snapshot-interval-entries", 10_000),
                getLong(m, "raft.apply-batch-entries", 64)));

        builder.region(new KvConfig.RegionConfig(
                getLong(m, "region.max-region-bytes", 96L << 20),
                getLong(m, "region.split-region-bytes", 64L << 20),
                getLong(m, "region.merge-region-bytes", 8L << 20),
                getInt(m, "region.region-max-keys", 1_440_000)));

        builder.worker(new KvConfig.WorkerConfig(
                getLong(m, "worker.log-compaction-interval-ms", 60_000),
                getLong(m, "worker.log-compaction-gap-threshold", 10_000),
                getLong(m, "worker.log-compaction-safety-margin", 1_000),
                getLong(m, "worker.gc-interval-ms", 60_000)));

        return builder.build();
    }

    private static TlsConfig loadTlsConfig(Map<String, String> m, String prefix) {
        String cert = getString(m, prefix + ".cert-chain", null);
        String key = getString(m, prefix + ".private-key", null);
        String trust = getString(m, prefix + ".trust-certs", null);
        if (cert == null && key == null && trust == null) return null;
        boolean mtls = getBool(m, prefix + ".mtls", false);
        return new TlsConfig(
                cert != null ? Path.of(cert) : null,
                key != null ? Path.of(key) : null,
                trust != null ? Path.of(trust) : null,
                mtls);
    }
}
