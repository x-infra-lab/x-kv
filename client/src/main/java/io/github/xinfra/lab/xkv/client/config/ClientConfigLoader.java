package io.github.xinfra.lab.xkv.client.config;

import io.github.xinfra.lab.xkv.common.tls.TlsConfig;

import java.io.IOException;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.Map;

import static io.github.xinfra.lab.xkv.common.config.YamlConfigLoader.*;

/**
 * Builds a {@link ClientConfig} from YAML file + env vars + CLI args.
 *
 * <p>Priority: CLI args > env vars ({@code X_KV_CLIENT_*}) > YAML file > defaults.
 */
public final class ClientConfigLoader {

    private ClientConfigLoader() {}

    public static ClientConfig load(String[] args) throws IOException {
        var m = loadAll(args, "X_KV_CLIENT");

        var builder = ClientConfig.builder()
                .pdEndpoints(getStringList(m, "pd-endpoints", List.of("127.0.0.1:2379")))
                .grpcTimeout(Duration.ofMillis(getLong(m, "grpc-timeout-ms", 10_000)));

        String authToken = getString(m, "auth-token", null);
        if (authToken != null) builder.authToken(authToken);

        builder.backoff(new ClientConfig.BackoffConfig(
                getLong(m, "backoff.region-miss-base-ms", 2),
                getLong(m, "backoff.region-miss-cap-ms", 1_000),
                getLong(m, "backoff.txn-lock-base-ms", 100),
                getLong(m, "backoff.txn-lock-cap-ms", 3_000),
                getLong(m, "backoff.server-busy-base-ms", 500),
                getLong(m, "backoff.server-busy-cap-ms", 5_000),
                getLong(m, "backoff.network-base-ms", 500),
                getLong(m, "backoff.network-cap-ms", 20_000),
                getLong(m, "backoff.not-leader-base-ms", 5),
                getLong(m, "backoff.not-leader-cap-ms", 1_000),
                getLong(m, "backoff.max-overall-elapsed-ms", 40_000)));

        builder.tso(new ClientConfig.TsoConfig(
                getInt(m, "tso.batch-wait-micros", 50),
                getInt(m, "tso.max-batch-size", 4096),
                Duration.ofMillis(getLong(m, "tso.stream-reconnect-backoff-ms", 200))));

        builder.regionCache(new ClientConfig.RegionCacheConfig(
                getInt(m, "region-cache.max-entries", 50_000),
                Duration.ofMillis(getLong(m, "region-cache.entry-ttl-ms", 600_000)),
                Duration.ofMillis(getLong(m, "region-cache.negative-ttl-ms", 5_000))));

        builder.txn(new ClientConfig.TxnConfig(
                Duration.ofMillis(getLong(m, "txn.default-lock-ttl-ms", 20_000)),
                Duration.ofMillis(getLong(m, "txn.lock-heartbeat-interval-ms", 5_000)),
                getInt(m, "txn.prewrite-batch-size", 1024),
                getInt(m, "txn.commit-batch-size", 1024),
                getInt(m, "txn.batch-get-concurrency", 32),
                getInt(m, "txn.resolver-cache-size", 10_000),
                Duration.ofMillis(getLong(m, "txn.resolver-cache-ttl-ms", 300_000)),
                getBool(m, "txn.enable-async-commit", true),
                getBool(m, "txn.enable-one-pc", true),
                getLong(m, "txn.pessimistic-wait-timeout-ms", 3000)));

        var tls = loadTlsConfig(m, "conn.tls");
        builder.conn(new ClientConfig.ConnConfig(
                getInt(m, "conn.max-store-connections", 4),
                getInt(m, "conn.idle-connection-ttl-sec", 600),
                tls));

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
