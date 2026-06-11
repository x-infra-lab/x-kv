package io.github.xinfra.lab.xkv.pd.config;

import io.github.xinfra.lab.xkv.common.config.YamlConfigLoader;
import io.github.xinfra.lab.xkv.common.tls.TlsConfig;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static io.github.xinfra.lab.xkv.common.config.YamlConfigLoader.*;

/**
 * Builds a {@link PdConfig} from YAML file + env vars + CLI args.
 *
 * <p>Priority: CLI args > env vars ({@code X_PD_*}) > YAML file > defaults.
 */
public final class PdConfigLoader {

    private PdConfigLoader() {}

    public static PdConfig load(String[] args) throws IOException {
        var m = loadAll(args, "X_PD");

        var builder = PdConfig.builder()
                .nodeId(getLong(m, "node-id", 1))
                .clusterId(getLong(m, "cluster-id", 1))
                .clientAddress(getString(m, "client-address",
                        getString(m, "addr", "127.0.0.1:2379")))
                .raftAddress(getString(m, "raft-address",
                        getString(m, "raft-addr", "127.0.0.1:2380")))
                .dataDir(Path.of(getString(m, "data-dir",
                        System.getProperty("java.io.tmpdir") + "/x-pd-1")))
                .joinMode(getBool(m, "join", false))
                .metricsPort(getInt(m, "metrics-port", 0))
                .maxConcurrentRequests(getInt(m, "max-concurrent-requests", 5_000))
                .slowLogThresholdMs(getLong(m, "slow-log-threshold-ms", 1000));

        // Peers: --peer "1=127.0.0.1:2380,127.0.0.1:2379" --peer "2=..."
        // Or YAML: peers: [{id: 1, raft-address: ..., client-address: ...}, ...]
        var peerList = parsePeers(m);
        if (!peerList.isEmpty()) builder.peers(peerList);

        String authToken = getString(m, "security.auth-token",
                getString(m, "auth-token", null));
        if (authToken != null) builder.authToken(authToken);

        var clientTls = loadTlsConfig(m, "security.client-tls");
        if (clientTls != null) builder.clientTls(clientTls);
        var raftTls = loadTlsConfig(m, "security.raft-tls");
        if (raftTls != null) builder.raftTls(raftTls);

        builder.tso(new PdConfig.TsoConfig(
                getLong(m, "tso.saved-interval-ms", 50),
                getLong(m, "tso.update-interval-ms", 10)));

        builder.scheduler(new PdConfig.SchedulerConfig(
                getLong(m, "scheduler.heartbeat-interval-ms", 10_000),
                getLong(m, "scheduler.store-state-timeout-ms", 30_000),
                getInt(m, "scheduler.max-operators-per-store", 5),
                getInt(m, "scheduler.region-schedule-limit", 32),
                getInt(m, "scheduler.leader-schedule-limit", 4),
                getInt(m, "scheduler.hot-region-schedule-limit", 4),
                getLong(m, "scheduler.region-split-bytes", 64L * 1024 * 1024)));

        builder.safePoint(new PdConfig.SafePointConfig(
                getLong(m, "safe-point.default-gc-lifetime-ms", 10 * 60 * 1000L),
                getLong(m, "safe-point.advance-interval-ms", 60_000L),
                getLong(m, "safe-point.service-safe-point-ttl-ms", 5 * 60_000L)));

        return builder.build();
    }

    /**
     * Parse peer definitions. Supports two formats:
     * <ul>
     *   <li>CLI: {@code --peer "1=host:raftPort,host:clientPort"} (repeatable)</li>
     *   <li>YAML flat keys: {@code peers.0.id}, {@code peers.0.raft-address}, etc.</li>
     * </ul>
     */
    private static List<PdConfig.PeerAddress> parsePeers(Map<String, String> m) {
        var peers = new ArrayList<PdConfig.PeerAddress>();

        // Try indexed YAML form: peers.0.id, peers.0.raft-address, ...
        for (int i = 0; i < 20; i++) {
            String idKey = "peers." + i + ".id";
            String id = getString(m, idKey, null);
            if (id == null) break;
            String raftAddr = getString(m, "peers." + i + ".raft-address", "");
            String clientAddr = getString(m, "peers." + i + ".client-address", "");
            peers.add(new PdConfig.PeerAddress(Long.parseLong(id), raftAddr, clientAddr));
        }

        // CLI form: --peer "id=raftAddr,clientAddr"
        for (var entry : m.entrySet()) {
            if (!entry.getKey().equals("peer")) continue;
            for (String peerSpec : entry.getValue().split(";")) {
                peerSpec = peerSpec.trim();
                if (peerSpec.isEmpty()) continue;
                int eq = peerSpec.indexOf('=');
                if (eq < 0) continue;
                long id = Long.parseLong(peerSpec.substring(0, eq).trim());
                String[] addrs = peerSpec.substring(eq + 1).split(",", 2);
                String raftAddr = addrs[0].trim();
                String clientAddr = addrs.length > 1 ? addrs[1].trim() : "";
                peers.add(new PdConfig.PeerAddress(id, raftAddr, clientAddr));
            }
        }
        return peers;
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
