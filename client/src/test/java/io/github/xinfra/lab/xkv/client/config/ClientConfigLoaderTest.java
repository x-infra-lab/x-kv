package io.github.xinfra.lab.xkv.client.config;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

final class ClientConfigLoaderTest {

    @TempDir Path tempDir;

    @Test
    void defaultValues_noArgsNoConfig() throws IOException {
        var config = ClientConfigLoader.load(new String[0]);
        assertThat(config.pdEndpoints()).containsExactly("127.0.0.1:2379");
        assertThat(config.grpcTimeout()).isEqualTo(Duration.ofSeconds(10));
        assertThat(config.authToken()).isNull();
        assertThat(config.backoff().maxOverallElapsedMs()).isEqualTo(40_000);
        assertThat(config.tso().maxBatchSize()).isEqualTo(4096);
        assertThat(config.conn().maxStoreConnections()).isEqualTo(4);
        assertThat(config.conn().tls()).isNull();
        assertThat(config.txn().enableAsyncCommit()).isTrue();
        assertThat(config.txn().enableOnePc()).isTrue();
    }

    @Test
    void yamlOverridesDefaults() throws IOException {
        var yaml = tempDir.resolve("client.yaml");
        Files.writeString(yaml, """
                pd-endpoints:
                  - "10.0.0.1:2379"
                  - "10.0.0.2:2379"
                grpc-timeout-ms: 5000
                auth-token: "secret123"
                """);
        var config = ClientConfigLoader.load(new String[]{"-c", yaml.toString()});
        assertThat(config.pdEndpoints()).containsExactly("10.0.0.1:2379", "10.0.0.2:2379");
        assertThat(config.grpcTimeout()).isEqualTo(Duration.ofMillis(5000));
        assertThat(config.authToken()).isEqualTo("secret123");
    }

    @Test
    void cliArgsOverrideYaml() throws IOException {
        var yaml = tempDir.resolve("client.yaml");
        Files.writeString(yaml, "grpc-timeout-ms: 5000\n");
        var config = ClientConfigLoader.load(new String[]{
                "-c", yaml.toString(),
                "--grpc-timeout-ms", "2000"
        });
        assertThat(config.grpcTimeout()).isEqualTo(Duration.ofMillis(2000));
    }

    @Test
    void backoffFullParsing() throws IOException {
        var yaml = tempDir.resolve("backoff.yaml");
        Files.writeString(yaml, """
                backoff:
                  region-miss-base-ms: 10
                  region-miss-cap-ms: 2000
                  txn-lock-base-ms: 200
                  txn-lock-cap-ms: 6000
                  server-busy-base-ms: 1000
                  server-busy-cap-ms: 10000
                  network-base-ms: 1000
                  network-cap-ms: 30000
                  not-leader-base-ms: 10
                  not-leader-cap-ms: 2000
                  max-overall-elapsed-ms: 60000
                """);
        var config = ClientConfigLoader.load(new String[]{"-c", yaml.toString()});
        var bo = config.backoff();
        assertThat(bo.regionMissBaseMs()).isEqualTo(10);
        assertThat(bo.regionMissCapMs()).isEqualTo(2000);
        assertThat(bo.txnLockBaseMs()).isEqualTo(200);
        assertThat(bo.serverBusyCapMs()).isEqualTo(10000);
        assertThat(bo.networkCapMs()).isEqualTo(30000);
        assertThat(bo.notLeaderBaseMs()).isEqualTo(10);
        assertThat(bo.maxOverallElapsedMs()).isEqualTo(60000);
    }

    @Test
    void tlsParsing() throws IOException {
        var yaml = tempDir.resolve("tls.yaml");
        Files.writeString(yaml, """
                conn:
                  tls:
                    cert-chain: /etc/tls/client.crt
                    private-key: /etc/tls/client.key
                    trust-certs: /etc/tls/ca.crt
                    mtls: true
                """);
        var config = ClientConfigLoader.load(new String[]{"-c", yaml.toString()});
        var tls = config.conn().tls();
        assertThat(tls).isNotNull();
        assertThat(tls.certChain()).isEqualTo(Path.of("/etc/tls/client.crt"));
        assertThat(tls.privateKey()).isEqualTo(Path.of("/etc/tls/client.key"));
        assertThat(tls.trustCerts()).isEqualTo(Path.of("/etc/tls/ca.crt"));
        assertThat(tls.mtls()).isTrue();
    }

    @Test
    void txnConfigParsing() throws IOException {
        var yaml = tempDir.resolve("txn.yaml");
        Files.writeString(yaml, """
                txn:
                  default-lock-ttl-ms: 30000
                  lock-heartbeat-interval-ms: 10000
                  prewrite-batch-size: 2048
                  commit-batch-size: 512
                  batch-get-concurrency: 64
                  resolver-cache-size: 20000
                  resolver-cache-ttl-ms: 600000
                  enable-async-commit: false
                  enable-one-pc: false
                  pessimistic-wait-timeout-ms: 5000
                """);
        var config = ClientConfigLoader.load(new String[]{"-c", yaml.toString()});
        var txn = config.txn();
        assertThat(txn.defaultLockTtl()).isEqualTo(Duration.ofSeconds(30));
        assertThat(txn.lockHeartbeatInterval()).isEqualTo(Duration.ofSeconds(10));
        assertThat(txn.prewriteBatchSize()).isEqualTo(2048);
        assertThat(txn.commitBatchSize()).isEqualTo(512);
        assertThat(txn.batchGetConcurrency()).isEqualTo(64);
        assertThat(txn.resolverCacheSize()).isEqualTo(20000);
        assertThat(txn.enableAsyncCommit()).isFalse();
        assertThat(txn.enableOnePc()).isFalse();
        assertThat(txn.pessimisticWaitTimeoutMs()).isEqualTo(5000);
    }
}
