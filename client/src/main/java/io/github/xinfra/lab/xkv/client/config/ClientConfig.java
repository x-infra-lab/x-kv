package io.github.xinfra.lab.xkv.client.config;

import io.github.xinfra.lab.xkv.common.tls.TlsConfig;

import java.time.Duration;
import java.util.List;

/**
 * Static configuration for one client instance.
 *
 * <p>v1's client wrote {@code Thread.sleep(100)} for backoff, used a single
 * blocking gRPC channel for TSO, and rebuilt {@code ExecutorService} on
 * every {@code commit()} call. v2 fixes these by surfacing every relevant
 * knob here and feeding them into the layered SDK ({@code Backoffer},
 * {@code TsoBatcher}, {@code RegionRequestSender}).
 */
public final class ClientConfig {

    private final List<String> pdEndpoints;
    private final Duration grpcTimeout;
    private final BackoffConfig backoff;
    private final TsoConfig tso;
    private final RegionCacheConfig regionCache;
    private final TxnConfig txn;
    private final ConnConfig conn;
    private final String authToken;

    private ClientConfig(Builder b) {
        this.pdEndpoints = List.copyOf(b.pdEndpoints);
        this.grpcTimeout = b.grpcTimeout == null ? Duration.ofSeconds(10) : b.grpcTimeout;
        this.backoff = b.backoff == null ? BackoffConfig.defaults() : b.backoff;
        this.tso = b.tso == null ? TsoConfig.defaults() : b.tso;
        this.regionCache = b.regionCache == null ? RegionCacheConfig.defaults() : b.regionCache;
        this.txn = b.txn == null ? TxnConfig.defaults() : b.txn;
        this.conn = b.conn == null ? ConnConfig.defaults() : b.conn;
        this.authToken = b.authToken;
    }

    public List<String> pdEndpoints() { return pdEndpoints; }
    public Duration grpcTimeout() { return grpcTimeout; }
    public BackoffConfig backoff() { return backoff; }
    public TsoConfig tso() { return tso; }
    public RegionCacheConfig regionCache() { return regionCache; }
    public TxnConfig txn() { return txn; }
    public ConnConfig conn() { return conn; }
    public String authToken() { return authToken; }

    public static Builder builder() { return new Builder(); }

    /**
     * Per-error-class backoff parameters mirror client-go's {@code BoConfig}:
     *
     * <pre>
     *   error              base   cap   jitterMs
     *   -------------------------------------------
     *   region miss        2 ms    1s    1
     *   txn lock           100 ms  3s   30
     *   server is busy     500 ms  5s   100
     *   network            500 ms  20s  500
     *   not leader         5 ms    1s   1
     * </pre>
     *
     * <p>Cap on the total elapsed time (across retries) per request:
     * {@code maxOverallElapsedMs}. Beyond it the request fails fast.
     */
    public record BackoffConfig(
            long regionMissBaseMs, long regionMissCapMs,
            long txnLockBaseMs,    long txnLockCapMs,
            long serverBusyBaseMs, long serverBusyCapMs,
            long networkBaseMs,    long networkCapMs,
            long notLeaderBaseMs,  long notLeaderCapMs,
            long maxOverallElapsedMs) {
        public static BackoffConfig defaults() {
            return new BackoffConfig(
                2,  1_000,
                100, 3_000,
                500, 5_000,
                500, 20_000,
                5,   1_000,
                40_000);
        }
    }

    /**
     * TSO batcher tuning. v1 issued one blocking RPC per {@code getTimestamp(1)};
     * v2 maintains a single bidirectional stream and batches concurrent
     * callers by waiting up to {@code batchWaitMicros} or until
     * {@code maxBatchSize} reach.
     */
    public record TsoConfig(int batchWaitMicros, int maxBatchSize, Duration streamReconnectBackoff) {
        public static TsoConfig defaults() {
            return new TsoConfig(50, 4096, Duration.ofMillis(200));
        }
    }

    public record RegionCacheConfig(
            int maxEntries,
            Duration entryTtl,
            Duration negativeTtl) {
        public static RegionCacheConfig defaults() {
            return new RegionCacheConfig(50_000, Duration.ofMinutes(10), Duration.ofSeconds(5));
        }
    }

    public record TxnConfig(
            Duration defaultLockTtl,
            Duration lockHeartbeatInterval,
            int prewriteBatchSize,
            int commitBatchSize,
            int batchGetConcurrency,
            int resolverCacheSize,
            Duration resolverCacheTtl,
            boolean enableAsyncCommit,
            boolean enableOnePc,
            long pessimisticWaitTimeoutMs) {
        public static TxnConfig defaults() {
            return new TxnConfig(
                Duration.ofSeconds(20),
                Duration.ofSeconds(5),
                1024, 1024, 32,
                10_000, Duration.ofMinutes(5),
                true, true,
                3000L);
        }
    }

    public record ConnConfig(
            int maxStoreConnections,
            int idleConnectionTtlSec,
            TlsConfig tls) {
        public static ConnConfig defaults() {
            return new ConnConfig(4, 600, null);
        }
    }

    public record RetryConfig(
            int maxRetries,
            long backoffBaseMs,
            long backoffCapMs) {
        public static RetryConfig defaults() {
            return new RetryConfig(20, 5, 1000);
        }
    }

    public static final class Builder {
        private List<String> pdEndpoints = List.of();
        private Duration grpcTimeout;
        private BackoffConfig backoff;
        private TsoConfig tso;
        private RegionCacheConfig regionCache;
        private TxnConfig txn;
        private ConnConfig conn;
        private String authToken;

        public Builder pdEndpoints(List<String> v)       { this.pdEndpoints = v; return this; }
        public Builder grpcTimeout(Duration v)           { this.grpcTimeout = v; return this; }
        public Builder backoff(BackoffConfig v)          { this.backoff = v; return this; }
        public Builder tso(TsoConfig v)                  { this.tso = v; return this; }
        public Builder regionCache(RegionCacheConfig v)  { this.regionCache = v; return this; }
        public Builder txn(TxnConfig v)                  { this.txn = v; return this; }
        public Builder conn(ConnConfig v)                { this.conn = v; return this; }
        public Builder authToken(String v)               { this.authToken = v; return this; }

        public ClientConfig build() { return new ClientConfig(this); }
    }
}
