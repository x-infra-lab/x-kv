package io.github.xinfra.lab.xkv.kv.config;

import io.github.xinfra.lab.xkv.common.tls.TlsConfig;

import java.nio.file.Path;
import java.util.List;

/**
 * Static configuration for one KV store node.
 *
 * <p>Two address surfaces:
 * <ul>
 *   <li>{@code clientAddress} — gRPC for {@code Tikv} service (client traffic)</li>
 *   <li>{@code raftAddress}   — gRPC for {@code KvRaft} service (peer traffic)</li>
 * </ul>
 *
 * <p>Splitting client and peer traffic onto separate ports lets ops snapshot
 * one without affecting the other (firewall-level partition for chaos
 * testing, separate TLS configs, separate connection limits).
 */
public final class KvConfig {

    private final long storeId;
    private final List<String> pdEndpoints;
    private final String clientAddress;
    private final String raftAddress;
    private final Path dataDir;
    private final EngineConfig engine;
    private final RaftConfig raft;
    private final RegionConfig region;
    private final TlsConfig clientTls;
    private final TlsConfig raftTls;
    private final String authToken;
    private final int maxConcurrentRequests;
    private final int metricsPort;
    private final WorkerConfig worker;
    private final long slowLogThresholdMs;
    private final long drainTimeoutMs;
    private final boolean enableDebugService;

    private KvConfig(Builder b) {
        this.storeId = b.storeId;
        this.pdEndpoints = List.copyOf(b.pdEndpoints);
        this.clientAddress = b.clientAddress;
        this.raftAddress = b.raftAddress;
        this.dataDir = b.dataDir;
        this.engine = b.engine == null ? EngineConfig.defaults() : b.engine;
        this.raft = b.raft == null ? RaftConfig.defaults() : b.raft;
        this.region = b.region == null ? RegionConfig.defaults() : b.region;
        this.clientTls = b.clientTls;
        this.raftTls = b.raftTls;
        this.authToken = b.authToken;
        this.maxConcurrentRequests = b.maxConcurrentRequests;
        this.metricsPort = b.metricsPort;
        this.worker = b.worker == null ? WorkerConfig.defaults() : b.worker;
        this.slowLogThresholdMs = b.slowLogThresholdMs;
        this.drainTimeoutMs = b.drainTimeoutMs;
        this.enableDebugService = b.enableDebugService;
    }

    public long storeId() { return storeId; }
    public List<String> pdEndpoints() { return pdEndpoints; }
    public String clientAddress() { return clientAddress; }
    public String raftAddress() { return raftAddress; }
    public Path dataDir() { return dataDir; }
    public EngineConfig engine() { return engine; }
    public RaftConfig raft() { return raft; }
    public RegionConfig region() { return region; }
    public TlsConfig clientTls() { return clientTls; }
    public TlsConfig raftTls() { return raftTls; }
    public String authToken() { return authToken; }
    public int maxConcurrentRequests() { return maxConcurrentRequests; }
    public int metricsPort() { return metricsPort; }
    public WorkerConfig worker() { return worker; }
    public long slowLogThresholdMs() { return slowLogThresholdMs; }
    public long drainTimeoutMs() { return drainTimeoutMs; }
    public boolean enableDebugService() { return enableDebugService; }

    public static Builder builder() { return new Builder(); }

    /**
     * RocksDB tuning. v1 used default options across all CFs and ate the
     * cost: no BlockCache, no Bloom filter, no prefix extractor on the
     * write CF. v2 differentiates per-CF (see {@code StoreEngines}):
     *
     * <ul>
     *   <li><b>default CF</b> — point-lookup-optimized, BlockCache</li>
     *   <li><b>lock CF</b>    — small, point-lookup-optimized, fully cached</li>
     *   <li><b>write CF</b>   — prefix bloom on the {@code userKey} prefix
     *       (everything except the trailing 8-byte commit_ts) so seek-by-userKey
     *       skips IO when no version exists</li>
     * </ul>
     */
    public record EngineConfig(
            long blockCacheBytes,
            long writeBufferBytes,
            int maxBackgroundJobs,
            boolean enableStatistics) {
        public static EngineConfig defaults() {
            return new EngineConfig(256L * 1024 * 1024, 64L * 1024 * 1024, 4, false);
        }
    }

    public record RaftConfig(
            long electionTickMs,
            long heartbeatTickMs,
            int maxSizePerMsg,
            int maxInflightMsgs,
            long snapshotIntervalEntries,
            long applyBatchEntries,
            boolean leaseBasedRead,
            int pollerThreads) {
        public RaftConfig(long electionTickMs, long heartbeatTickMs,
                          int maxSizePerMsg, int maxInflightMsgs,
                          long snapshotIntervalEntries, long applyBatchEntries) {
            this(electionTickMs, heartbeatTickMs, maxSizePerMsg, maxInflightMsgs,
                    snapshotIntervalEntries, applyBatchEntries, true,
                    Math.max(4, Runtime.getRuntime().availableProcessors()));
        }
        public RaftConfig(long electionTickMs, long heartbeatTickMs,
                          int maxSizePerMsg, int maxInflightMsgs,
                          long snapshotIntervalEntries, long applyBatchEntries,
                          boolean leaseBasedRead) {
            this(electionTickMs, heartbeatTickMs, maxSizePerMsg, maxInflightMsgs,
                    snapshotIntervalEntries, applyBatchEntries, leaseBasedRead,
                    Math.max(4, Runtime.getRuntime().availableProcessors()));
        }
        public static RaftConfig defaults() {
            return new RaftConfig(1000, 100, 1024 * 1024, 256, 10_000, 64, true,
                    Math.max(4, Runtime.getRuntime().availableProcessors()));
        }
    }

    public record RegionConfig(
            long maxRegionBytes,
            long splitRegionBytes,
            long mergeRegionBytes,
            int regionMaxKeys) {
        public static RegionConfig defaults() {
            return new RegionConfig(96L << 20, 64L << 20, 8L << 20, 1_440_000);
        }
    }

    public record WorkerConfig(
            long logCompactionIntervalMs,
            long logCompactionGapThreshold,
            long logCompactionSafetyMargin,
            long gcIntervalMs,
            int applyPoolThreads) {
        public static WorkerConfig defaults() {
            return new WorkerConfig(60_000, 10_000, 1_000, 60_000,
                    Math.max(2, Runtime.getRuntime().availableProcessors() / 2));
        }
    }

    public static final class Builder {
        private long storeId;
        private List<String> pdEndpoints = List.of();
        private String clientAddress;
        private String raftAddress;
        private Path dataDir;
        private EngineConfig engine;
        private RaftConfig raft;
        private RegionConfig region;
        private TlsConfig clientTls;
        private TlsConfig raftTls;
        private String authToken;
        private int maxConcurrentRequests = 10_000;
        private int metricsPort;
        private WorkerConfig worker;
        private long slowLogThresholdMs = 1000;
        private long drainTimeoutMs = 10_000;
        private boolean enableDebugService = false;

        public Builder storeId(long v)            { this.storeId = v; return this; }
        public Builder pdEndpoints(List<String> v){ this.pdEndpoints = v; return this; }
        public Builder clientAddress(String v)    { this.clientAddress = v; return this; }
        public Builder raftAddress(String v)      { this.raftAddress = v; return this; }
        public Builder dataDir(Path v)            { this.dataDir = v; return this; }
        public Builder engine(EngineConfig v)     { this.engine = v; return this; }
        public Builder raft(RaftConfig v)         { this.raft = v; return this; }
        public Builder region(RegionConfig v)     { this.region = v; return this; }
        public Builder clientTls(TlsConfig v)     { this.clientTls = v; return this; }
        public Builder raftTls(TlsConfig v)       { this.raftTls = v; return this; }
        public Builder authToken(String v)        { this.authToken = v; return this; }
        public Builder maxConcurrentRequests(int v){ this.maxConcurrentRequests = v; return this; }
        public Builder metricsPort(int v)         { this.metricsPort = v; return this; }
        public Builder worker(WorkerConfig v)     { this.worker = v; return this; }
        public Builder slowLogThresholdMs(long v) { this.slowLogThresholdMs = v; return this; }
        public Builder drainTimeoutMs(long v) { this.drainTimeoutMs = v; return this; }
        public Builder enableDebugService(boolean v) { this.enableDebugService = v; return this; }
        public KvConfig build() { return new KvConfig(this); }
    }
}
