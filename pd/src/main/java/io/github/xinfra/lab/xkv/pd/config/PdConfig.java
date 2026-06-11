package io.github.xinfra.lab.xkv.pd.config;

import io.github.xinfra.lab.xkv.common.tls.TlsConfig;

import java.nio.file.Path;
import java.util.List;

/**
 * Static configuration for one PD node. Wired via builder; immutable after
 * the server has started so the runtime never reads stale values.
 */
public final class PdConfig {

    private final long nodeId;
    private final long clusterId;
    private final String clientAddress;
    private final String raftAddress;
    private final List<PeerAddress> peers;
    private final Path dataDir;
    private final boolean joinMode;
    private final TsoConfig tso;
    private final SchedulerConfig scheduler;
    private final SafePointConfig safePoint;
    private final TlsConfig clientTls;
    private final TlsConfig raftTls;
    private final String authToken;
    private final int maxConcurrentRequests;
    private final int metricsPort;
    private final long slowLogThresholdMs;

    private PdConfig(Builder b) {
        this.nodeId = b.nodeId;
        this.clusterId = b.clusterId;
        this.clientAddress = b.clientAddress;
        this.raftAddress = b.raftAddress;
        this.peers = List.copyOf(b.peers);
        this.dataDir = b.dataDir;
        this.joinMode = b.joinMode;
        this.tso = b.tso == null ? TsoConfig.defaults() : b.tso;
        this.scheduler = b.scheduler == null ? SchedulerConfig.defaults() : b.scheduler;
        this.safePoint = b.safePoint == null ? SafePointConfig.defaults() : b.safePoint;
        this.clientTls = b.clientTls;
        this.raftTls = b.raftTls;
        this.authToken = b.authToken;
        this.maxConcurrentRequests = b.maxConcurrentRequests;
        this.metricsPort = b.metricsPort;
        this.slowLogThresholdMs = b.slowLogThresholdMs;
    }

    public long nodeId()         { return nodeId; }
    public long clusterId()      { return clusterId; }
    public String clientAddress(){ return clientAddress; }
    public String raftAddress()  { return raftAddress; }
    public List<PeerAddress> peers() { return peers; }
    public Path dataDir()        { return dataDir; }
    public boolean joinMode()    { return joinMode; }
    public TsoConfig tso()       { return tso; }
    public SchedulerConfig scheduler() { return scheduler; }
    public SafePointConfig safePoint() { return safePoint; }
    public TlsConfig clientTls() { return clientTls; }
    public TlsConfig raftTls()   { return raftTls; }
    public String authToken()    { return authToken; }
    public int maxConcurrentRequests() { return maxConcurrentRequests; }
    public int metricsPort()     { return metricsPort; }
    public long slowLogThresholdMs() { return slowLogThresholdMs; }

    public static Builder builder() { return new Builder(); }

    public record PeerAddress(long id, String raftAddress, String clientAddress) {
        public PeerAddress(long id, String raftAddress) {
            this(id, raftAddress, "");
        }
    }

    /**
     * TSO tuning.
     *
     * @param savedIntervalMs window the leader pre-allocates and persists in
     *     one Raft round-trip. Larger values amortize Raft cost but waste up
     *     to this many ms across leader changes (the new leader bumps past
     *     the persisted bound).
     * @param updateIntervalMs how often the leader proactively extends the
     *     persisted physical bound; should be a fraction of savedIntervalMs.
     */
    public record TsoConfig(long savedIntervalMs, long updateIntervalMs) {
        public static TsoConfig defaults() { return new TsoConfig(50, 10); }
    }

    public record SchedulerConfig(
            long heartbeatIntervalMs,
            long storeStateTimeoutMs,
            int maxOperatorsPerStore,
            int regionScheduleLimit,
            int leaderScheduleLimit,
            int hotRegionScheduleLimit,
            long regionSplitBytes) {
        public static SchedulerConfig defaults() {
            return new SchedulerConfig(10_000, 30_000, 5, 32, 4, 4, 64 * 1024 * 1024);
        }
    }

    public record SafePointConfig(
            long defaultGcLifetimeMs,
            long advanceIntervalMs,
            long serviceSafePointTtlMs) {
        public static SafePointConfig defaults() {
            return new SafePointConfig(10 * 60 * 1000L, 60_000L, 5 * 60_000L);
        }
    }

    public static final class Builder {
        private long nodeId;
        private long clusterId;
        private String clientAddress;
        private String raftAddress;
        private List<PeerAddress> peers = List.of();
        private Path dataDir;
        private boolean joinMode;
        private TsoConfig tso;
        private SchedulerConfig scheduler;
        private SafePointConfig safePoint;
        private TlsConfig clientTls;
        private TlsConfig raftTls;
        private String authToken;
        private int maxConcurrentRequests = 5_000;
        private int metricsPort;
        private long slowLogThresholdMs = 1000;

        public Builder nodeId(long v)            { this.nodeId = v; return this; }
        public Builder clusterId(long v)         { this.clusterId = v; return this; }
        public Builder clientAddress(String v)   { this.clientAddress = v; return this; }
        public Builder raftAddress(String v)     { this.raftAddress = v; return this; }
        public Builder peers(List<PeerAddress> v){ this.peers = v; return this; }
        public Builder dataDir(Path v)           { this.dataDir = v; return this; }
        public Builder joinMode(boolean v)       { this.joinMode = v; return this; }
        public Builder tso(TsoConfig v)          { this.tso = v; return this; }
        public Builder scheduler(SchedulerConfig v) { this.scheduler = v; return this; }
        public Builder safePoint(SafePointConfig v) { this.safePoint = v; return this; }
        public Builder clientTls(TlsConfig v)    { this.clientTls = v; return this; }
        public Builder raftTls(TlsConfig v)      { this.raftTls = v; return this; }
        public Builder authToken(String v)       { this.authToken = v; return this; }
        public Builder maxConcurrentRequests(int v) { this.maxConcurrentRequests = v; return this; }
        public Builder metricsPort(int v)        { this.metricsPort = v; return this; }
        public Builder slowLogThresholdMs(long v) { this.slowLogThresholdMs = v; return this; }

        public PdConfig build() { return new PdConfig(this); }
    }
}
