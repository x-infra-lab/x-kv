# Configuration Reference

[English](./configuration.md) | [中文](./configuration_zh.md)

Configuration is loaded in order of precedence: **CLI args > env vars >
YAML file > defaults**. Most settings have sensible defaults.

---

## KV Store (`KvConfig`)

### Top-Level

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `storeId` | long | — | Unique store identifier (required) |
| `pdEndpoints` | List\<String\> | `[]` | PD cluster endpoints (e.g., `["pd1:2379","pd2:2379"]`) |
| `clientAddress` | String | — | gRPC listen address for client traffic |
| `raftAddress` | String | — | gRPC listen address for peer raft traffic |
| `dataDir` | Path | — | RocksDB data directory |
| `authToken` | String | — | Bearer token for client authentication |
| `maxConcurrentRequests` | int | `10,000` | Max in-flight gRPC requests (rate limiter) |
| `metricsPort` | int | — | HTTP port for Prometheus metrics / health checks |
| `slowLogThresholdMs` | long | `1000` | Requests slower than this are logged as WARN |
| `drainTimeoutMs` | long | `10,000` | Grace period on shutdown for in-flight requests |

### Engine (`KvConfig.EngineConfig`)

RocksDB tuning. Four column families: DEFAULT, LOCK, WRITE, RAFT.

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `blockCacheBytes` | long | `256 MB` | Shared block cache across all CFs |
| `writeBufferBytes` | long | `64 MB` | Write buffer (memtable) size per CF |
| `maxBackgroundJobs` | int | `4` | RocksDB background compaction/flush threads |
| `enableStatistics` | boolean | `false` | Enable RocksDB internal statistics |

### Raft (`KvConfig.RaftConfig`)

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `electionTickMs` | long | `1000` | Election timeout tick interval |
| `heartbeatTickMs` | long | `100` | Heartbeat interval |
| `maxSizePerMsg` | int | `1 MB` | Max Raft message payload size |
| `maxInflightMsgs` | int | `256` | Max in-flight append entries |
| `snapshotIntervalEntries` | long | `10,000` | Snapshot every N entries |
| `applyBatchEntries` | long | `64` | Apply loop batch size |

### Region (`KvConfig.RegionConfig`)

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `maxRegionBytes` | long | `96 MB` | Region size hard limit |
| `splitRegionBytes` | long | `64 MB` | Trigger split when region exceeds this size |
| `mergeRegionBytes` | long | `8 MB` | Merge candidate threshold |
| `regionMaxKeys` | int | `1,440,000` | Max keys per region (approximate) |

### Worker (`KvConfig.WorkerConfig`)

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `logCompactionIntervalMs` | long | `60,000` | Raft log compaction check interval |
| `logCompactionGapThreshold` | long | `10,000` | Compact when applied - first > this gap |
| `logCompactionSafetyMargin` | long | `1,000` | Keep this many entries above compact point |
| `gcIntervalMs` | long | `60,000` | MVCC garbage collection interval |

### TLS (`TlsConfig`)

Used for both `clientTls` and `raftTls`.

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `certPath` | String | — | PEM certificate file |
| `keyPath` | String | — | PEM private key file |
| `caPath` | String | — | PEM CA certificate (enables mTLS when set) |

---

## Placement Driver (`PdConfig`)

### Top-Level

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `nodeId` | long | — | Unique PD node identifier (required) |
| `clusterId` | long | — | Cluster identifier (required, must match across nodes) |
| `clientAddress` | String | — | gRPC listen address for client traffic |
| `raftAddress` | String | — | gRPC listen address for inter-PD raft traffic |
| `peers` | List\<PeerAddress\> | `[]` | Other PD nodes (`id=raftAddr,clientAddr`) |
| `dataDir` | Path | — | RocksDB data directory for PD state machine |
| `joinMode` | boolean | `false` | Join an existing cluster (not yet implemented) |
| `authToken` | String | — | Bearer token for client authentication |
| `maxConcurrentRequests` | int | `5,000` | Max in-flight gRPC requests |
| `metricsPort` | int | — | HTTP port for Prometheus metrics / health checks |
| `slowLogThresholdMs` | long | `1000` | Slow log threshold |

### TSO (`PdConfig.TsoConfig`)

Hybrid Logical Clock timestamp oracle.

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `savedIntervalMs` | long | `50` | Physical bound pre-allocation window per Raft round-trip |
| `updateIntervalMs` | long | `10` | Proactive physical bound extension interval |

### Scheduler (`PdConfig.SchedulerConfig`)

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `heartbeatIntervalMs` | long | `10,000` | Scheduler tick interval |
| `storeStateTimeoutMs` | long | `30,000` | Store state expiration timeout |
| `maxOperatorsPerStore` | int | `5` | Max concurrent operators targeting one store |
| `regionScheduleLimit` | int | `32` | Max pending region-balance operators |
| `leaderScheduleLimit` | int | `4` | Max pending leader-balance operators |
| `hotRegionScheduleLimit` | int | `4` | Max pending hot-region operators |
| `regionSplitBytes` | long | `64 MB` | Split trigger threshold |

### Safe Point (`PdConfig.SafePointConfig`)

GC safe-point management.

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `defaultGcLifetimeMs` | long | `600,000` (10 min) | Default GC lifetime |
| `advanceIntervalMs` | long | `60,000` | How often PD advances the safe-point |
| `serviceSafePointTtlMs` | long | `300,000` (5 min) | TTL for service-registered safe-points (BR/CDC) |

---

## YAML Configuration File

Both PD and KV support YAML configuration files. Pass via `--config`
flag:

```bash
java -jar x-kv-server.jar --config /etc/x-kv/kv.yml
```

Example KV YAML:

```yaml
store-id: 1
pd-endpoints:
  - pd1:2379
  - pd2:2379
  - pd3:2379
client-address: 0.0.0.0:20160
raft-address: 0.0.0.0:20170
data-dir: /var/lib/x-kv
metrics-port: 9191
max-concurrent-requests: 10000
slow-log-threshold-ms: 1000

engine:
  block-cache-bytes: 268435456
  write-buffer-bytes: 67108864
  max-background-jobs: 4

raft:
  election-tick-ms: 1000
  heartbeat-tick-ms: 100
  snapshot-interval-entries: 10000

region:
  split-region-bytes: 67108864
  merge-region-bytes: 8388608
```
