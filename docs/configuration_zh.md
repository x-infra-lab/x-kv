# 配置参考

[English](./configuration.md) | [中文](./configuration_zh.md)

配置加载优先级：**命令行参数 > 环境变量 > YAML 文件 > 默认值**。

---

## KV 存储（`KvConfig`）

### 顶层配置

| 字段 | 类型 | 默认值 | 说明 |
|------|------|--------|------|
| `storeId` | long | — | 唯一存储标识（必填） |
| `pdEndpoints` | List\<String\> | `[]` | PD 集群端点 |
| `clientAddress` | String | — | 客户端 gRPC 监听地址 |
| `raftAddress` | String | — | Raft 对等 gRPC 监听地址 |
| `dataDir` | Path | — | RocksDB 数据目录 |
| `authToken` | String | — | 客户端认证 Bearer Token |
| `maxConcurrentRequests` | int | `10,000` | 最大并发 gRPC 请求数 |
| `metricsPort` | int | — | Prometheus 指标/健康检查 HTTP 端口 |
| `slowLogThresholdMs` | long | `1000` | 慢请求日志阈值（毫秒） |
| `drainTimeoutMs` | long | `10,000` | 关闭时等待进行中请求的超时（毫秒） |

### 存储引擎（`KvConfig.EngineConfig`）

RocksDB 调优。四个列族：DEFAULT、LOCK、WRITE、RAFT。

| 字段 | 类型 | 默认值 | 说明 |
|------|------|--------|------|
| `blockCacheBytes` | long | `256 MB` | 所有 CF 共享的 Block Cache |
| `writeBufferBytes` | long | `64 MB` | 每个 CF 的写缓冲区大小 |
| `maxBackgroundJobs` | int | `4` | RocksDB 后台压缩/刷新线程数 |
| `enableStatistics` | boolean | `false` | 启用 RocksDB 内部统计 |

### Raft（`KvConfig.RaftConfig`）

| 字段 | 类型 | 默认值 | 说明 |
|------|------|--------|------|
| `electionTickMs` | long | `1000` | 选举超时 tick 间隔 |
| `heartbeatTickMs` | long | `100` | 心跳间隔 |
| `maxSizePerMsg` | int | `1 MB` | 最大 Raft 消息载荷 |
| `maxInflightMsgs` | int | `256` | 最大在途 Append 条目数 |
| `snapshotIntervalEntries` | long | `10,000` | 每 N 条日志生成快照 |
| `applyBatchEntries` | long | `64` | Apply 循环批次大小 |

### Region（`KvConfig.RegionConfig`）

| 字段 | 类型 | 默认值 | 说明 |
|------|------|--------|------|
| `maxRegionBytes` | long | `96 MB` | Region 大小硬限制 |
| `splitRegionBytes` | long | `64 MB` | 超过此大小触发分裂 |
| `mergeRegionBytes` | long | `8 MB` | 合并候选阈值 |
| `regionMaxKeys` | int | `1,440,000` | 每个 Region 最大 Key 数（近似） |

### Worker（`KvConfig.WorkerConfig`）

| 字段 | 类型 | 默认值 | 说明 |
|------|------|--------|------|
| `logCompactionIntervalMs` | long | `60,000` | Raft 日志压缩检查间隔 |
| `logCompactionGapThreshold` | long | `10,000` | applied - first 超过此值时压缩 |
| `logCompactionSafetyMargin` | long | `1,000` | 压缩点之上保留的日志条数 |
| `gcIntervalMs` | long | `60,000` | MVCC 垃圾回收间隔 |

### TLS（`TlsConfig`）

适用于 `clientTls` 和 `raftTls`。

| 字段 | 类型 | 默认值 | 说明 |
|------|------|--------|------|
| `certPath` | String | — | PEM 证书文件 |
| `keyPath` | String | — | PEM 私钥文件 |
| `caPath` | String | — | PEM CA 证书（设置后启用 mTLS） |

---

## Placement Driver（`PdConfig`）

### 顶层配置

| 字段 | 类型 | 默认值 | 说明 |
|------|------|--------|------|
| `nodeId` | long | — | PD 节点唯一标识（必填） |
| `clusterId` | long | — | 集群标识（必填，所有节点须一致） |
| `clientAddress` | String | — | 客户端 gRPC 监听地址 |
| `raftAddress` | String | — | PD 间 Raft gRPC 监听地址 |
| `peers` | List\<PeerAddress\> | `[]` | 其他 PD 节点 |
| `dataDir` | Path | — | PD 状态机 RocksDB 数据目录 |
| `authToken` | String | — | 客户端认证 Bearer Token |
| `maxConcurrentRequests` | int | `5,000` | 最大并发 gRPC 请求数 |
| `metricsPort` | int | — | Prometheus 指标 HTTP 端口 |
| `slowLogThresholdMs` | long | `1000` | 慢请求日志阈值 |

### TSO（`PdConfig.TsoConfig`）

混合逻辑时钟时间戳分配器。

| 字段 | 类型 | 默认值 | 说明 |
|------|------|--------|------|
| `savedIntervalMs` | long | `50` | 每次 Raft 往返预分配的物理时间窗口 |
| `updateIntervalMs` | long | `10` | 主动延长物理上界的间隔 |

### 调度器（`PdConfig.SchedulerConfig`）

| 字段 | 类型 | 默认值 | 说明 |
|------|------|--------|------|
| `heartbeatIntervalMs` | long | `10,000` | 调度器 tick 间隔 |
| `storeStateTimeoutMs` | long | `30,000` | Store 状态过期超时 |
| `maxOperatorsPerStore` | int | `5` | 单 Store 最大并发 Operator 数 |
| `regionScheduleLimit` | int | `32` | Region 均衡最大待执行 Operator |
| `leaderScheduleLimit` | int | `4` | Leader 均衡最大待执行 Operator |
| `hotRegionScheduleLimit` | int | `4` | 热点 Region 最大待执行 Operator |
| `regionSplitBytes` | long | `64 MB` | 分裂触发阈值 |

### 安全点（`PdConfig.SafePointConfig`）

GC 安全点管理。

| 字段 | 类型 | 默认值 | 说明 |
|------|------|--------|------|
| `defaultGcLifetimeMs` | long | `600,000`（10 分钟） | 默认 GC 生命周期 |
| `advanceIntervalMs` | long | `60,000` | PD 推进安全点的间隔 |
| `serviceSafePointTtlMs` | long | `300,000`（5 分钟） | 服务注册安全点的 TTL（BR/CDC） |

---

## YAML 配置文件

PD 和 KV 均支持 YAML 配置文件，通过 `--config` 指定：

```bash
java -jar x-kv-server.jar --config /etc/x-kv/kv.yml
```

KV YAML 示例：

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
