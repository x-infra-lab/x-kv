# x-kv 架构文档

## 1. 系统概述

x-kv 是一个分布式事务 KV 存储系统，目标是实现与 TiKV v8.x 的线协议完全兼容。技术栈基于 Java 17、RocksDB（JNI）、gRPC 和 Multi-Raft（通过 x-raft-lib 实现）。

系统由三个核心组件构成：

- **PD（Placement Driver）** -- 集群元数据管理、时间戳分配、Region 调度。
- **KV Store** -- 基于 Multi-Raft 的存储引擎，支持 MVCC 和 Percolator 事务模型。
- **Client SDK** -- 感知 PD 拓扑的 Java 客户端，内置 Region 缓存、锁解析和两阶段提交。

```
                        +-----------+
                        |  Client   |
                        |   SDK     |
                        +-----+-----+
                              |
                 +------------+------------+
                 |                         |
          TSO / 路由查询            原始 KV / 事务 RPC
                 |                         |
           +-----v-----+            +-----v-----+
           |    PD      |            |  KV Store  |
           | (3 节点 HA) |            | (Multi-Raft)|
           +-----------+            +-----------+
```

---

## 2. 模块架构

项目包含 7 个 Maven 模块，模块间无循环依赖。

```
                      +----------+
                      |  tests   |
                      +----+-----+
                           |
              +------------+------------+
              |            |            |
         +----v----+  +----v----+  +----v----+
         |  client |  |   ctl   |  |   kv    |
         +---------+  +---------+  +----+----+
              |            |            |
              +------+-----+       +----+
                     |             |
                +----v----+   +----v----+
                |  proto  |   |   pd    |
                +---------+   +---------+
                     |             |
                +----v-------------v----+
                |        common         |
                +-----------------------+
```

### proto（x-proto）

10 个 `.proto` 文件，提供 TiKV v8.x gRPC 线协议接口：

| 服务          | RPC 数量 |
|---------------|----------|
| Tikv          | 29 + BatchCommands |
| PD            | 22       |
| KvRaft        | 2        |
| ChangeData    | 2        |
| Debug         | 9        |

### common（x-common）

各上层模块共享的基础工具：

- TLS / mTLS 配置
- gRPC 认证拦截器
- 限流
- 指标采集（Micrometer + Prometheus）
- 结构化日志
- 配置加载

### pd（x-pd）

Placement Driver -- 集群的"大脑"：

- **HlcTsoOracle** -- 混合逻辑时钟时间戳分配器，跨 Leader 切换严格单调递增。
- **RocksDbPdStateMachine** -- PD 元数据的 Raft 状态机。
- **调度器** -- Leader 均衡、Region 均衡、分裂检查器。
- **DeadlockDetector** -- 分布式死锁检测（悲观事务场景）。
- **SafePointService** -- GC 安全点管理。
- **PdRaftNode** -- 通过内部 Raft 共识实现 3 节点高可用。

### kv（x-kv-store）

Multi-Raft KV 存储引擎：

- **RocksStorageEngine** -- 单个 RocksDB 实例，4 个列族（DEFAULT、LOCK、WRITE、RAFT）。
- **RegionPeerImpl** -- 每个 Region 的 Raft Peer 生命周期与快照管理。
- **MvccTxn** -- MVCC 事务逻辑（Percolator 模型）。
- **ConcurrencyManager** -- 32 分片读写锁，控制并发访问。
- **GrpcRaftTransport** -- 跨 Store 的 Raft 消息传输层。
- **CDC** -- 变更数据捕获（Change Data Capture）事件流。

### client（x-client）

感知 PD 拓扑的 Java SDK：

- **PdClient** -- PD 连接管理、TSO 获取、集群信息查询。
- **RegionCache** -- 基于 `TreeMap` + epoch 的 Region 路由缓存。
- **TsoBatcher** -- 双向流合并，实现高吞吐量 TSO 分配。
- **TwoPhaseCommitter** -- Percolator 两阶段提交协调器。
- **LockResolver** -- 过期锁检测与解析。
- **Backoffer** -- 指数退避与随机抖动重试策略。
- **RawKvClient / TxnClient** -- 面向用户的原始 KV 和事务接口。

### ctl（x-kv-ctl）

命令行运维工具：集群检查、Store 管理、Region 操作、GC 触发等。

### tests（x-tests）

集成与正确性测试：68 个测试文件、309 个测试方法、`ClusterHarness` 嵌入式多节点集群、线性一致性检查器。

### 依赖关系

模块间无循环依赖。`proto` 和 `common` 是叶子模块。`pd` 和 `kv` 处于同一层级，彼此无依赖。`client` 和 `ctl` 依赖 `proto` + `common`。`tests` 位于最顶层，依赖所有模块。

```
tests --> client, ctl, kv, pd, proto, common
client --> proto, common
ctl    --> proto, common
kv     --> proto, common
pd     --> proto, common
proto  --> common
common --> (无)
```

---

## 3. 组件交互

### 客户端与 PD 的交互

```
+----------+                     +----------+
|  Client  | --- GetTimestamp --> |    PD    |
|          | --- GetRegion ----> |          |
|          | --- GetStore -----> |          |
|          | <-- TSO 响应 -------|          |
|          | <-- Region 元数据 --|          |
+----------+                     +----------+
```

客户端访问 PD 的主要目的：

1. 分配时间戳（TSO），用于事务的 startTs 和 commitTs。
2. 查询 Region 路由（Key 范围到 Store 地址的映射）。
3. 发现 Store 拓扑信息。

### 客户端与 KV Store 的交互

```
+----------+                     +----------+
|  Client  | --- RawGet/Put ---> | KV Store |
|          | --- Prewrite -----> |          |
|          | --- Commit -------> |          |
|          | --- Scan ---------->|          |
|          | --- CheckTxnStatus >|          |
|          | <-- 响应 ---------- |          |
+----------+                     +----------+
```

原始 KV 操作绕过事务层直接执行。事务操作使用 Percolator 系列 RPC（Prewrite、Commit、Cleanup、CheckTxnStatus、ResolveLock）。

### KV Store 与 PD 的交互

```
+----------+                     +----------+
| KV Store | -- StoreHeartbeat ->|    PD    |
|          | -- RegionHeartbeat >|          |
|          | <-- 调度指令 -------|          |
|          |   (分裂、合并、     |          |
|          |    Leader 转移)     |          |
+----------+                     +----------+
```

每个 KV Store 定期上报 Store 级别和 Region 级别的心跳。PD 通过心跳响应下发调度指令（Region 分裂、Region 合并、Leader 转移、副本增删）。

### KV Store 之间的 Raft 复制

```
+-----------+    GrpcRaftTransport    +-----------+
| KV Store  | <-------- Raft -------> | KV Store  |
| (Leader)  | --- AppendEntries ----> | (Follower)|
|           | <-- AppendResponse ---- |           |
|           | --- Snapshot ---------> |           |
+-----------+                         +-----------+
```

Raft 消息（AppendEntries、投票、快照）通过 `GrpcRaftTransport` 在专用端口（20170）上传输。

### PD 之间的内部 Raft 共识

```
+--------+     +--------+     +--------+
| PD-1   |<--->| PD-2   |<--->| PD-3   |
| (Leader)|    |(Follower)|   |(Follower)|
+--------+     +--------+     +--------+
       内部 Raft 共识
       用于元数据复制
```

三个 PD 节点组成内部 Raft 组。所有元数据变更都通过 Raft 共识，即使 PD Leader 发生故障切换也能保证一致性。

---

## 4. 数据流

### 写入路径

```
Client                KV Store (Leader)        Followers
  |                        |                      |
  |--- RawPut/Prewrite --->|                      |
  |                        |-- Raft 提议           |
  |                        |-- 日志复制 ---------->|
  |                        |<-- 确认 --------------|
  |                        |                      |
  |                        |-- 应用:              |
  |                        |   WriteBatch {       |
  |                        |     列族变更,        |
  |                        |     appliedIndex     |
  |                        |   } -> flushWal      |
  |                        |                      |
  |<--- 响应 --------------|                      |
```

单条 Raft 日志的所有变更打包到一个 RocksDB `WriteBatch` 中，包含业务列族的写入和 `appliedIndex` 的更新，通过一次 `flushWal` 刷盘。这保证了状态机应用与 Raft 进度追踪的原子性。

### 读取路径

```
Client                KV Store (Leader)
  |                        |
  |--- RawGet/KvGet ------>|
  |                        |-- ReadIndex（线性一致性读）
  |                        |   或直接引擎读（原始 KV）
  |                        |-- 从 RocksDB 快照读取
  |<--- 响应 --------------|
```

事务读操作先执行 ReadIndex 确认 Leader 身份（保证线性一致性），然后从时间点一致的 RocksDB 快照中读取。原始 KV 读可以走直接引擎读路径。

### 事务流程

```
Client              PD            KV Store (主键)       KV Store (次键)
  |                  |                  |                       |
  |-- GetTimestamp ->|                  |                       |
  |<- startTs -------|                  |                       |
  |                                     |                       |
  |-- 读取（检查锁）----------------->|                       |
  |<- 数据 / 锁信息 -----------------|                       |
  |                                     |                       |
  |-- Prewrite（主键）--------------->|                       |
  |-- Prewrite（次键）---------------------------------------->|
  |<- prewrite 成功 ------------------|                       |
  |<- prewrite 成功 ------------------------------------------------|
  |                                     |                       |
  |-- GetTimestamp ->|                  |                       |
  |<- commitTs ------|                  |                       |
  |                                     |                       |
  |-- Commit（主键）----------------->|                       |
  |<- commit 成功 --------------------|                       |
  |                                     |                       |
  |-- Commit（次键，异步）----------------------------------->|
```

1. **开启事务**：从 PD 的 TSO 获取 `startTs`。
2. **读取**：检查是否存在冲突锁；发现过期锁则进行锁解析。
3. **预写（Prewrite）**：两轮全有或全无的锁获取，覆盖主键和所有次键。
4. **提交（Commit）**：先探测后提交主键；次键的提交异步进行。
5. **清理（Cleanup）**：事务中止时解析残留锁。

---

## 5. 关键不变量

以下不变量是系统正确性的基石。违反任何一条都会导致数据丢失或一致性问题。

### Inv-1：单次 fsync 应用一条 Raft 日志

每条 Raft 日志的应用恰好产生一个 RocksDB `WriteBatch`，包含所有列族的变更以及 `appliedIndex` 的更新，通过一次 `flushWal` 持久化。这保证了业务变更与进度标记要么同时落盘，要么同时丢失。若拆成两次写入，宕机发生在两次写入之间时，会导致 appliedIndex 超前于实际状态（数据丢失）或滞后于实际状态（重复应用）。

### Inv-2：跨列族的单一快照视图

同一请求内的所有列族读取共用一个 RocksDB `Snapshot` 句柄，保证跨 DEFAULT、LOCK、WRITE 列族的时间点一致视图。原子摄取操作也遵守此边界。若不满足此不变量，读操作可能看到 WRITE 记录但读不到对应的 DEFAULT 值，反之亦然。

### Inv-3：PD 状态列族仅通过 Raft 写入

PD 元数据列族的所有变更都经过内部 Raft 状态机，不存在绕过 Raft 的 Leader 快速路径。这保证了 PD 元数据在确认前一定已被复制，防止脑裂导致的元数据分歧。

### Inv-4：TSO 跨 Leader 切换严格单调递增

新的 PD Leader 选举后，以 `physicalBound + 1` 初始化 TSO，并通过 `reloadAfterLeaderChange` 重新加载状态。这保证时间戳永远不会回退，即使经历 Leader 切换也是如此。时间戳回退会破坏快照隔离：新事务的写入可能对持有更早时间戳的快照不可见。

### Inv-5：Region 合并是双边的且 epoch 幂等

Region 合并需要源 Region 和目标 Region 的双边协调。PD 在执行合并和回滚之前都会验证 Region epoch。epoch 幂等性确保过时的合并指令（来自延迟的心跳）会被安全拒绝，而不会被重复执行。

### Inv-6：提交状态有且仅有三态

事务提交解析返回三种状态之一：`COMMITTED`、`ROLLED_BACK`、`UNKNOWN`。`UNKNOWN` 状态永远不会被折叠为其他两种状态。将 `UNKNOWN` 折叠为 `COMMITTED` 有读到未提交数据的风险；折叠为 `ROLLED_BACK` 则可能回滚一个在其他节点上已经提交的事务。

---

## 6. 存储布局

每个 KV Store 使用一个 RocksDB 实例，包含四个列族。

```
+---------------------------------------------------------------+
|                      RocksDB 实例                              |
+---------------+---------------+---------------+---------------+
|  DEFAULT 列族  |   LOCK 列族   |  WRITE 列族   |   RAFT 列族   |
+---------------+---------------+---------------+---------------+
```

### DEFAULT 列族 -- 用户数据

存储 MVCC 编码键对应的用户原始值。

```
Key:   userKey + bigEndian(~ts)     （时间戳降序排列）
Value: 用户原始值字节
```

对时间戳取按位取反（`~ts`）确保在 RocksDB 默认字节序比较器下，较新的版本排在较旧版本之前，使"读取最新版本"只需一次前缀 Seek。

### LOCK 列族 -- Percolator 锁

存储活跃事务的锁信息。

```
Key:   userKey
Value: Lock 编码 {
         lockType,
         primaryKey,
         startTs,
         ttl,
         asyncCommit 字段 (minCommitTs, secondaries),
         pessimistic 字段 (forUpdateTs)
       }
```

### WRITE 列族 -- Percolator 写记录

存储已提交事务的写记录。

```
Key:   userKey + bigEndian(~commitTs)
Value: Write 编码 {
         writeType (Put, Delete, Rollback, Lock),
         startTs,
         shortValue（小值内联）
       }
```

当值小于阈值时，直接内联到 WRITE 记录中，避免额外的 DEFAULT 列族查找。

### RAFT 列族 -- Raft 元数据与日志

存储每个 Region 的 Raft 状态，采用紧凑的键布局（9-17 字节）。

```
存储内容：
  - Raft 日志条目（按 Region 分区）
  - 硬状态（term、vote、commit index）
  - 已应用索引（applied index）
  - 去重映射表
```

---

## 7. 部署架构

### 拓扑

```
                    +------ 网络 ------+
                    |                  |
   +--------+  +--------+  +--------+ |
   | PD-1   |  | PD-2   |  | PD-3   | |   PD 集群（3 节点 Raft 高可用）
   | :2379  |  | :2379  |  | :2379  | |
   +--------+  +--------+  +--------+ |
                    |                  |
   +--------+  +--------+  +--------+ |
   | KV-1   |  | KV-2   |  | KV-3   | |   KV Store（Multi-Raft）
   | :20160 |  | :20160 |  | :20160 | |   每个 Store 承载多个 Region
   | :20170 |  | :20170 |  | :20170 | |
   | :20180 |  | :20180 |  | :20180 | |
   +--------+  +--------+  +--------+ |
                    |                  |
                    +------------------+
```

### 端口

| 端口  | 用途                         |
|-------|------------------------------|
| 2379  | PD 客户端 API（gRPC）         |
| 20160 | KV 客户端 API（gRPC）         |
| 20170 | KV Raft 传输（gRPC）          |
| 20180 | KV 指标暴露（Prometheus）     |

### Docker Compose

项目提供 Docker Compose 支持，标准部署为 3 PD + 3 KV 节点，用于开发和测试环境快速搭建生产级拓扑。

### 最小部署要求

- **PD**：3 节点（Raft 仲裁需要多数派）。
- **KV**：3 个或更多 Store（保证 3 副本数据安全）。
- 所有节点需要本地 SSD 存储，用于 RocksDB 数据和 Raft 日志持久化。
