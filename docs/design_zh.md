# x-kv 设计文档

x-kv 是一个 TiKV 兼容的分布式事务 KV 存储，采用 Java 编写。基于单 RocksDB 多 CF 存储引擎实现 Percolator 事务协议，通过 [x-raft-lib](https://github.com/xinfra-lab/x-raft-lib) 提供 Raft 复制，使用 Placement Driver (PD) 进行集群管理，并提供完整的客户端 SDK。

---

## 1. 存储引擎

### 架构

节点上所有 Region 的数据存储在单个 RocksDB 实例中，包含四个列族（CF）：`default`、`lock`、`write` 和 `raft`。`RocksStorageEngine` 类是 RocksDB 句柄及所有 CF 句柄的唯一持有者。

```
RocksDB 实例
  +-- CF default   (MVCC 值)
  +-- CF lock      (Percolator 锁记录)
  +-- CF write     (MVCC 提交/回滚记录)
  +-- CF raft      (每 Region 的 Raft 日志、HardState、Applied Index)
```

### 各 CF 调优

每个 CF 在 `RocksStorageEngine.open()` 中配置了针对各自负载特征的 RocksDB 参数：

- **DEFAULT CF** — 针对点查优化。10-bit 布隆过滤器，共享 LRU block cache，16 KB block size，启用 `optimizeForPointLookup`，LZ4 压缩，level-style compaction + dynamic level bytes。

- **LOCK CF** — 与 DEFAULT 类似，但 write buffer 更小（上限 16 MB），仅 2 个 memtable（`maxWriteBufferNumber=2`）。锁条目小且短命，CF 始终热在缓存中。

- **WRITE CF** — 全键布隆过滤器（`setWholeKeyFiltering(true)`），16 KB block size。主要访问模式为按 userKey seek（除末尾 8 字节 `~commitTs` 后缀外的所有部分）。LZ4 压缩，4 个 memtable。

- **RAFT CF** — 针对顺序写优化，64 KB block size（追加写为主的 Raft 日志条目）。无布隆过滤器（顺序访问无收益）。2 个 memtable。

### 共享 Block Cache

四个 CF 共享同一个 `LRUCache`（默认 256 MB，可通过 `KvConfig.EngineConfig.blockCacheBytes` 配置）。索引和过滤器块被缓存并钉在 L0（`setCacheIndexAndFilterBlocks(true)`、`setPinL0FilterAndIndexBlocksInCache(true)`）。

### WriteBatch 原子性

单个 Raft 条目内的所有 CF 变更打包到一个 `org.rocksdb.WriteBatch` 中。批量写入以 `sync=false` 执行，在每个 Raft Ready 轮次结束时通过一次 `flushWal(true)` 刷盘。这保证了：

- **Inv-1**：Applied Index 与业务数据的原子持久化。Ready 处理期间崩溃要么持久化整个批次，要么什么都不持久化。
- **每 Ready 轮次单次 fsync**：相比逐条目 fsync，显著降低写放大。

### Snapshot 生命周期

`RocksSnapshot` 封装 `org.rocksdb.Snapshot`。快照会钉住 SST 文件（阻碍 compaction 并占用磁盘空间），必须通过 `close()` 显式释放（调用 `RocksDB.releaseSnapshot()`）。`MvccReader` 将三个数据 CF 的读取绑定到同一快照，确保跨 CF 一致性。

### SnapshotEngine

`SnapshotEngineImpl` 负责 Raft 快照的生成和安装：

- **生成**：起始时钉住一个 RocksDB 快照，在该快照下遍历三个数据 CF（`default`、`lock`、`write`），以 `[4B keyLen][key][4B valueLen][value]` 线格式发射 64 KB 数据块，每块附带 CRC32C 校验和。

- **安装**：对每个接收到的数据块验证 CRC32C。通过原子 `WriteBatch` 完成安装：(1) 对三个 CF 执行 `deleteRange` 清除 Region 键范围内的旧数据，(2) 写入快照中的所有键值对。批量刷盘时 `sync=true`。

---

## 2. Raft 集成

### 库依赖

x-kv 使用 x-raft-lib 作为外部库。每个 Region 有一个 `RegionPeerImpl` 实例，持有一个 x-raft-lib `Node`。

### RegionPeerImpl

核心 Raft 集成类。关键配置（通过 `Config.builder()`）：

- `preVote(true)` + `checkQuorum(true)` — 防止被网络分区隔离的节点发起破坏性选举。
- `maxSizePerMsg(1 << 20)` — 每条 Raft 消息上限 1 MB。
- `maxInflightMsgs(256)` — Append Entries 的流水线深度。

### Apply 循环（readyLoop）

`readyLoop()` 每次迭代从 x-raft-lib 拉取一个 `Ready` 并处理：

**阶段 0 — 快照安装**：如果 Leader 发送了快照（`MsgSnapshot`），先安装用户数据 CF 和 Raft 元数据，再处理任何日志条目。

**阶段 A — 日志持久化**：将新条目和 HardState 写入一个 `WriteBatch`（`sync=false`）。条目立即落入 memtable + WAL 缓冲区。

**阶段 B — 应用已提交条目**：每条已提交条目独立应用在自己的 `WriteBatch` 中。批次包含该条目的业务 CF 变更 + `appliedIndex` 递增，以 `sync=false` 写入。逐条目应用至关重要：MVCC apply 路径在冲突检查时读取 RocksDB 状态（`engine.get`）；如果两个条目共享同一批次，条目 N 的读取器将无法看到条目 N-1 已暂存但未刷盘的 lock CF 写入。

**最终 flushWal**：在 Ready 轮次结束时执行一次 `flushWal(true)`，将阶段 A + 阶段 B 的所有写入 fsync 到磁盘。此操作必须在 `node.advance()` 告知 Raft 数据已持久化之前完成。

### ReadIndex 实现线性一致性读

`RegionPeerImpl` 通过 Raft ReadIndex 协议实现线性一致性读：

1. 调用方调用 `readIndex()`，生成 UUID 请求上下文并调用 `node.readIndex(ctx)`。
2. `pendingReadIndices` 映射（键：`ByteBuffer` 封装的 UUID，值：`CompletableFuture<Void>`）跟踪进行中的请求。
3. 当 `Ready.readStates()` 到达时，Ready 循环将上下文与待处理的 Future 匹配。如果 `appliedIndex >= readState.index()`，Future 立即完成。否则存入 `readIndexWaiters`（`ConcurrentSkipListMap<Long, List<CompletableFuture<Void>>>`），在每次已提交条目批量 apply 后排空。

### GrpcRaftTransport

每 Region 的传输层。关键设计：

- **按目标节点复用 Channel**：`ConcurrentHashMap<Long, OutboundLink>` 以 peer ID 为键。Channel 在首次发送时延迟创建。
- **双向流**：出站 Raft 消息通过流式 `StreamObserver` 发送。流错误触发下次发送时自动重连。
- **快照投递**：`MsgSnapshot` 消息通过独立的 `sendSnapshot` 流式 RPC 发送，不走普通消息流。

### RaftMessageDispatcher

每 Store 的路由表，`region_id` → 本地 `GrpcRaftTransport`：

- `ConcurrentHashMap<Long, GrpcRaftTransport>` 实现 O(1) 分发。
- 按需创建 Peer：当收到未知 Region 的消息时，触发 `MissingRegionHandler` 回调（每 Region 仅一次，由 `spawnInFlight` 标志保护）。

### LogCompactionWorker

定期 Raft 日志压缩：

- 扫描所有 Leader Peer。对每个 Peer，如果 `appliedIndex - firstIndex > gapThreshold`（默认 10,000），提议 `ADMIN_COMPACT_LOG`，将 `firstIndex` 推进到 `appliedIndex - safetyMargin`（默认 1,000）。
- 压缩前在 `appliedIndex` 处生成快照，以便慢 Follower 仍可追赶。
- 可配置间隔（默认 60s）、间隙阈值和安全边距。

---

## 3. MVCC + Percolator 事务协议

### 键编码

`MvccKey` 将 MVCC 键编码为 `userKey || bigEndian(~ts)`。对时间戳取反后大端编码，使最新版本在 RocksDB 的字典序中排列在前。后缀始终为 8 字节（`TS_SUFFIX_LEN = 8`）。

- `encode(userKey, ts)` — 生成物理键。
- `firstVersionFor(userKey)` — `encode(userKey, Long.MAX_VALUE)`，用于 seek 到最新版本。
- `afterAllVersionsOf(userKey)` — 追加 9 字节 `0xFF`，用于跳过 userKey 的所有版本。

### 锁编码（v3）

`Lock` 使用二进制格式版本 `0x33`：

```
[1B version=0x33] [1B type] [8B startTs] [8B ttlMs] [8B txnSize]
[8B minCommitTs] [8B forUpdateTs] [1B flags: bit0=useAsyncCommit]
[4B primaryLen] [primaryLen B primary]
[4B secondariesCount] { [4B keyLen] [keyLen B key] }*
```

锁类型：`PUT`、`DELETE`、`LOCK`、`PESSIMISTIC`。

v3 相比 v1 的关键改进：`forUpdateTs` 用于悲观锁（v1 中仅运行时存在，重启后丢失），`flags + secondaries` 用于异步提交（v1 声明了该选项但磁盘锁不携带 secondaries，导致锁解析器恢复不可能）。

### Write 编码（v3）

`Write` 使用二进制格式版本 `0x33`：

```
[1B version=0x33] [1B type] [8B startTs] [1B flags: bit0=hasOverlappedRollback]
[4B shortValueLen] [shortValueLen B shortValue]
```

Write 类型：`PUT`、`DELETE`、`ROLLBACK`、`LOCK`。

**短值内联**：不超过 `SHORT_VALUE_MAX_LEN`（64 字节）的值直接内联在 Write 记录中。MVCC 读取器可仅从 write CF 完成 `Get`，无需二次读取 default CF，对小值负载可将快照读延迟降低约一半。

**hasOverlappedRollback**：用于异步提交 / 1PC，标识与并发写入重叠的回滚记录。锁解析器据此标志避免重复中止。

### MvccReader

`MvccReader` 执行跨 CF 的快照绑定读取。所有读取绑定到同一个 `StorageEngine.Snapshot`，确保跨 CF 一致性。

**Get 协议**：
1. 读取 lock CF 中 userKey 的锁。如果存在 `startTs <= readTs` 的锁且类型不是 `LOCK` 或 `PESSIMISTIC`，抛出 `KeyLockedException`。
2. 在 write CF 中从 `MvccKey.encode(userKey, readTs)` 处 seek。向前遍历 userKey 的版本，跳过 `ROLLBACK` 和 `LOCK` 记录，直到找到第一个 `PUT` 或 `DELETE`。
3. 对于 `PUT`：如果有内联短值则直接返回；否则读取 default CF 中 `MvccKey.encode(userKey, write.startTs)` 处的值。对于 `DELETE`：返回未找到。

**Scan**：对 write CF 进行单次正向扫描。对每个 userKey，选取第一个可见的（commitTs <= readTs 且非回滚）write 记录，解析其值，然后通过 `afterAllVersionsOf()` 跳过该 userKey。

### MvccTxn

服务端单个 Percolator 轮次的累积器。关键操作：

**checkPrewrite**：只读冲突检测。检查：
1. 自身回滚（该 startTs 处存在 ROLLBACK 记录）。
2. 写入冲突（存在 commitTs >= startTs 的非回滚 write 记录）。
3. 被其他事务锁定。
4. 幂等 OK：同一事务的锁已存在。

**writePrewrite**：两阶段全有或全无。调用方必须先对每个键执行 `checkPrewrite`；仅当所有检查通过后，才对每个键执行 `writePrewrite`。这避免了 v1 单阶段设计导致的孤儿锁问题。

**commit**：验证 `commitTs > startTs` 且 `commitTs >= lock.minCommitTs`。将锁提升为 Write 记录。在可能时将短值内联到 Write 记录中并删除 default CF 条目。

**rollback**：对已提交事务返回已发现的 `commitTs`（v1 竞态修复）。在 `(key, startTs)` 处写入 `ROLLBACK` 记录并删除已有的锁 + default CF 条目。

**checkTxnStatus**：TTL 检查 + 并发提交再检查（v1 修复：防止对已提交事务强制回滚）。包含异步提交门控：绝不对异步提交的 primary 强制回滚，因为事务可能已通过 secondaries 的 `min_commit_ts` 路径提交。

**pessimisticLock**：以 `forUpdateTs`（而非 `startTs`）进行写冲突检查来获取悲观锁。支持重新加锁（刷新 `forUpdateTs`）。

### MaxTsTracker

每 Region 的 `AtomicLong`，跟踪该 Leader 服务过的最大读时间戳。每次快照读调用 `observe(readTs)`。每次 prewrite 推导 `minCommitTs = max(startTs + 1, maxTs + 1)`。这保证没有"事后"提交能潜入已服务的 `readTs` 之下，维护快照隔离。

定期持久化（在 Ready 轮次的 fsync 批次中），重启后 Leader 至少以该下界恢复。

### ConcurrencyManager

32 条带的 `ReentrantReadWriteLock`（可配置），按 `Math.floorMod(Arrays.hashCode(key), stripeCount)` 选择条带。

- **点读取器**：获取一个条带的读锁，调用 `maxTs.observe(readTs)`，执行操作。
- **粗粒度读取器**（scan）：按升序获取所有条带的读锁。
- **多键写入器**：通过 `BitSet` 计算去重的条带集合，按升序获取写锁（免死锁）。
- **粗粒度写入器**（快照安装、Region 管理）：按升序获取所有条带的写锁。

### 三态提交结果

`TwoPhaseCommitterImpl.CommitResult` 有三个终态：`COMMITTED`、`ROLLED_BACK`、`UNKNOWN`。当 primary commit 以模糊方式失败（网络错误、响应丢失）时返回 `UNKNOWN`。v1 将 `UNKNOWN` 归并到 `ROLLED_BACK`，导致事务丢失。

---

## 4. Placement Driver (PD)

### HlcTsoOracle

混合逻辑时钟编码：`(physical_ms << 18) | logical`。18 位逻辑计数器每毫秒支持 262,143 次分配。

**单调性契约**：
- 物理上界通过 Raft 持久化。分配器可自由签发 physical <= 此上界的 TSO。
- 构造时：`currentPhysical = physicalBound + 1`（+1 至关重要——v1 存在差一错误）。
- Leader 切换时：`reloadAfterLeaderChange()` 设置 `currentPhysical = physicalBound + 1`、`currentLogical = 0`。v1 忘记在 Leader 观察者上接入此逻辑。
- **单次飞行扩展**：当游标接近 `physicalBound` 时，并发分配器通过 `CompletableFuture` 共享一个进行中的 Raft 提议。绝不发起两个并发扩展。
- **正确性不依赖 NTP**：墙上时钟仅作为提示；单调性依赖持久化的上界 + Raft 排序。

默认前瞻窗口：50ms（`DEFAULT_SAVED_INTERVAL_MS`），上限 1000ms。

### RocksDbPdStateMachine

PD 状态持久化在专用 RocksDB 中，仅通过 Raft 写入（所有变更走 Raft 提议——Inv-3）。热路径读取使用写穿缓存。

### 调度器

- **LeaderBalanceScheduler**：跨 Store 均衡 Leader，感知 `StoreStatsCache` 中的慢分数指标。
- **RegionBalanceScheduler**：按数量均衡 Region，检查低可用空间率以避免向接近满的 Store 迁移数据。
- **SplitCheckerScheduler**：当 Region 近似大小超过 64 MB 阈值（`KvConfig.RegionConfig.splitRegionBytes`）时触发分裂。

### DeadlockDetector

用于悲观锁死锁检测的全局等待图：

- 在 `addWaitFor(edge)` 时：插入边，从持有者到等待者执行 BFS。如果可达，则发现环——返回环链（等待者优先序）。检测到环时不插入该边。
- 每次插入 O(V + E) 的 BFS 复杂度。
- 基于 TTL 的垃圾回收（`cleanupExpired()`），防止崩溃的客户端永久钉住边。
- 通过 synchronized 方法保证线程安全。适用于数千个进行中的悲观事务。

### SafePointService

全局 GC 安全点管理，支持每服务注册：

- **全局安全点**：MVCC 版本可被 GC 回收的下界。
- **服务安全点**：BR / CDC / 长时间运行的 SQL 注册带 TTL 的安全点下界。活跃期间，全局安全点不能推进超过它。
- `advance()`：定期推进全局安全点，尊重活跃的服务注册。
- `updateGcSafePoint(target)`：运维驱动的单调棘轮。

### PD 高可用

通过 `PdRaftNode` + `PdRaftTransport` 的 3 节点 HA。PD 集群本身使用 Raft 进行状态复制，确保 TSO、集群元数据和调度决策在 Leader 故障时存活。

---

## 5. 客户端 SDK

### PdClient

发现 PD Leader，流式获取 TSO，路由 Region 查询。提供阻塞和异步两种 stub。`switchLeader()` 处理 PD Leader 故障转移。

### TsoBatcherImpl

到 PD 的单条双向流用于 TSO 分配：

- 调用线程通过 `getTimestamps(count)` 入队，接收 `CompletableFuture<Long>`。
- 一个分发线程排空队列，将条目合并（最多 `maxBatchSize` 个或等待 `batchWaitMicros` 超时）为单个 `TsoRequest`。
- PD 响应携带首个已分配的 TSO + 数量；分发线程将其拆分为独立的 Future。
- 流错误使所有进行中的 Future 失败并触发重连。
- 单条 TCP 连接可承载 >100k TSO/s（v1 每次 `getTimestamp(1)` 阻塞调用，上限约 1k/s）。

### RegionCacheImpl

双索引 Region 缓存：

- `byStartKey`：`TreeMap<byte[], RegionInfo>`（无符号字典序比较），通过 `floorEntry` 进行基于键的查找。
- `byId`：`ConcurrentHashMap<Long, RegionInfo>`，用于基于 ID 的查找。
- **Epoch 优势检查**：`(conf_ver, version)` 更旧的更新被丢弃，防止过期的 PD 回复覆盖更新的缓存。
- **重叠失效**：在分裂/合并时，使用 `TreeMap.subMap` 实现 O(log N + k) 的范围失效（替代 v1 的全表扫描）。
- 由 `ReentrantReadWriteLock` 保护。

### StoreChannelCache

将 `storeId` 映射到 `ManagedChannel`，通过 PD 的 `GetStore` RPC 解析。Channel 跨 Region 复用。

### BackofferImpl

每请求的指数退避 + 抖动：

- 按错误类别区分 base/cap：`REGION_MISS`、`TXN_LOCK`、`SERVER_BUSY`、`NETWORK`、`NOT_LEADER` 等。
- 公式：`sleep = min(cap, base * 2^attempt) +/- jitter`。
- 总耗时受 `maxOverallElapsedMs` 限制（默认 40s）。
- 感知 gRPC Context deadline：有效 deadline = `min(配置预算, 外部 deadline)`。
- `fork()` 创建子退避器，共享相同 deadline。

### TwoPhaseCommitterImpl

驱动 Percolator 2PC 协议：

1. **Prewrite**：按 Region 分组变更。先 prewrite primary Region，再 prewrite secondaries。任何 prewrite 错误都回滚整个事务。
2. **获取 commitTs**：通过 `TsoBatcher` 从 PD 获取 TSO。
3. **提交 Primary**：如果 primary 提交以已知永久性错误（已回滚）失败，返回 `ROLLED_BACK`。如果模糊失败，返回 `UNKNOWN`。
4. **异步 Secondaries**：尽力而为；secondary 失败不改变事务结果。锁解析器稍后清理。
5. **1PC 短路**：单键乐观事务在服务端返回 `onePcCommitTs > 0` 时跳过提交阶段。
6. **异步提交**：启用时，`commitTs = max(fetched commitTs, primaryMinCommitTs)`。

### LockResolverImpl

解析遇到的锁：

- **Caffeine LRU 缓存**：`lock_ts` 到终态判决（COMMITTED 或 ROLLED_BACK）的映射。有界大小 + TTL，替代 v1 中无界的 `ConcurrentHashMap`（会增长到 OOM）。
- **每 lock_ts 单次飞行**：`ConcurrentMap<Long, CompletableFuture<Verdict>>` 确保 N 个并发读取器命中同一锁时，都等待同一个 `CheckTxnStatus` + `ResolveLock` 对。
- **诚实的 CheckTxnStatus 链**：当状态返回 `NoAction`（锁存活，TTL 未过期）时，解析器不调用 `ResolveLock`。v1 对活跃事务强制回滚导致资金丢失。

### TransactionImpl

客户端事务：

- **缓冲写入**：`TreeMap<byte[], Mutation>` 按键排序（确定性 primary 选择）。最后写入获胜语义。
- **读自己的写入**：`get()` 先检查缓冲区。`scan()` / `reverseScan()` 合并服务端结果与缓冲变更。
- **锁解析重试**：遇到 `KeyError(locked)` 时，解析锁并在退避预算内重试。
- **悲观加锁**：`lockKeysForUpdate()` 从 TSO 获取新的 `forUpdateTs`，按 Region 分组发起 `PessimisticLockRequest`。死锁错误视为不可重试。
- **三种终态**：`COMMITTED`、`ROLLED_BACK`、`UNKNOWN`。`close()` 在仍为 `ACTIVE` 状态时自动回滚。

---

## 6. Region 生命周期

### 分裂

由 `SplitDriver` + `SplitCheckerScheduler` 驱动：

1. PD 通过 `AskBatchSplit` 分配新的 Region ID 和 Peer ID。
2. 构建 `SplitRegionProposal`，包含缩小后的父 Region + 子 Region。每个结果 Region 的 `epoch.version` 递增。
3. 通过 Raft 提议 `ADMIN_SPLIT`。Apply 时：原子 `WriteBatch` 持久化所有 Region 描述符，刷新父 Region 的内存描述符，为每个子 Region 触发分裂观察者。

分裂仅涉及元数据：数据留在共享 RocksDB 中。阈值为近似大小 > 64 MB（`KvConfig.RegionConfig.splitRegionBytes`）。

### 合并

通过 `MergeProtocolImpl` 的三阶段协议：

1. **PrepareMerge**：源 Region 提议 `ADMIN_PREPARE_MERGE`，进入静默状态（拒绝所有非合并的业务写入）。记录准备时刻目标 Region 的 epoch。
2. **CommitMerge**：目标 Region 提议 `ADMIN_COMMIT_MERGE`，携带合并后的描述符（扩展范围，递增 `epoch.version`）。
3. **RollbackMerge**：源 Region 可提议 `ADMIN_ROLLBACK_MERGE` 退出静默——但仅在向 PD 查询目标的当前 epoch 之后。如果 `target.epoch.version > prepareTimeBaseline`，则合并已提交，禁止回滚（Inv-5）。

合并过程是 epoch 幂等的：重放相同提议是安全的，因为 epoch 检查防止重复应用。

### 快照追赶

当 Follower 落后太多时，Leader 发送 Raft 快照：

1. `SnapshotEngineImpl.buildAndStream()` 钉住 RocksDB 快照，遍历三个数据 CF，发射带 CRC32C 校验的数据块。
2. `GrpcRaftTransport.sendViaSnapshotStream()` 通过 `sendSnapshot` gRPC 流式 RPC 投递数据块。
3. `SnapshotEngineImpl.receiveAndInstall()` 对每个数据块验证 CRC32C，然后通过 3-CF 原子 `WriteBatch`（删除旧范围 + 插入新数据）以 `sync=true` 写入。

---

## 7. CDC（变更数据捕获）

### CdcEventBus

Region 级发布/订阅：

```java
ConcurrentHashMap<Long, CopyOnWriteArrayList<Consumer<CdcEvent>>>
```

- `subscribe(regionId, consumer)` — 注册 Region 级事件消费者。
- `publish(event)` — 向该事件所属 Region 的所有订阅者扇出。订阅者异常被捕获并记录日志。

### MvccApplyHandler 集成

CDC 事件在 Raft apply 循环中 commit/rollback 操作应用后发布。事件携带 `regionId`、`type`（PUT、DELETE、ROLLBACK）、`key`、`value`、`oldValue`、`startTs`、`commitTs`。

### ChangeDataServiceImpl

实现两个双向流 RPC 的 gRPC 服务：

- **EventFeed**：客户端发送 `ChangeDataRequest`（按 Region 注册/注销）。服务端订阅 `CdcEventBus` 并流式推送 `ChangeDataEvent` 消息。一个 `sendLock` 对象序列化出站写入。
- **ResolvedTs**：向已注册 Region 周期性推送当前 resolved timestamp（通过 `ScheduledExecutorService` 每 1 秒一次）。

### 当前局限

- At-most-once 投递：流错误时事件可能丢失。
- 无序列号用于间隙检测。
- 无持久化事件日志：新订阅者只能看到订阅时刻之后的事件。

---

## 8. 运维特性

### Prometheus 指标

`XKvMetrics` 管理单例 `PrometheusMeterRegistry`。所有组件通过它注册计数器和直方图：

- `xkv_errors_total`，带 `component` 和 `operation` 标签。
- gRPC 拦截器（`GrpcServerMetricsInterceptor`、`GrpcClientMetricsInterceptor`）跟踪请求时长、计数和活跃请求数。
- `MetricsHttpServer` 暴露 `/metrics` 端点供 Prometheus 采集。

### 健康检查 / 就绪检查

- `/healthz` — 存活探针：进程正在运行且响应。
- `/readyz` — 就绪探针：Store 已向 PD 注册且至少有一个 Region Peer 处于活跃状态。
- `/metrics` — Prometheus 采集端点。

### 结构化日志

`logstash-logback-encoder` 输出 JSON 格式。`MdcContextUtil` + `MdcServerInterceptor` 将 MDC 字段（`store_id`、`region_id`、`rpc_method`）注入每个 gRPC handler 的日志上下文。

### TLS / mTLS

`SslContextFactory` 为三个平面构建 Netty `SslContext`：

- **客户端平面**：SDK 客户端与 KV Server 之间（`KvConfig.clientTls`）。
- **Raft 平面**：KV 节点间 Raft 消息传输（`KvConfig.raftTls`）。
- **指标平面**：指标 HTTP 端点的可选 TLS。

`GrpcChannelFactory` 封装 Channel 构建，附带可选的 `TlsConfig` TLS 配置。

### 认证

`AuthServerInterceptor` 验证 `x-auth-token` 元数据头中的共享密钥令牌。`AuthClientInterceptor` 在每个出站 RPC 上附带令牌。

### 优雅下线

`DrainingInterceptor` 在调用 `startDraining()` 后以 `UNAVAILABLE` 状态（gRPC code）拒绝新请求。收到此状态的客户端重试到其他 Store。drain 标志是一个 `AtomicBoolean`，翻转一次后不可重置。

下线超时通过 `KvConfig.drainTimeoutMs` 配置（默认 10s）。

### 限流

`ConcurrencyLimitInterceptor` 使用 `Semaphore` 控制最大并发许可数（默认 10,000，通过 `KvConfig.maxConcurrentRequests` 配置）。无法获取许可的请求以 `RESOURCE_EXHAUSTED` 状态被拒绝。许可在 `onCancel`、`onComplete` 或 `onHalfClose` 异常时释放。
