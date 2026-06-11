# x-kv 测试指南

## 测试策略概述

x-kv 共有 309 个测试方法，分布在 68 个测试文件中。

| 分类 | 所属模块 | 测试数量 |
|---|---|---|
| 单元测试 | common (15), client (6), kv (43), pd (30) | 101 |
| 集成 / 端到端测试 | tests | 208 |
| **合计** | | **309** |

测试比例有意向端到端倾斜。分布式事务 KV 存储的正确性主要取决于各组件之间的
交互——Raft 共识、MVCC、Percolator 两阶段提交、PD 调度——因此验证真实的
分布式行为比测试 mock 后的独立单元更有价值。

所有端到端测试均使用真实的 gRPC 传输、真实的 PD 和真实的 Raft，共识和网络
通信层没有任何 mock。

---

## 测试分类

### 单元测试

位于 `common`、`pd`、`kv`、`client` 模块。

| 领域 | 覆盖内容 | 测试数 |
|---|---|---|
| 存储引擎 | RocksDB 4-CF 操作、原子批量写入、崩溃恢复 | 6 |
| Raft 持久化 | 日志/状态往返、快照构建与应用 | 7 |
| MVCC | Key 编码、Lock/Write 序列化、Reader、事务累加器 | 13 |
| TSO | 单调性、Leader 切换安全性、single-flight 续约 | 8 |
| PD 状态机 | 引导、Region 路由、epoch 感知更新 | 6 |
| 安全点服务 | 全局 + 各服务安全点、TTL | 6 |
| 死锁检测 | 环检测、自环、TTL 清理 | 8 |

### 端到端测试

所有端到端测试通过 `ClusterHarness` 管理的 3 节点集群在进程内运行。

**Raw KV**（5 个测试）
Put/get/delete/scan/batch/deleteRange/崩溃恢复。

**Percolator**（31 个测试）
2PC、锁阻塞读、回滚、冲突、async-commit、悲观锁、TxnHeartBeat、GC。

**Multi-Raft**（3 个测试）
Leader 选举、复制、故障转移。

**Region 分裂**（5 个测试）
PD 驱动分裂、子 Peer 生成、自动分裂。

**Region 合并**（5 个测试）
先分裂后合并、协议安全性、静默状态。

**跨 Region 事务**（5 个测试）
跨分裂后 Region 的事务、写冲突。

**PD 路由**（5 个测试）
缓存未命中时拉取、TSO 单调性、Leader 路由。

**银行转账**（2 个测试）
串行和并发下的 SI 守恒。

**崩溃恢复**（3 个测试）
Follower 宕机、Leader 宕机、恢复期间的写入。

**线性一致性**（3 个测试）
Wing-Gong 检验器，分别在无故障、Follower 故障、Leader 故障场景下运行。

**混沌测试**（3 个测试）
Leader 宕机、Follower 宕机、网络分区下的余额守恒。

**基准测试**（4 个测试）
rawPut、rawGet、txnCommit、txnConflictRetry，使用 `LatencyHistogram`。

**压力测试**（2 个测试）
Raw KV（16 工作线程，每线程 2000 次操作）、事务（8 工作线程，银行转账）。

**CDC**（5 个测试）
提交事件、删除事件、回滚事件、取消注册、多键。

**运维相关**（若干）
TLS、认证、限流、指标、健康检查、Follower Read、优雅下线、调试服务、
Coprocessor。

---

## 线性一致性测试

线性一致性测试采用 Jepsen 风格的方法：

1. 多个并发的读写客户端在共享的 Key 空间上操作。
2. 每个操作记录纳秒级的调用/返回时间戳。
3. 使用 Wing-Gong 回溯算法对每个 Key 的操作历史进行线性一致性验证。
4. 失败或结果不确定的操作（网络错误、读到过期 Follower 数据）被标记为
   `INDETERMINATE`，以避免误判为违反线性一致性。

三种测试模式逐步提升故障强度：

- **无故障** —— 基准正确性验证。
- **Follower 故障** —— 后台线程以 2-3 秒为周期反复杀死并重启 Follower。
- **Leader 故障** —— 同样的模式，但目标是 Raft Leader。Leader 故障期间，
  来自过期 Follower 的空读被标记为 `INDETERMINATE`，因为 Raw 读取不经过
  Raft。

---

## 混沌测试

`ChaosTest` 使用银行转账负载：4 个工作线程、10 个账户、每个账户初始余额
1000。

测试三种场景：

| 场景 | 注入的故障 |
|---|---|
| Leader 宕机 | 关闭并重启 Raft Leader |
| Follower 宕机 | 关闭并重启一个 Follower |
| 网络分区 | 关闭一个节点模拟分区 |

混沌线程随机选择受害节点，将其杀死 1.5-5 秒后重启。混沌阶段结束后，预留
5 秒的恢复窗口让集群趋于稳定。

验证逻辑检查总余额守恒（SI 不变量）。检查最多重试 5 次，每次间隔 2 秒，
使用 `RetryConfig(30, 2, 1000)`。

关键设计：混沌线程退出前必须重启受害节点，防止节点永久下线导致后续断言失败。

---

## 基准测试套件

基准测试使用 `LatencyHistogram`，这是一个基于 `LongAdder` 的无锁直方图，
桶宽 1ms，共 101 个桶。

报告指标：p50、p95、p99、最大值、平均值、吞吐量（ops/sec）。

JSON 报告写入 `target/benchmark-results/`，供 CI 进行回归追踪。

| 基准项 | 线程 x 操作数 | 备注 |
|---|---|---|
| rawPut | 4 x 1000 | |
| rawGet | 4 x 1000 | |
| txnCommit | 4 x 500 | 完整 Percolator 2PC |
| txnConflictRetry | 4 x 50 次转账，10 个热点 Key | 同时验证余额守恒 |

---

## 测试基础设施

### ClusterHarness

`ClusterHarness` 约 677 行代码，是所有端到端测试的基础。它在单个 JVM 内
启动 1 个 PD 和 N 个 KV Store，使用真实的 gRPC 传输。

关键设计：

- **端口预留**：使用 ServerSocket 占位-释放模式，避免端口选定到绑定之间被
  其他进程抢占的 TOCTOU 竞争。
- **服务绑定**：3 次重试逻辑，处理瞬时绑定失败。
- **节点重启**：支持杀死并重启单个节点。重启时从持久化的 RocksDB 状态恢复
  Raft 日志。
- **按需生成 Peer**：可动态添加 Peer，用于 conf-change 和分裂测试。
- **有序关闭**：先停止 gRPC 服务器，再停止 Raft Peer，最后关闭存储引擎，
  整个过程限时 10 秒。
- **数据隔离**：每个测试使用独立的 `@TempDir`，测试数据不会在运行之间泄漏。

---

## 如何运行测试

```bash
# 完整测试（约 309 个测试，约 9 分钟）
mvn test

# 仅单元测试（较快，约 10 秒）
mvn test -pl common,pd,kv,client

# 仅端到端测试
mvn test -pl tests

# 运行单个测试类
mvn test -pl tests -Dtest=LinearizabilityE2ETest

# 运行单个测试方法
mvn test -pl tests -Dtest="ChaosTest#balanceConservedUnderLeaderKillChaos"

# 显示详细输出
mvn test -pl tests -Dtest=PercolatorE2ETest -Dsurefire.useFile=false
```

---

## CI 集成

CI 基于 GitHub Actions，使用 JDK 17，运行在 `ubuntu-latest` 上。流水线
阶段：

1. 编译
2. 测试
3. 打包
4. Docker 构建

产物：

| 产物 | 保留时间 |
|---|---|
| JaCoCo 覆盖率报告 | 14 天 |
| Surefire 报告（仅失败时上传） | 7 天 |
| 基准测试 JSON 结果 | 30 天 |
