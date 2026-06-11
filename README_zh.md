# x-kv

[English](./README.md) | [中文](./README_zh.md)

分布式事务 KV 存储。基于 Java 17、RocksDB、gRPC、Multi-Raft
（通过 [`x-raft-lib`](https://github.com/x-infra-lab/x-raft-lib)）构建，
目标是在协议层面达到 TiKV v8.x 兼容。

> **当前状态：v0.2.0-SNAPSHOT** — 完整的 Multi-Raft 集群，支持 MVCC + Percolator
> 事务、PD 驱动路由、客户端 SDK、Region 自动分裂/合并、
> Jepsen 风格线性一致性测试及性能基准。

---

## 核心特性

- **Multi-Raft 共识** — 每个 Region 是独立的 Raft 组；支持 Leader 选举、日志复制、
  快照传输（通过 gRPC）
- **MVCC + Percolator 事务** — 乐观 2PC、悲观锁、异步提交（Async Commit）、
  1PC 短路优化、分布式死锁检测
- **Placement Driver (PD)** — 3 节点 HA Raft 集群，负责集群元数据管理、
  单调递增 TSO、Region/Leader/Split 调度、GC 安全点管理
- **客户端 SDK** — PD 感知路由、Region 缓存（Epoch 失效机制）、
  TSO 批量器（合并 N 个调用者到一次 RPC）、锁解析器、
  事务冲突自动重试（指数退避）
- **Region 自动分裂** — PD 驱动，基于心跳上报的 Region 近似大小触发分裂调度
- **Region 合并** — 双向确认、Epoch 幂等的合并协议
- **Follower Read** — 从 Follower 提供过期读，降低 Leader 负载
- **Raw KV API** — get、put、delete、scan、batchGet、batchPut、batchDelete、
  deleteRange、compare-and-swap、TTL
- **CDC（变更数据捕获）** — gRPC 双向流 EventFeed，Region 级事件总线，
  Resolved Timestamp 推送
- **运维工具** — CLI（`xkv-ctl`）、Prometheus 指标、结构化日志
  （logstash-logback-encoder）、健康检查、限流、TLS/mTLS、Token 认证、优雅下线
- **测试套件** — 309 个测试方法：E2E、线性一致性（Wing-Gong + Chaos Monkey）、
  混沌测试（Leader Kill / Follower Kill / 网络分区）、压力测试、
  银行转账 SI 不变量、崩溃恢复、Leader 故障转移、快照追赶、性能基准

---

## 模块结构

| 模块 | Artifact | 职责 |
|------|----------|------|
| `proto/`   | `x-proto`     | gRPC + Protobuf 协议定义（10 个 `.proto` 文件；TiKV v8.x 兼容） |
| `common/`  | `x-common`    | 共享基础设施 — TLS、认证、指标、配置、限流 |
| `pd/`      | `x-pd`        | Placement Driver — 集群元数据、TSO、调度器 |
| `kv/`      | `x-kv-store`  | Multi-Raft KV 存储，MVCC + Percolator 事务引擎 |
| `client/`  | `x-client`    | PD 感知 Java SDK（Raw KV + 事务） |
| `ctl/`     | `x-kv-ctl`    | 命令行工具，支持集群、Store、Region、GC 操作 |
| `tests/`   | `x-tests`     | E2E / 混沌 / 线性一致性 / 性能基准测试套件 |

---

## 快速开始

### Docker Compose（3 PD + 3 KV）

```bash
# 构建并启动集群
cd docker
docker compose up -d

# 检查服务健康状态
docker compose ps

# 使用 CLI 查看集群信息
docker compose exec kv1 java -jar /opt/x-kv/x-kv-ctl.jar \
    --pd pd1:2379 cluster members
```

PD 端点暴露在 `localhost:2379`、`localhost:2381`、`localhost:2383`。
KV Store 监听 `localhost:20160`、`localhost:20161`、`localhost:20162`。

### Java SDK

添加依赖（发布后）：

```xml
<dependency>
    <groupId>io.github.x-infra-lab</groupId>
    <artifactId>x-client</artifactId>
    <version>0.2.0-SNAPSHOT</version>
</dependency>
```

**Raw KV：**

```java
var config = ClientConfig.builder()
        .pdEndpoints(List.of("127.0.0.1:2379"))
        .build();

try (var client = XKvClient.create(config)) {
    var raw = client.raw();

    raw.put("hello".getBytes(), "world".getBytes());

    Optional<byte[]> val = raw.get("hello".getBytes());
    // val.get() == "world"

    raw.delete("hello".getBytes());
}
```

**事务：**

```java
var config = ClientConfig.builder()
        .pdEndpoints(List.of("127.0.0.1:2379"))
        .build();

try (var txnClient = TxnClient.create(config)) {
    // 写冲突时自动重试，指数退避
    txnClient.executeWithRetry(txn -> {
        byte[] balanceA = txn.get("account-A".getBytes())
                .orElse(new byte[]{0});
        byte[] balanceB = txn.get("account-B".getBytes())
                .orElse(new byte[]{0});

        txn.put("account-A".getBytes(), subtract(balanceA, 100));
        txn.put("account-B".getBytes(), add(balanceB, 100));
        return null;
    });
}
```

### CLI

```bash
# 构建 CLI
mvn package -pl ctl -am -DskipTests -q

# 集群操作
java -jar ctl/target/x-kv-ctl.jar --pd 127.0.0.1:2379 cluster members
java -jar ctl/target/x-kv-ctl.jar --pd 127.0.0.1:2379 cluster health

# Store 操作
java -jar ctl/target/x-kv-ctl.jar --pd 127.0.0.1:2379 store list

# Region 操作
java -jar ctl/target/x-kv-ctl.jar --pd 127.0.0.1:2379 region list --limit 10

# GC 安全点
java -jar ctl/target/x-kv-ctl.jar --pd 127.0.0.1:2379 gc safepoint
```

---

## 构建与测试

**前置条件：** JDK 17+、Maven 3.8+

```bash
# 编译所有模块
mvn install -DskipTests

# 运行完整测试套件（309 个测试方法，约 9 分钟）
mvn test

# 运行指定模块
mvn test -pl tests

# 运行单个测试类
mvn test -pl tests -Dtest=LinearizabilityE2ETest
```

---

## 文档

| 文档 | 说明 |
|------|------|
| [架构](./docs/architecture_zh.md)（[English](./docs/architecture.md)） | 系统概览、模块依赖、核心不变量、部署架构 |
| [设计](./docs/design_zh.md)（[English](./docs/design.md)） | 存储引擎、Raft 集成、MVCC/Percolator、PD、客户端 SDK、CDC |
| [测试](./docs/testing_zh.md)（[English](./docs/testing.md)） | 测试策略、分类、线性一致性/混沌测试、基准测试、CI |
| [变更日志](./CHANGELOG.md) | 所有重要变更（Keep a Changelog 格式） |
| [贡献指南](./CONTRIBUTING.md) | 构建说明、代码规范、PR 工作流 |

---

## 路线图

| 阶段 | 主题 | 状态 |
|------|------|------|
| **0** | Proto + 模块骨架                            | 完成 |
| **1** | Raft 持久化契约 + Raw KV                     | 完成 |
| **2** | MVCC + Percolator                           | 完成 |
| **3** | PD + TSO（Leader 切换后严格单调）             | 完成 |
| **4** | Multi-Raft + Region 分裂/合并                | 完成 |
| **5** | 客户端 SDK（Backoffer / TsoBatcher / 2PC）   | 完成 |
| **6** | 测试基础设施（线性一致性 / 基准 / 混沌）       | 完成 |
| **7** | 高级特性                                     | 进行中 |

### 后续工作（Phase 7+）

- 备份/恢复（BR 风格 SST 导出 + Service 安全点）
- 完整调度器集：热点 Region、规则检查、合并检查
- `OperatorController` + Store 级别令牌桶限流
- JMH 基准测试套件
- 24 小时持续压测框架

---

## 贡献

请参阅 [CONTRIBUTING.md](./CONTRIBUTING.md) 了解构建说明、代码规范和 PR 工作流。

---

## 许可证

[Apache License 2.0](./LICENSE)
