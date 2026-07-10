# Coprocessor 模块设计与使用文档

x-kv 的 Coprocessor 模块实现了 TiKV 兼容的下推计算框架，允许上层 SQL 引擎（如 TiDB）将过滤、聚合、排序等算子下推到存储层执行，减少网络传输和上层计算开销。

---

## 1. 架构总览

```
┌─────────────────────────────────────────────────────────────┐
│                     gRPC Layer (TikvServiceImpl)             │
│  ┌───────────────────────────────────────────────────────┐  │
│  │  PriorityBlockingQueue + Semaphore(1024)              │  │
│  │  PrioritizedTask: High(2) > Normal(0) > Low(1)       │  │
│  └───────────────────────────────────────────────────────┘  │
├─────────────────────────────────────────────────────────────┤
│                   CoprocessorService                         │
│  ┌──────────┐  ┌───────────┐  ┌──────────┐  ┌──────────┐  │
│  │  Cache   │  │  Metrics  │  │ Slow Log │  │ Deadline │  │
│  └──────────┘  └───────────┘  └──────────┘  └──────────┘  │
├─────────────────────────────────────────────────────────────┤
│              Coprocessor Handler Registry                    │
│  ┌────────┐ ┌────────┐ ┌────────┐ ┌────────┐ ┌─────────┐  │
│  │ tp=0   │ │ tp=1   │ │ tp=2   │ │ tp=3   │ │ tp=4/5  │  │
│  │TableScan│ │SQLScan │ │Analyze │ │IdxScan │ │Split/Chk│  │
│  └────────┘ └────────┘ └────────┘ └────────┘ └─────────┘  │
├─────────────────────────────────────────────────────────────┤
│            Vectorized Execution Pipeline (DAG)              │
│  VecTableScanOp → VecSelectionOp → VecProjectionOp         │
│       → VecTopNOp / VecLimitOp → VecDeadlineOp             │
├─────────────────────────────────────────────────────────────┤
│              Expression Evaluation (eval)                    │
│  ExprEvaluator · CopDatumComparator · CopAggFunction        │
├─────────────────────────────────────────────────────────────┤
│                  MVCC Layer (MvccReader)                     │
│         snapshot-isolated scan with lock detection           │
└─────────────────────────────────────────────────────────────┘
```

---

## 2. 请求类型

| tp | 名称 | 说明 |
|----|------|------|
| 0 | TableScan | 简单 KV 范围扫描，返回原始 KV 对 |
| 1 | SQLScan (DAG) | 完整 DAG 执行：过滤、投影、聚合、排序 |
| 2 | Analyze | 统计信息收集（采样 + NDV） |
| 3 | IndexScan | 索引扫描（覆盖索引 / 索引回表） |
| 4 | SplitKeys | 批量分裂键计算 |
| 5 | Checksum | 数据校验和计算 |

---

## 3. 向量化执行管线

### 3.1 算子接口

```java
public interface VecOperator {
    void open();
    CopChunk nextChunk(int batchSize);
    void close();
}
```

所有算子按 pull-based 模型工作：上层调用 `nextChunk(batchSize)` 拉取一批记录（`CopChunk`），底层按需从存储中读取。

### 3.2 算子清单

| 算子 | 职责 |
|------|------|
| `VecTableScanOp` | MVCC 范围扫描，支持多 range，透传锁冲突 |
| `VecIndexScanOp` | 索引范围扫描 |
| `VecIndexLookupOp` | 索引回表（通过 handle 做点查） |
| `VecSelectionOp` | WHERE 条件过滤 |
| `VecProjectionOp` | SELECT 列投影 |
| `VecTopNOp` | ORDER BY + LIMIT（bounded heap） |
| `VecLimitOp` | 简单 LIMIT 截断 |
| `VecDeadlineOp` | 超时检测包装器 |

### 3.3 管线组装

`SQLScanCoprocessor.buildVecPipeline()` 根据 DAGRequest 自动组装：

```
TableScan → [Selection if conditions] → [Projection if projections]
         → [TopN if topn_limit] or [Limit]
         → [Deadline if max_execution_duration_ms > 0]
```

---

## 4. 表达式求值

### 4.1 数据类型

`CopDatum` 是所有值的统一包装，支持以下类型：

- `IntVal` — int64
- `RealVal` — float64
- `StringVal` — UTF-8 字符串
- `BytesVal` — 原始字节
- `Nil` — SQL NULL

### 4.2 支持的标量函数（~55 个）

**字符串：** CONCAT, CONCAT_WS, LENGTH, CHAR_LENGTH, UPPER, LOWER, TRIM, LTRIM, RTRIM, REVERSE, SPACE, REPEAT, SUBSTR/SUBSTRING/MID, LEFT, RIGHT, REPLACE, LPAD, RPAD, LOCATE/INSTR/POSITION, HEX, UNHEX

**数学：** ABS, CEIL, FLOOR, ROUND, TRUNCATE, MOD, SQRT, POW/POWER, LOG, LOG2, LOG10, EXP, SIGN, PI, RAND, CRC32, GREATEST, LEAST

**日期/时间：** CURDATE, CURTIME, YEAR, MONTH, DAY, HOUR, MINUTE, SECOND, DAYOFWEEK, DAYOFYEAR, DATEDIFF, DATE_ADD, DATE_SUB, UNIX_TIMESTAMP, FROM_UNIXTIME, DATE_FORMAT

**类型转换：** CAST

**逻辑/比较：** AND, OR, NOT, =, !=, <, >, <=, >=, IN, BETWEEN, LIKE, IS NULL, IS NOT NULL, CASE WHEN, IF, IFNULL, NULLIF, COALESCE

### 4.3 Collation 支持

通过 `CopDatumComparator` 的 ThreadLocal 机制实现：

| collation 值 | 行为 |
|-------------|------|
| 0 (binary) | `String.compareTo()` 字节序 |
| 1 (utf8_general_ci) | `String.compareToIgnoreCase()` |
| 2 (utf8_unicode_ci) | `String.compareToIgnoreCase()` |

DAGRequest 中通过 `int32 collation = 15` 字段传递，SQLScanCoprocessor 在请求开始时设置、结束时清除。

### 4.4 聚合函数

支持的聚合类型：COUNT, SUM, AVG, MIN, MAX, GROUP_CONCAT

- 支持 DISTINCT 语义
- 支持 GROUP BY 分组（上限 100,000 组）
- 返回 partial state 供上层合并

---

## 5. 资源安全与限制

### 5.1 内存配额 (CopMemTracker)

每个请求创建独立的 `CopMemTracker`（默认 256MB 配额），所有算子共享：

- VecTopNOp：每个 heap entry 计 256 bytes
- HashAgg：每个新 group 计 256 bytes
- 超限抛出 `MemoryQuotaExceededException`

### 5.2 各算子上限

| 组件 | 上限 | 说明 |
|------|------|------|
| VecTopNOp heap | 65,536 entries | 防止无界排序 |
| Agg groups | 100,000 groups | 防止高基数 GROUP BY |
| GroupConcat | 1 MB | 防止单值无限拼接 |
| Analyze NDV | 100,000 distinct values | 采样上限 |
| VecIndexLookupOp | 10,000 empty rounds | 防止无结果死循环 |
| Stream chunks | 4,096 chunks/请求 | 防止无界流式输出 |

### 5.3 Deadline 强制执行

`VecDeadlineOp` 包装最外层管线，每次 `nextChunk()` 检查已用时间，超时抛出 `DeadlineExceededException`。

---

## 6. 调度与背压

### 6.1 请求优先级

Coprocessor 线程池使用 `PriorityBlockingQueue`，任务按 `CommandPri` 排序：

```
High(2) > Normal(0) > Low(1)
```

同优先级内按 FIFO（递增 seqNo）排序。

### 6.2 背压

- **队列上限**：`Semaphore(1024)` 控制最大 pending 任务数
- **满载响应**：返回 `ServerIsBusy` 错误（TiDB 客户端会自动 backoff 重试）
- **CallerRunsPolicy**：线程池拒绝策略，在提交线程执行（兜底）

### 6.3 线程池配置

```
core = 4, max = 16, keepAlive = 60s
```

---

## 7. 可观测性

### 7.1 慢日志

当单次 Coprocessor 请求耗时超过 100ms 时，输出结构化慢日志：

```
[COP-SLOW] tp=1 duration_ms=523 ranges=3 cache_enabled=false deadline_exceeded=false
```

### 7.2 指标计数器

| 指标 | 说明 |
|------|------|
| `requestCount` | 总请求数 |
| `requestErrorCount` | 错误请求数 |
| `cacheHitCount` | 缓存命中数 |
| `cacheMissCount` | 缓存未命中数 |
| `deadlineExceededCount` | 超时次数 |
| `totalExecNanos` | 累计执行时间 |

### 7.3 结果缓存

`CoprocessorService` 内置 Caffeine 缓存：
- 最大 256 条目
- 30 秒 TTL
- 基于 (tp, data, startTs, ranges) 的精确键
- 通过 `invalidateCache()` 在数据变更时失效

---

## 8. 流式响应

`coprocessorStream` RPC 以流式返回数据，每 256 条记录一个 chunk：

- 最大 4096 chunks/请求（约 100 万行）
- 超限自动截断并 warn
- 锁冲突信息在最后一个 chunk 之后发送

---

## 9. 锁冲突处理

MVCC 扫描遇到未提交事务的锁时：
- 不中断扫描（跳过冲突 key）
- 将第一个遇到的锁信息记录到 `VecTableScanOp.lockError()`
- 在 response 中通过 `locked` 字段返回给客户端
- 客户端可据此执行 lock resolve（CheckTxnStatus → ResolveLock）

---

## 10. 批量请求 (BatchCoprocessor)

`batchCoprocessor` RPC 允许单次请求跨多个 Region 发送：
- 逐 Region 执行，独立返回 response
- 支持 Region epoch 校验
- 支持 range 裁剪（跨 Region 边界自动截断）

---

## 11. 与 TiDB 的兼容性

### 协议兼容

- Protobuf 定义兼容 TiKV 5.x+ 的 `tipb.DAGRequest` 格式
- 支持 `CommandPri` 优先级字段
- 支持 `max_execution_duration_ms` 超时语义
- 支持 `is_cache_enabled` / `cache_if_match_version` 缓存协议

### 已知差异

- Collation 实现简化为 binary / case-insensitive 两档（未实现 ICU 级 unicode collation）
- 部分罕见标量函数未实现（未推送的函数 TiDB 会在自身层执行）
- 统计信息格式兼容但精度可能略有差异

---

## 12. 优雅关停

`KvServer.stop()` 调用链：
1. gRPC server shutdown（停止接受新请求）
2. `TikvServiceImpl.shutdown()` → `copExecutor.shutdownNow()`（中断正在执行的任务）
3. ThreadLocal 清理（PrioritizedTask finally 块）

---

## 13. 配置参数

| 参数 | 默认值 | 说明 |
|------|--------|------|
| copExecutor core | 4 | 核心线程数 |
| copExecutor max | 16 | 最大线程数 |
| copExecutor pending limit | 1024 | 最大排队任务数 |
| memory quota | 256 MB | 每请求内存上限 |
| scan batch size | 1024 | 内部批次大小 |
| stream chunk size | 256 | 流式响应每批记录数 |
| max stream chunks | 4096 | 流式响应最大批次数 |
| cache max entries | 256 | 缓存条目上限 |
| cache TTL | 30s | 缓存过期时间 |
| slow log threshold | 100ms | 慢日志阈值 |
