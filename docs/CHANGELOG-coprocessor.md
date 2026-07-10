# Coprocessor Module Changelog

本文档记录 Coprocessor 模块从审计到生产就绪的全部变更。

---

## Phase 1: 资源安全修复（6 项关键 Bug）

### AnalyzeCoprocessor 加固
- **文件**: `kv/.../coprocessor/AnalyzeCoprocessor.java`
- 新增 `MAX_DISTINCT = 100,000` 上限，防止高基数列 OOM
- 限制采样大小：`Math.min(analyzeReq.getSampleSize(), DEFAULT_SAMPLE_SIZE)`
- 新增内联 deadline 检查（每批次检查超时）
- 新增锁冲突捕获与透传
- 新增 `exec_details_ms` 到 response

### VecIndexLookupOp 有界循环
- **文件**: `kv/.../dag/VecIndexLookupOp.java`
- 新增 `MAX_EMPTY_ROUNDS = 10,000`
- 将 `while (true)` 改为有界 for 循环

### GroupConcatAgg 内存上限
- **文件**: `kv/.../eval/CopAggFunction.java`
- 新增 `MAX_CONCAT_LEN = 1,048,576` (1MB)
- `update()` 和 `merge()` 中超限时跳过追加

### copExecutor 优雅关停
- **文件**: `kv/.../server/TikvServiceImpl.java`
- 新增 `shutdown()` 方法调用 `copExecutor.shutdownNow()`
- **文件**: `kv/.../server/KvServer.java`
- `stop()` 中调用 `tikvService.shutdown()`

### DeadlineExceeded 指标接入
- **文件**: `kv/.../server/CoprocessorService.java`
- `handler.handle()` 包裹 try-catch 捕获 `DeadlineExceededException`
- 捕获后递增 `deadlineExceededCount` 并返回 error response

---

## Phase 2: 生产完善（5 项增强）

### 1. Collation 支持
- **文件**: `proto/.../tipb.proto`
  - DAGRequest 新增 `int32 collation = 15`
- **文件**: `kv/.../eval/CopDatumComparator.java`
  - 新增 ThreadLocal `COLLATION` (0=binary, 1=utf8_general_ci, 2=utf8_unicode_ci)
  - `setCollation()` / `getCollation()` / `clearCollation()`
  - `compare()` 字符串比较改用 `compareStrings()` 分派
- **文件**: `kv/.../coprocessor/SQLScanCoprocessor.java`
  - `handle()` / `handleStream()` 设置并清除 collation
- **文件**: `kv/.../coprocessor/IndexScanCoprocessor.java`
  - 同上 collation 设置/清除

### 2. 扩展标量函数覆盖（~40 个新增）
- **文件**: `kv/.../eval/ExprEvaluator.java`
  - **字符串**: CONCAT_WS, LTRIM, RTRIM, REVERSE, SPACE, REPEAT, SUBSTR/SUBSTRING/MID, LEFT, RIGHT, REPLACE, LPAD, RPAD, LOCATE/INSTR/POSITION, HEX, UNHEX
  - **数学**: TRUNCATE, SQRT, POW/POWER, LOG, LOG2, LOG10, EXP, SIGN, PI, RAND, CRC32, GREATEST, LEAST
  - **日期**: CURDATE, CURTIME, YEAR, MONTH, DAY, HOUR, MINUTE, SECOND, DAYOFWEEK, DAYOFYEAR, DATEDIFF, DATE_ADD, DATE_SUB, UNIX_TIMESTAMP, FROM_UNIXTIME, DATE_FORMAT
  - **信息**: DATABASE, USER, CONNECTION_ID
  - 修复 lambda 中 `yield` → `return`（5 处）

### 3. 每请求内存配额 (CopMemTracker)
- **新文件**: `kv/.../dag/CopMemTracker.java`
  - `AtomicLong` 计数器，默认 256MB 配额
  - `track(bytes)` 超限抛出 `MemoryQuotaExceededException`
- **文件**: `kv/.../dag/VecTopNOp.java`
  - 新增带 `CopMemTracker` 的构造器
  - 每个新增 heap entry 计 256 bytes
- **文件**: `kv/.../coprocessor/SQLScanCoprocessor.java`
  - `handle()` / `handleStream()` 创建 `CopMemTracker` 并传递给算子
  - `executeAgg()` 按 group 追踪内存

### 4. 请求优先级队列
- **文件**: `kv/.../server/TikvServiceImpl.java`
  - `SynchronousQueue` → `PriorityBlockingQueue<>(64)`
  - 新增 `PrioritizedTask` 内部类（实现 `Comparable + Runnable`）
  - 排序：High(2) > Normal(0) > Low(1)，seqNo FIFO 打破平局
  - 新增 `extractPriority(Request)` 从 Context.priority 读取
  - 新增 `prioritized(int, Runnable)` 包装辅助方法
  - 三个 coprocessor dispatch 方法均使用 `prioritized()` 包装

### 5. Coprocessor 慢日志
- **文件**: `kv/.../server/CoprocessorService.java`
  - 新增 `COP_SLOW_LOG_THRESHOLD_NS = 100_000_000L` (100ms)
  - 新增 `logSlowCop(Request, elapsedNs, deadlineExceeded)` 方法
  - `handle()` 缓存/非缓存路径均接入慢日志
  - `handleStream()` 接入慢日志
  - 日志格式：`[COP-SLOW] tp={} duration_ms={} ranges={} cache_enabled={} deadline_exceeded={}`

---

## Phase 3: 上线前修复（3 项）

### 1. ThreadLocal 防泄漏
- **文件**: `kv/.../server/TikvServiceImpl.java`
  - `PrioritizedTask.run()` 新增 finally 块调用 `CopDatumComparator.clearCollation()`
  - 防止线程池复用时 collation 状态泄漏到下一个请求

### 2. 队列有界化
- **文件**: `kv/.../server/TikvServiceImpl.java`
  - 新增 `COP_MAX_PENDING = 1024`
  - 新增 `Semaphore copPendingPermits`
  - `coprocessor()` / `coprocessorStream()`：满载时返回 `ServerIsBusy` regionError
  - `batchCoprocessor()`：满载时返回 `RESOURCE_EXHAUSTED` gRPC status
  - 执行完成后 finally 释放 permit

### 3. 流式背压上限
- **文件**: `kv/.../coprocessor/SQLScanCoprocessor.java`
  - 新增 `MAX_STREAM_CHUNKS = 4096`
  - `streamKvPairChunks()` 超限截断并 warn
- **文件**: `kv/.../coprocessor/IndexScanCoprocessor.java`
  - 同上 `MAX_STREAM_CHUNKS = 4096` + 截断逻辑

---

## 新增文件总览

| 文件 | 说明 |
|------|------|
| `CopMemTracker.java` | 每请求内存配额追踪器 |
| `VecDeadlineOp.java` | 超时检测包装算子 |
| `VecProjectionOp.java` | 列投影算子 |
| `ChecksumCoprocessor.java` | 数据校验和 (tp=5) |
| `ChecksumCoprocessorTest.java` | 校验和单测 |
| `ExprEvaluatorTest.java` | 表达式求值全量单测 |

---

## 测试验证

- 296 单元测试全部通过（0 failures, 0 errors）
- 6 Coprocessor E2E 测试全部通过
- 变更前后无回归
