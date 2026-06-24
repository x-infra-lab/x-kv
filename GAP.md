# x-kv vs TiKV v8.x Architecture Gap Analysis

## Overview

x-kv targets wire-protocol parity with TiKV v8.x. Core correctness invariants
(per-entry fsync, unified-CF design, two-pass prewrite, async-commit gate) are
correctly implemented. The main gaps are in performance architecture (threading
model, leader lease, in-memory lock) and advanced features (backup, multi-tenancy).

---

## 1. MVCC / Percolator

| Capability | TiKV | x-kv | Status |
|-----------|------|------|--------|
| Three CF (default/lock/write) | ✅ | ✅ | ✅ |
| Prewrite (optimistic 2PC, two-pass all-or-nothing) | ✅ | ✅ | ✅ |
| Pessimistic lock acquire/release (forUpdateTs) | ✅ | ✅ | ✅ |
| Pessimistic prewrite upgrade | ✅ | ✅ | ✅ |
| Commit (short-value inline) | ✅ | ✅ | ✅ |
| Rollback (idempotent + ROLLBACK record) | ✅ | ✅ | ✅ |
| CheckTxnStatus (TTL + commit race fix) | ✅ | ✅ | ✅ |
| CheckSecondaryLocks (async-commit recovery) | ✅ | ✅ | ✅ |
| TxnHeartBeat | ✅ | ✅ | ✅ |
| ResolveLock (commit/rollback + pessimistic fallback) | ✅ | ✅ | ✅ |
| Async Commit (min_commit_ts + secondaries list) | ✅ | ✅ | ✅ |
| 1PC short-circuit | ✅ | ✅ | ✅ |
| GC (PD safe-point + per-region propose) | ✅ | ✅ | ✅ |
| Write conflict detection via seek | ✅ | ✅ | ✅ |
| Overlapping rollback collapse | ✅ | ✅ | ✅ |
| Pipelined pessimistic lock | ✅ | ✅ | ✅ |
| In-memory lock (write intent) | ✅ (v7+) | ✅ | ✅ |

---

## 2. Raft / Store

| Capability | TiKV | x-kv | Status |
|-----------|------|------|--------|
| Multi-Raft (per-region Raft group) | ✅ | ✅ | ✅ |
| Per-entry apply + final flushWal | ✅ | ✅ | ✅ |
| ReadIndex (linearizable read) | ✅ | ✅ | ✅ |
| Pre-Vote + Check Quorum | ✅ | ✅ | ✅ |
| Conf Change (V2, AddNode/Remove/Learner) | ✅ | ✅ | ✅ |
| Snapshot generation / application | ✅ | ✅ | ✅ |
| Region Split | ✅ | ✅ | ✅ |
| Region Merge (3-phase with PD verify) | ✅ | ✅ | ✅ |
| Merge quiescence (freeze writes) | ✅ | ✅ | ✅ |
| Unified-CF atomic (applied_index + data in one batch) | ✅ | ✅ | ✅ |
| Log compaction | ✅ | ✅ | ✅ |
| Leader Lease (local read without RTT) | ✅ | ✅ | ✅ |
| BatchSystem (fixed poll threads + mailbox) | ✅ | ✅ | ✅ |
| Async Apply | ✅ | ✅ | ✅ |

---

## 3. PD (Placement Driver)

| Capability | TiKV PD | x-kv PD | Status |
|-----------|---------|---------|--------|
| Raft-based HA (3-node) | ✅ | ✅ | ✅ |
| TSO (HLC, persistent bound, leader-change +1) | ✅ | ✅ | ✅ |
| TSO single-flight extend | ✅ | ✅ | ✅ |
| Region heartbeat + epoch management | ✅ | ✅ | ✅ |
| Store heartbeat + state management | ✅ | ✅ | ✅ |
| Split scheduler (size-based) | ✅ | ✅ | ✅ |
| Merge scheduler | ✅ | ✅ | ✅ |
| Leader balance | ✅ | ✅ | ✅ |
| Region balance | ✅ | ✅ | ✅ |
| Hot region scheduler | ✅ | ✅ | ✅ |
| Rule checker (placement rules) | ✅ | ✅ (simplified) | ⚠️ |
| OperatorController (concurrency limit + timeout) | ✅ | ✅ | ✅ |
| GC safe-point service | ✅ | ✅ | ✅ |
| Deadlock detector (centralized, BFS + TTL) | ✅ | ✅ | ✅ |
| Full placement rules (label constraint engine) | ✅ | ⚠️ Simplified | ⚠️ |
| Dashboard / hot-reload scheduling policy | ✅ | ❌ | ❌ |
| Keyspace / Resource Group | ✅ (v7+) | ❌ | ❌ |

---

## 4. Client SDK

| Capability | TiKV client-java | x-kv client | Status |
|-----------|-----------------|-------------|--------|
| PD-aware routing | ✅ | ✅ | ✅ |
| Region cache + epoch invalidation | ✅ | ✅ | ✅ |
| TSO Batcher (N callers coalesce) | ✅ | ✅ | ✅ |
| Backoffer (exponential + jitter) | ✅ | ✅ | ✅ |
| Lock Resolver (LRU cache + single-flight) | ✅ | ✅ | ✅ |
| 2PC Committer (three-state: COMMITTED/ROLLED_BACK/UNKNOWN) | ✅ | ✅ | ✅ |
| Async commit client | ✅ | ✅ | ✅ |
| 1PC client | ✅ | ✅ | ✅ |
| Raw KV API (get/put/delete/scan/batch/CAS/TTL/deleteRange) | ✅ | ✅ | ✅ |
| Txn executeWithRetry | ✅ | ✅ | ✅ |
| Coprocessor client + streaming | ✅ | ✅ | ✅ |
| Store connection pool (per-store multi-channel) | ✅ | ✅ | ✅ |
| BatchCommands client multiplexing | ✅ | ✅ | ✅ |
| Batch ResolveLock (Green GC) | ✅ | ✅ | ✅ |

---

## 5. Coprocessor

| Capability | TiKV | x-kv | Status |
|-----------|------|------|--------|
| DAG executor (Volcano pull model) | ✅ | ✅ | ✅ |
| TableScan operator | ✅ | ✅ | ✅ |
| Selection (filter push-down) | ✅ | ✅ | ✅ |
| Limit | ✅ | ✅ | ✅ |
| TopN + OrderBy | ✅ | ✅ | ✅ |
| Aggregation (COUNT/SUM/AVG/MIN/MAX) | ✅ | ✅ | ✅ |
| Expression evaluator | ✅ | ✅ | ✅ |
| Streaming response (chunked) | ✅ | ✅ | ✅ |
| Analyze (statistics collection) | ✅ | ✅ | ✅ |
| Index scan / index lookup | ✅ | ❌ TableScan only | ❌ |
| Vectorized execution (chunk-based) | ✅ | ❌ Row-at-a-time | ⚠️ |
| Batch-split-region coprocessor | ✅ | ❌ | ❌ |

---

## 6. CDC (Change Data Capture)

| Capability | TiKV CDC | x-kv | Status |
|-----------|---------|------|--------|
| Region-scoped event bus | ✅ | ✅ | ✅ |
| gRPC bidi EventFeed | ✅ | ✅ | ✅ |
| Resolved TS push | ✅ | ⚠️ Proto defined, simplified impl | ⚠️ |
| Incremental scan (catch-up) | ✅ | ❌ | ❌ |
| Multi-region resolved TS aggregation | ✅ | ❌ | ❌ |

---

## 7. Proto / Wire Protocol

| Aspect | TiKV | x-kv | Status |
|--------|------|------|--------|
| Core RPC set (~30+ RPCs) | ✅ | ✅ Full coverage | ✅ |
| BatchCommands multiplexing (server) | ✅ | ✅ | ✅ |
| Error model (region_error + KeyError) | ✅ | ✅ Complete types | ✅ |
| Context (epoch/term/stale_read/resource_group) | ✅ | ✅ | ✅ |
| Compatibility version negotiation | ✅ | ❌ | ❌ |

---

## 8. Operations / Observability

| Aspect | TiKV | x-kv | Status |
|--------|------|------|--------|
| Prometheus metrics | ✅ | ✅ | ✅ |
| Structured logging | ✅ | ✅ | ✅ |
| CLI (tikv-ctl equivalent) | ✅ | ✅ xkv-ctl | ✅ |
| Docker Compose (3PD+3KV) | ✅ | ✅ | ✅ |
| TLS/mTLS | ✅ | ✅ | ✅ |
| Rate limiting | ✅ | ✅ | ✅ |
| Graceful drain | ✅ | ✅ | ✅ |
| Online config change | ✅ | ❌ | ❌ |
| Backup / Restore (BR) | ✅ | ❌ (roadmap) | ❌ |

---

## Priority Summary

### P0 — Production Blockers

| # | Gap | Impact | TiKV Solution | Status |
|---|-----|--------|---------------|--------|
| 1 | ~~Per-region thread model~~ | ~~Thread/memory explosion beyond ~200 regions~~ | ~~BatchSystem: fixed poll threads + mailbox~~ | ✅ |
| 2 | ~~No Leader Lease~~ | ~~Every read requires ReadIndex RTT~~ | ~~Lease-based local read while lease valid~~ | ✅ |
| 3 | ~~No in-memory lock~~ | ~~Every prewrite pays lock CF write + fsync~~ | ~~In-memory lock table + async persist~~ | ✅ |
| 4 | ~~No pipelined pessimistic lock~~ | ~~Large txn lock phase bottleneck~~ | ~~Return success, async Raft propose~~ | ✅ |

### P1 — Performance

| # | Gap | Impact | Status |
|---|-----|--------|--------|
| ~~5~~ | ~~Write conflict scan is linear (not seek-based)~~ | ~~Hot keys with many versions slow prewrite~~ | ✅ |
| ~~6~~ | ~~Client not using BatchCommands multiplexing~~ | ~~One gRPC call per RPC, no fsync amortization~~ | ✅ |
| 7 | No Index Scan in coprocessor | SQL workloads require full table scan | |
| 8 | Row-at-a-time coprocessor (not vectorized) | Low CPU utilization | |
| ~~9~~ | ~~Single channel per store~~ | ~~Concurrency limited~~ | ✅ |

### P2 — Features

| # | Gap |
|---|-----|
| 10 | Backup / Restore (BR-style SST export) |
| 11 | Keyspace / Resource Group (multi-tenancy) |
| 12 | Full placement rules (label constraints) |
| 13 | CDC incremental scan + resolved TS aggregation |
| 14 | Online config change |
| ~~15~~ | ~~Rollback record aggregation (prevent write CF bloat)~~ ✅ |

---

## Correctness Highlights (What x-kv Gets Right)

1. **Two-pass prewrite** — no partial orphan locks on conflict
2. **Unified-CF design** — applied_index + data in one WriteBatch, crash-safe
3. **CheckTxnStatus race fix** — won't force-rollback an already-committed txn
4. **Async-commit gate** — expired primary with useAsyncCommit is not rolled back
5. **TSO +1 on leader change** — prevents duplicate timestamps
6. **commit_ts >= min_commit_ts validation** — async-commit atomicity guarantee
7. **Merge 3-phase protocol + PD verification** — no unilateral rollback causing double-write
8. **ConcurrencyManager striped latch** — better throughput than coarse region lock
9. **LockResolver Caffeine LRU + single-flight** — bounded memory, no duplicate resolves
10. **CommitResult three-state** — UNKNOWN state prevents silent transaction loss

---

## Suggested Next Steps

1. ~~**Write conflict seek optimization** — reverse seek from `encode(key, MAX_TS)` to first non-rollback~~ ✅
2. ~~**Client BatchCommands** — single stream, multiple RPCs, amortized fsync~~ ✅
3. ~~**Async Apply** — decouple apply from the poller ready loop into a dedicated apply pool~~ ✅
