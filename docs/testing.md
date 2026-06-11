# x-kv Testing Guide

## Test Strategy Overview

x-kv has 309 test methods spread across 68 test files.

| Category | Module(s) | Test count |
|---|---|---|
| Unit tests | common (15), client (6), kv (43), pd (30) | 101 |
| Integration / E2E tests | tests | 208 |
| **Total** | | **309** |

The ratio deliberately favors E2E tests. A distributed transactional KV store
derives most of its correctness from the interaction between components --
Raft consensus, MVCC, Percolator 2PC, PD scheduling -- so validating real
distributed behavior matters more than testing mocked units in isolation.

Every E2E test uses real gRPC transport, a real PD, and real Raft. There are
no mocks for consensus or network communication.

---

## Test Categories

### Unit Tests

These live in the `common`, `pd`, `kv`, and `client` modules.

| Area | What is covered | Tests |
|---|---|---|
| Storage engine | RocksDB 4-CF operations, atomic batches, crash recovery | 6 |
| Raft persistence | Log/state round-trip, snapshot build/apply | 7 |
| MVCC | Key encoding, lock/write serde, reader, txn accumulator | 13 |
| TSO | Monotonicity, leader-change safety, single-flight extend | 8 |
| PD state machine | Bootstrap, region routing, epoch-aware update | 6 |
| Safe-point service | Global + per-service safe point, TTL | 6 |
| Deadlock detector | Cycle detection, self-loop, TTL cleanup | 8 |

### E2E Tests

All E2E tests run against a `ClusterHarness`-managed 3-node cluster started
in-process.

**Raw KV** (5 tests)
Put/get/delete/scan/batch/deleteRange/crash-recovery.

**Percolator** (31 tests)
2PC, lock-blocked read, rollback, conflict, async-commit, pessimistic lock,
TxnHeartBeat, GC.

**Multi-Raft** (3 tests)
Leader election, replication, failover.

**Region split** (5 tests)
PD-driven split, child peer spawn, auto-split.

**Region merge** (5 tests)
Split-then-merge, protocol safety, quiescence.

**Cross-region txn** (5 tests)
Transaction across split, write conflict.

**PD routing** (5 tests)
Cache-on-miss, TSO monotonicity, leader routing.

**Bank transfer** (2 tests)
Serial and concurrent SI conservation.

**Crash recovery** (3 tests)
Follower kill, leader kill, writes during recovery.

**Linearizability** (3 tests)
Wing-Gong checker under no chaos, follower chaos, and leader chaos.

**Chaos** (3 tests)
Balance conservation under leader kill, follower kill, and network partition.

**Benchmark** (4 tests)
rawPut, rawGet, txnCommit, txnConflictRetry with `LatencyHistogram`.

**Stress** (2 tests)
Raw KV (16 workers, 2000 ops each), txn (8 workers, bank transfer).

**CDC** (5 tests)
Commit event, delete event, rollback event, deregister, multi-key.

**Operational** (various)
TLS, auth, rate limiting, metrics, health check, follower read, graceful
drain, debug service, coprocessor.

---

## Linearizability Testing

The linearizability tests follow a Jepsen-style approach:

1. Multiple concurrent reader and writer clients operate on a shared key space.
2. Every operation is recorded with nanosecond invoke/return timestamps.
3. A Wing-Gong backtracking algorithm verifies per-key linearizability against
   the recorded history.
4. Operations that fail or produce ambiguous results (network errors, stale
   follower reads) are assigned an `INDETERMINATE` outcome so they do not
   falsely violate the history.

Three test modes exercise increasing severity:

- **No chaos** -- baseline correctness.
- **Follower-kill chaos** -- a background thread kills and restarts followers
  in 2-3 second cycles.
- **Leader-kill chaos** -- same pattern targeting the Raft leader. During
  leader chaos, null reads from stale followers are marked `INDETERMINATE`
  because raw reads bypass Raft.

---

## Chaos Testing

`ChaosTest` runs a bank-transfer workload with 4 workers, 10 accounts, and an
initial balance of 1000 per account.

Three scenarios are tested:

| Scenario | Fault injected |
|---|---|
| Leader kill | Raft leader is shut down and restarted |
| Follower kill | A follower is shut down and restarted |
| Network partition | A node is shut down to simulate partition |

The chaos thread randomly selects a victim, kills it for 1.5-5 seconds, then
restarts it. After the chaos period ends, a 5-second recovery window allows
the cluster to stabilize.

Verification checks total balance conservation (the SI invariant). The check
retries up to 5 times with 2-second gaps using `RetryConfig(30, 2, 1000)`.

A key design rule: the chaos thread always restarts the victim before exiting,
preventing a permanently downed node from breaking subsequent assertions.

---

## Benchmark Suite

Benchmarks use `LatencyHistogram`, a lock-free histogram built on
`LongAdder` with 1ms bucket width and 101 buckets.

Reported metrics: p50, p95, p99, max, avg, throughput (ops/sec).

JSON reports are written to `target/benchmark-results/` for CI regression
tracking.

| Benchmark | Workers x Ops | Notes |
|---|---|---|
| rawPut | 4 x 1000 | |
| rawGet | 4 x 1000 | |
| txnCommit | 4 x 500 | Full Percolator 2PC |
| txnConflictRetry | 4 x 50 transfers, 10 hot keys | Also verifies balance conservation |

---

## Test Infrastructure

### ClusterHarness

`ClusterHarness` is approximately 677 lines and is the backbone of all E2E
tests. It starts 1 PD server and N KV stores in a single JVM with real gRPC
transport.

Key design decisions:

- **Port reservation**: uses a ServerSocket hold-and-release pattern to avoid
  TOCTOU races where another process grabs the port between selection and bind.
- **Server binding**: retry logic with 3 attempts handles transient bind
  failures.
- **Node restart**: supports killing and restarting individual nodes. On
  restart, Raft logs are recovered from the persisted RocksDB state.
- **On-demand peer spawn**: peers can be added dynamically for conf-change and
  split tests.
- **Ordered shutdown**: gRPC servers are stopped first, then Raft peers, then
  storage engines, all within a 10-second deadline.
- **Data isolation**: each test gets its own `@TempDir` so test data never
  leaks between runs.

---

## How to Run Tests

```bash
# Full suite (~309 tests, ~9 min)
mvn test

# Unit tests only (fast, ~10s)
mvn test -pl common,pd,kv,client

# E2E tests only
mvn test -pl tests

# Single test class
mvn test -pl tests -Dtest=LinearizabilityE2ETest

# Single test method
mvn test -pl tests -Dtest="ChaosTest#balanceConservedUnderLeaderKillChaos"

# With verbose output
mvn test -pl tests -Dtest=PercolatorE2ETest -Dsurefire.useFile=false
```

---

## CI Integration

CI runs on GitHub Actions with JDK 17 on `ubuntu-latest`. The pipeline stages
are:

1. Compile
2. Test
3. Package
4. Docker build

Artifacts:

| Artifact | Retention |
|---|---|
| JaCoCo coverage reports | 14 days |
| Surefire reports (uploaded on failure) | 7 days |
| Benchmark JSON results | 30 days |
