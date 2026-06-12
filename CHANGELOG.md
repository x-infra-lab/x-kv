# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.2.0] — 2026-06-11

Complete rewrite of x-kv v0.1. The v0.1 audit identified 10 classes of
data-integrity defects (C1-C10); v0.2 addresses every one at its layer,
behind hard interface contracts.

### Added

**Proto & wire**
- 10 `.proto` files targeting TiKV v8.x wire parity: metapb, errorpb,
  kvrpcpb, tikvpb, pdpb, coprocessor, kv_serverpb, cdcpb, debugpb,
  pd_internalpb
- `BatchCommandsRequest` for throughput multiplexing
- `PeerRole.IncomingVoter / DemotingVoter` for joint consensus
- `UpdateServiceGCSafePoint` for BR/CDC GC protection
- `PrewriteRequest.use_async_commit / try_one_pc / max_commit_ts`

**Storage engine**
- `RocksStorageEngine` — single RocksDB with four column families
  (DEFAULT, LOCK, WRITE, RAFT) and per-CF tuning
- `PerRegionRaftEngine` — raft log, hard state, applied index, dedup map
  co-located in RAFT CF with compact key layout
- `RegionRaftStorage` — bridges engine onto x-raft-lib `Storage` SPI
- Single-fsync apply loop (Inv-1): one `WriteBatch` per entry containing
  all CF mutations + applied index + dedup update
- `SnapshotEngine` — single-instant cross-CF snapshots (Inv-2)

**MVCC & transactions**
- `MvccKey` — userKey + bigEndian(~ts) encoding
- `Lock` / `Write` — v3 encoding with async-commit secondaries,
  pessimistic `for_update_ts`, short-value inlining, overlapped-rollback
- `MvccReader` — get, scan, readLock, findWriteByStartTs
- `MvccTxn` — checkPrewrite, writePrewrite (all-or-nothing), commit
  (validates `commit_ts > start_ts`), rollback (returns commit_ts on
  already-committed), pessimisticLock, pessimisticRollback, checkTxnStatus
- `MvccApplyHandler` — two-pass all-or-nothing prewrite, probe-then-commit
- `MaxTsTracker` — propagates largest read_ts to prewrite for
  `min_commit_ts` enforcement
- `TransactionService` — full KV RPC surface (KvGet, KvPrewrite, KvCommit,
  KvBatchRollback, KvCheckTxnStatus, KvResolveLock, KvPessimisticLock,
  KvPessimisticRollback, KvTxnHeartBeat, KvGC, etc.)

**Placement Driver (PD)**
- `HlcTsoOracle` — HLC encoding `(physical_ms << 18) | logical`,
  `currentPhysical = physicalBound + 1` (v1 C5 fix), single-flight extend,
  `reloadAfterLeaderChange`
- `InMemorySafePointService` — global GC safe-point + per-service
  registrations with TTL
- `RocksDbPdStateMachine` — RocksDB-backed state machine with
  write-through cache for production deployments
- `PdServiceImpl` — full PD gRPC service: GetMembers, Bootstrap, AllocID,
  GetTimestamp (bidi stream), PutStore, GetStore, GetAllStores,
  StoreHeartbeat, RegionHeartbeat (bidi stream), GetRegion, GetRegionByID,
  ScanRegions, GC safe-point management
- `PdRaftNode` — 3-node HA Raft group for PD with `RocksDbStorage`,
  snapshot every 100 entries, state machine restore on restart
- `PdRaftTransport` — inter-PD gRPC bidi raft messaging

**Multi-Raft**
- `StoreImpl` — local region peer index with O(log N) `peerForKey`
- `GrpcRaftTransport` — gRPC bidi streams for inter-store raft messaging,
  per-target channel reuse, automatic reconnect
- `RaftMessageDispatcher` — routes inbound raft messages by region_id
- `KvRaftServiceImpl` — bidi-stream server for raft message delivery
- `RegionHeartbeater` — publishes leader info to PD every 100ms
- `ConcurrencyManager` — per-key striped latches (32 stripes), deadlock-free
  writer lock ordering

**Region lifecycle**
- `SplitDriver` — Raft-driven region split with epoch bump
- `SplitCheckerScheduler` — PD-driven auto-split based on approximate size
- `MergeDriver` — two-sided epoch-idempotent merge (Inv-5)
- Snapshot streaming for catch-up of slow/restarted followers

**Client SDK**
- `BackofferImpl` — per-reason exponential backoff with jitter + total budget
- `TsoBatcherImpl` — bidi PD stream coalescing N concurrent callers
- `RegionCacheImpl` — TreeMap byStartKey + epoch-aware update + overlap
  invalidation
- `StoreChannelCache` — storeId -> gRPC channel resolved via PD
- `RegionRequestSenderImpl` — handles all region error types (NotLeader,
  EpochNotMatch, ServerIsBusy, etc.) with backoff routing
- `RawKvClientImpl` — full raw KV surface with cross-region auto-grouping
- `LockResolverImpl` — Caffeine LRU + per-lock_ts single-flight resolve
- `TwoPhaseCommitterImpl` — Percolator 2PC with 1PC short-circuit, async
  commit, three-state outcome {COMMITTED, ROLLED_BACK, UNKNOWN}
- `TransactionImpl` — buffered writes, read-your-own-writes, lock resolver
  retry on reads
- `TxnClient.executeWithRetry` — automatic retry on WriteConflict/KeyLocked
  with exponential backoff and jitter

**Operational**
- `xkv-ctl` CLI — cluster members/health, store list/info, region list/info,
  GC safe-point
- `MetricsHttpServer` — Prometheus scrape endpoint with health/readiness
- `GrpcServerMetricsInterceptor` — per-RPC latency + status code histograms
- `MdcServerInterceptor` — structured MDC logging per request
- `ConcurrencyLimitInterceptor` — max in-flight request gating
- `AuthServerInterceptor` — bearer token authentication
- TLS/mTLS for client and raft planes (`GrpcChannelFactory`)
- Graceful drain (GracefulDrainTest)
- Docker images: `Dockerfile.pd`, `Dockerfile.kv`, `docker-compose.yml`
  for 3 PD + 3 KV cluster
- YAML config loading with CLI args > env vars > YAML file > defaults

**Schedulers**
- `LeaderBalanceScheduler` — balances region leaders across stores
- `RegionBalanceScheduler` — balances region replicas across stores
- `SplitCheckerScheduler` — auto-split when region exceeds size threshold
- `HotRegionScheduler` — transfers leadership of QPS-hot regions to less-hot stores
- `MergeCheckerScheduler` — merges adjacent small regions below size threshold
- `RuleCheckerScheduler` — enforces replica count rules (under/over-replicated, down-peer replacement)
- `OperatorControllerImpl` — operator lifecycle management with per-store concurrency limits (token bucket)
- `SimpleOperator` — minimal Operator implementation for scheduler-to-controller bridge
- `DeadlockDetector` — distributed wait-for graph cycle detection

**Test infrastructure**
- `ClusterHarness` — in-process PD + KV cluster with real gRPC transport,
  reservation-based port allocation (no TOCTOU races)
- `Linearizability` — Wing-Gong backtracking checker with `INDETERMINATE`
  op support
- `LatencyHistogram` — lightweight p50/p95/p99 histogram for benchmarks
- LinearizabilityE2ETest — concurrent reads/writes with and without chaos
  monkey (random follower kill/restart)
- BenchmarkE2ETest — rawPut, rawGet, txnCommit, txnConflict throughput +
  latency reports
- StressRawKvTest, StressTxnTest — high-concurrency correctness tests
- 66 test files, 290+ test methods total

### Fixed

- **C1 (dual-fsync):** Unified apply batch — all CF mutations + applied
  index in one `WriteBatch` with one fsync (Inv-1)
- **C2 (split snapshot):** Single `Snapshot` handle across all CFs (Inv-2)
- **C3 (non-atomic snapshot apply):** Atomic `IngestExternalFile` (Inv-2)
- **C4 (unilateral merge rollback):** Two-sided PD verification (Inv-5)
- **C5 (TSO off-by-one):** `currentPhysical = physicalBound + 1` +
  mandatory `reloadAfterLeaderChange` (Inv-4)
- **C6 (leader fast-path write):** State CF written only by Raft apply;
  no leader-side fast path (Inv-3)
- **C7 (commit outcome collapse):** Three-state `{COMMITTED, ROLLED_BACK,
  UNKNOWN}` — UNKNOWN propagates to caller (Inv-6)
- **C8 (split during commit):** Region-aware key re-routing in 2PC
- **C9 (dedup collision):** Unique per-entry dedup keys
- **C10 (BatchGet single-region):** Cross-region auto-grouping by region
