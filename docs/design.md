# x-kv Design Document

x-kv is a TiKV-compatible distributed transactional KV store written in Java. It implements the Percolator transaction protocol on top of a single-RocksDB, multi-CF storage engine, with Raft-based replication via [x-raft-lib](https://github.com/xinfra-lab/x-raft-lib), a Placement Driver (PD) for cluster management, and a full client SDK.

---

## 1. Storage Engine

### Architecture

All data for every region on a node resides in a single RocksDB instance with four column families (CFs): `default`, `lock`, `write`, and `raft`. The class `RocksStorageEngine` is the sole owner of the RocksDB handle and all CF handles.

```
RocksDB instance
  +-- CF default   (MVCC values)
  +-- CF lock      (Percolator lock records)
  +-- CF write     (MVCC commit/rollback records)
  +-- CF raft      (per-region Raft log, hard state, applied index)
```

### Per-CF Tuning

Each CF has workload-specific RocksDB options configured in `RocksStorageEngine.open()`:

- **DEFAULT CF** -- Optimized for point lookups. 10-bit bloom filter, shared LRU block cache, 16 KB block size, `optimizeForPointLookup` enabled, LZ4 compression, level-style compaction with dynamic level bytes.

- **LOCK CF** -- Similar to DEFAULT but with a smaller write buffer (capped at 16 MB) and only 2 memtables (`maxWriteBufferNumber=2`). Lock entries are small and short-lived; the CF stays hot in cache.

- **WRITE CF** -- Whole-key bloom filter (`setWholeKeyFiltering(true)`) with 16 KB block size. The dominant access pattern is seek-by-userKey (everything except the trailing 8-byte `~commitTs` suffix). LZ4 compression, 4 memtables.

- **RAFT CF** -- Sequential-write-optimized with 64 KB block size (append-mostly Raft log entries). No bloom filter (sequential access does not benefit). 2 memtables.

### Shared Block Cache

A single `LRUCache` is shared across all four CFs (default: 256 MB, configurable via `KvConfig.EngineConfig.blockCacheBytes`). Index and filter blocks are cached and pinned at L0 (`setCacheIndexAndFilterBlocks(true)`, `setPinL0FilterAndIndexBlocksInCache(true)`).

### WriteBatch Atomicity

All CF mutations within a single Raft entry are packed into one `org.rocksdb.WriteBatch`. The batch is written with `sync=false` and flushed to disk via a single `flushWal(true)` at the end of each Raft Ready round. This guarantees:

- **Inv-1**: Applied index and business data are atomically persisted. A crash mid-Ready either persists the whole batch or nothing.
- **Single fsync per Ready round**: Reduces write amplification compared to per-entry fsync.

### Snapshot Lifecycle

`RocksSnapshot` wraps `org.rocksdb.Snapshot`. The snapshot pins SST files (blocking compaction and consuming disk). It must be explicitly released via `close()`, which calls `RocksDB.releaseSnapshot()`. The `MvccReader` binds all three data CF reads to a single snapshot for cross-CF consistency.

### SnapshotEngine

`SnapshotEngineImpl` handles Raft snapshot generation and installation:

- **Generation**: Pins a single RocksDB snapshot at the start. Iterates the three data CFs (`default`, `lock`, `write`) under that snapshot. Emits 64 KB chunks in wire format `[4B keyLen][key][4B valueLen][value]`. Each chunk carries a CRC32C checksum.

- **Installation**: Verifies CRC32C on every received chunk. Applies an atomic `WriteBatch` containing: (1) `deleteRange` of the region's key range on all three CFs, (2) all new key/value pairs from the snapshot. The batch flushes with `sync=true`.

---

## 2. Raft Integration

### Library Dependency

x-kv uses x-raft-lib as an external library. Each region has one `RegionPeerImpl` that owns one x-raft-lib `Node`.

### RegionPeerImpl

The central Raft integration class. Key configuration (via `Config.builder()`):

- `preVote(true)` and `checkQuorum(true)` -- Prevents disruptive elections from partitioned nodes.
- `maxSizePerMsg(1 << 20)` -- 1 MB per Raft message.
- `maxInflightMsgs(256)` -- Pipeline depth for append entries.

### Apply Loop (readyLoop)

Each iteration of `readyLoop()` pulls one `Ready` from x-raft-lib and processes it:

**Phase 0 -- Snapshot Install**: If the leader sent a snapshot (`MsgSnapshot`), install user-data CFs and Raft metadata before processing any log entries.

**Phase A -- Log Durability**: Write new entries and hard state in one `WriteBatch` with `sync=false`. The entries land in memtable + WAL buffer immediately.

**Phase B -- Apply Committed Entries**: Each committed entry is applied one at a time in its own `WriteBatch`. The batch contains the entry's business CF mutations AND the `appliedIndex` bump, written with `sync=false`. Per-entry application is critical: the MVCC apply path reads RocksDB state (`engine.get`) during conflict checks; if two entries shared a batch, entry N's reader would miss entry N-1's staged-but-unflushed lock CF writes.

**Final flushWal**: A single `flushWal(true)` at the end of the Ready round fsyncs all Phase A + Phase B writes to disk. This must complete before `node.advance()` tells Raft the data is persisted.

### ReadIndex for Linearizable Reads

`RegionPeerImpl` implements linearizable reads via Raft's ReadIndex protocol:

1. The caller invokes `readIndex()` which generates a UUID-based request context and calls `node.readIndex(ctx)`.
2. The `pendingReadIndices` map (key: `ByteBuffer`-wrapped UUID, value: `CompletableFuture<Void>`) tracks in-flight requests.
3. When `Ready.readStates()` arrives, the ready loop matches the context to the pending future. If `appliedIndex >= readState.index()`, the future completes immediately. Otherwise, it is parked in `readIndexWaiters` (a `ConcurrentSkipListMap<Long, List<CompletableFuture<Void>>>`) and drained after each committed-entry batch apply.

### GrpcRaftTransport

Per-region transport. Key design:

- **Per-target channel reuse**: `ConcurrentHashMap<Long, OutboundLink>` keyed by peer ID. Channels are created lazily on first send.
- **Bidi streams**: Outbound Raft messages are sent on a streaming `StreamObserver`. Stream errors trigger automatic reconnect on next send.
- **Snapshot delivery**: `MsgSnapshot` messages are sent via a separate `sendSnapshot` streaming RPC, not the normal message stream.

### RaftMessageDispatcher

Per-store routing table from `region_id` to local `GrpcRaftTransport`:

- `ConcurrentHashMap<Long, GrpcRaftTransport>` for O(1) dispatch.
- On-demand peer spawn: when a message arrives for an unknown region, fires a `MissingRegionHandler` callback (once per region, guarded by `spawnInFlight` flag).

### LogCompactionWorker

Periodic Raft log compaction:

- Scans all leader peers. For each, if `appliedIndex - firstIndex > gapThreshold` (default 10,000), proposes `ADMIN_COMPACT_LOG` that advances `firstIndex` to `appliedIndex - safetyMargin` (default 1,000).
- Generates a snapshot at `appliedIndex` before compacting, so slow followers can still catch up.
- Configurable interval (default 60s), gap threshold, and safety margin.

---

## 3. MVCC + Percolator Transaction Protocol

### Key Encoding

`MvccKey` encodes MVCC keys as `userKey || bigEndian(~ts)`. Bit-inverting the timestamp before big-endian encoding puts the newest version first in RocksDB's lexicographic order. The suffix is always 8 bytes (`TS_SUFFIX_LEN = 8`).

- `encode(userKey, ts)` -- Produces the physical key.
- `firstVersionFor(userKey)` -- `encode(userKey, Long.MAX_VALUE)` for seeking to the latest version.
- `afterAllVersionsOf(userKey)` -- Appends 9 bytes of `0xFF` for leaping past all versions of a userKey.

### Lock Encoding (v3)

`Lock` uses binary format version `0x33`:

```
[1B version=0x33] [1B type] [8B startTs] [8B ttlMs] [8B txnSize]
[8B minCommitTs] [8B forUpdateTs] [1B flags: bit0=useAsyncCommit]
[4B primaryLen] [primaryLen B primary]
[4B secondariesCount] { [4B keyLen] [keyLen B key] }*
```

Lock types: `PUT`, `DELETE`, `LOCK`, `PESSIMISTIC`.

Key v3 additions over v1: `forUpdateTs` for pessimistic locks (was runtime-only in v1, lost on restart), and `flags + secondaries` for async-commit (v1 declared the option but the on-disk lock did not carry secondaries, making lock resolver recovery impossible).

### Write Encoding (v3)

`Write` uses binary format version `0x33`:

```
[1B version=0x33] [1B type] [8B startTs] [1B flags: bit0=hasOverlappedRollback]
[4B shortValueLen] [shortValueLen B shortValue]
```

Write types: `PUT`, `DELETE`, `ROLLBACK`, `LOCK`.

**Short-value inlining**: Values up to `SHORT_VALUE_MAX_LEN` (64 bytes) are inlined in the Write record. The MVCC reader can answer `Get` from the write CF alone without a second read against the default CF, cutting snapshot-read latency roughly in half for small-value workloads.

**hasOverlappedRollback**: For async-commit / 1PC, indicates a rollback record that overlaps a concurrent write. Lock resolver respects this flag to avoid double-aborting.

### MvccReader

`MvccReader` performs snapshot-bound reads across CFs. All reads bind to a single `StorageEngine.Snapshot` for cross-CF consistency.

**Get protocol**:
1. Read lock CF for userKey. If lock exists with `startTs <= readTs` and type is not `LOCK` or `PESSIMISTIC`, throw `KeyLockedException`.
2. Seek write CF at `MvccKey.encode(userKey, readTs)`. Walk forward through the userKey's versions, skipping `ROLLBACK` and `LOCK` records, until the first `PUT` or `DELETE`.
3. For `PUT`: return inline short-value if present; otherwise read default CF at `MvccKey.encode(userKey, write.startTs)`. For `DELETE`: return not-found.

**Scan**: Single forward sweep of the write CF. For each userKey, picks the first visible (commitTs <= readTs, non-rollback) write, resolves its value, then leaps past the userKey using `afterAllVersionsOf()`.

### MvccTxn

Server-side accumulator for one Percolator round. Key operations:

**checkPrewrite**: Read-only conflict detection. Checks for:
1. Self-rollback (ROLLBACK record at this startTs).
2. Write conflict (non-rollback write with commitTs >= startTs).
3. Key locked by another txn.
4. Idempotent OK if lock from same txn already exists.

**writePrewrite**: Two-pass all-or-nothing. The caller MUST run `checkPrewrite` on every key first; only if all checks pass does the caller proceed to `writePrewrite` for each. This prevents orphan locks from v1's single-pass design.

**commit**: Validates `commitTs > startTs` and `commitTs >= lock.minCommitTs`. Promotes the lock to a Write record. Inlines short values into the Write record and drops the default CF entry when possible.

**rollback**: Returns discovered `commitTs` on already-committed transactions (v1 race fix). Writes a `ROLLBACK` record at `(key, startTs)` and drops any existing lock + default CF entry.

**checkTxnStatus**: TTL check with re-check for concurrent commit (v1 fix: prevents force-rollback of already-committed txns). Includes async-commit gate: never force-rollback an async-commit primary since the txn may have committed via secondaries' `min_commit_ts` path.

**pessimisticLock**: Acquire pessimistic lock with write conflict check at `forUpdateTs` (not `startTs`). Supports re-lock (refresh `forUpdateTs`).

### MaxTsTracker

Per-region `AtomicLong` tracking the largest read timestamp ever served by this leader. Every snapshot read calls `observe(readTs)`. Every prewrite derives `minCommitTs = max(startTs + 1, maxTs + 1)`. This guarantees no "after-the-fact" commit can sneak below an already-served `readTs`, preserving Snapshot Isolation.

Persisted periodically (in the Ready round's fsync batch) so a restarted leader resumes with at least this floor.

### ConcurrencyManager

32 striped `ReentrantReadWriteLock`s (configurable), keyed by `Math.floorMod(Arrays.hashCode(key), stripeCount)`.

- **Point reader**: Acquires read lock on one stripe, calls `maxTs.observe(readTs)`, runs work.
- **Coarse reader** (scan): Acquires every stripe's read lock in ascending id order.
- **Multi-key writer**: Computes distinct stripe set via `BitSet`, acquires write locks in ascending id order (deadlock-free).
- **Coarse writer** (snapshot install, region admin): Acquires every stripe's write lock in ascending id order.

### Three-State Commit Outcome

`TwoPhaseCommitterImpl.CommitResult` has three terminal states: `COMMITTED`, `ROLLED_BACK`, `UNKNOWN`. `UNKNOWN` is returned when primary commit fails ambiguously (network error, response lost). v1 collapsed `UNKNOWN` into `ROLLED_BACK` and lost transactions.

---

## 4. Placement Driver (PD)

### HlcTsoOracle

Hybrid-Logical-Clock encoding: `(physical_ms << 18) | logical`. 18-bit logical counter supports 262,143 allocations per millisecond.

**Monotonicity contract**:
- Physical bound is persisted via Raft. Allocator may issue TSOs with physical <= this bound freely.
- On construction: `currentPhysical = physicalBound + 1` (the +1 is critical -- v1 was off-by-one).
- On leader change: `reloadAfterLeaderChange()` sets `currentPhysical = physicalBound + 1`, `currentLogical = 0`. v1 forgot to wire this on the leader observer.
- **Single-flight extend**: When the cursor approaches `physicalBound`, concurrent allocators share one in-flight Raft proposal via `CompletableFuture`. Never propose two concurrent extends.
- **No NTP dependency for correctness**: Wall clock is used as a hint; monotonicity relies on the persisted bound + Raft ordering.

Default look-ahead window: 50ms (`DEFAULT_SAVED_INTERVAL_MS`), capped at 1000ms.

### RocksDbPdStateMachine

PD state persisted in a dedicated RocksDB via Raft-only writes (all mutations go through Raft proposals -- Inv-3). Write-through cache for hot-path reads.

### Schedulers

- **LeaderBalanceScheduler**: Balances leaders across stores, aware of slow-score metrics from `StoreStatsCache`.
- **RegionBalanceScheduler**: Balances regions by count, with low-space-ratio check to avoid moving data to nearly-full stores.
- **SplitCheckerScheduler**: Triggers split when a region's approximate size exceeds 64 MB threshold (`KvConfig.RegionConfig.splitRegionBytes`).

### DeadlockDetector

Global wait-for graph for pessimistic lock deadlock detection:

- On `addWaitFor(edge)`: Insert edge, BFS from holder to waiter. If reachable, a cycle is found -- return the cycle chain (waiter-first order). The edge is NOT inserted when a cycle is detected.
- O(V + E) per insertion via BFS.
- TTL-based garbage collection (`cleanupExpired()`) to prevent crashed clients from pinning edges.
- Thread-safe via synchronized methods. Suitable for thousands of in-flight pessimistic txns.

### SafePointService

Global GC safe-point management with per-service registrations:

- **Global safe-point**: Lower bound below which MVCC versions may be GC'd.
- **Service safe-points**: BR / CDC / long-running SQL register a safe-point lower bound with a TTL. While active, the global safe-point cannot advance past it.
- `advance()`: Periodically advances the global safe-point respecting active service registrations.
- `updateGcSafePoint(target)`: Operator-driven monotonic ratchet.

### PD HA

3-node HA via `PdRaftNode` with `PdRaftTransport`. The PD cluster itself uses Raft for state replication, ensuring the TSO, cluster metadata, and scheduling decisions survive leader failures.

---

## 5. Client SDK

### PdClient

Discovers the PD leader, streams TSO, routes region lookups. Provides both blocking and async stubs. `switchLeader()` handles PD leader failover.

### TsoBatcherImpl

Single bidi stream to PD for TSO allocation:

- Caller threads enqueue via `getTimestamps(count)` and receive a `CompletableFuture<Long>`.
- One dispatcher thread drains the queue, coalescing entries up to `maxBatchSize` or until `batchWaitMicros` elapses, into a single `TsoRequest`.
- PD's response carries the first allocated TSO + count; the dispatcher splits this into individual futures.
- Stream errors fail all in-flight futures and trigger reconnect.
- Can sustain >100k TSO/s on one TCP connection (v1 served one blocking call per `getTimestamp(1)` and capped at ~1k/s).

### RegionCacheImpl

Dual-index region cache:

- `byStartKey`: `TreeMap<byte[], RegionInfo>` (unsigned lexicographic compare) for key-based lookups via `floorEntry`.
- `byId`: `ConcurrentHashMap<Long, RegionInfo>` for ID-based lookups.
- **Epoch dominance check**: Updates with older `(conf_ver, version)` are dropped to prevent stale PD replies from clobbering a fresher cache.
- **Overlap invalidation**: On split/merge, uses `TreeMap.subMap` for O(log N + k) range invalidation (replaces v1's full table scan).
- Protected by a `ReentrantReadWriteLock`.

### StoreChannelCache

Maps `storeId` to `ManagedChannel`, resolved via PD's `GetStore` RPC. Channels are reused across regions.

### BackofferImpl

Per-request exponential-backoff-with-jitter:

- Per-error-class base/cap: `REGION_MISS`, `TXN_LOCK`, `SERVER_BUSY`, `NETWORK`, `NOT_LEADER`, etc.
- Formula: `sleep = min(cap, base * 2^attempt) +/- jitter`.
- Total elapsed bounded by `maxOverallElapsedMs` (default 40s).
- gRPC Context deadline aware: effective deadline is `min(config budget, external deadline)`.
- `fork()` creates a child backoffer sharing the same deadline.

### TwoPhaseCommitterImpl

Drives the Percolator 2PC protocol:

1. **Prewrite**: Group mutations by region. Prewrite primary region first, then secondaries. Any prewrite error rolls back the entire txn.
2. **Fetch commitTs**: Obtain a TSO from PD via `TsoBatcher`.
3. **Commit primary**: If primary commit fails with a known permanent error (rolled back), return `ROLLED_BACK`. If it fails ambiguously, return `UNKNOWN`.
4. **Async secondaries**: Best-effort; secondary failure does NOT change the txn outcome. Lock resolver cleans up later.
5. **1PC short-circuit**: Single-key optimistic txns skip the commit phase when the server returns `onePcCommitTs > 0`.
6. **Async-commit**: When enabled, `commitTs = max(fetched commitTs, primaryMinCommitTs)`.

### LockResolverImpl

Resolves encountered locks:

- **Caffeine LRU cache**: `lock_ts` to terminal verdict (COMMITTED or ROLLED_BACK). Bounded size + TTL, replacing v1's unbounded `ConcurrentHashMap` that grew until OOM.
- **Per-lock_ts single-flight**: `ConcurrentMap<Long, CompletableFuture<Verdict>>` ensures N concurrent readers hitting the same lock all await one `CheckTxnStatus` + `ResolveLock` pair.
- **Honest CheckTxnStatus chain**: When status returns `NoAction` (lock alive, TTL not expired), the resolver does NOT call `ResolveLock`. v1 force-rolled-back live txns and lost money.

### TransactionImpl

Client-side transaction:

- **Buffered writes**: `TreeMap<byte[], Mutation>` ordered by key (deterministic primary selection). Last-write-wins semantics.
- **Read-your-own-writes**: `get()` checks the buffer first. `scan()` / `reverseScan()` merge server results with buffered mutations.
- **Lock resolver retry**: On `KeyError(locked)`, resolves the lock and retries within backoff budget.
- **Pessimistic locking**: `lockKeysForUpdate()` acquires fresh `forUpdateTs` from TSO, issues `PessimisticLockRequest` per region group. Handles deadlock errors as non-retryable.
- **Three terminal states**: `COMMITTED`, `ROLLED_BACK`, `UNKNOWN`. `close()` auto-rollbacks if still `ACTIVE`.

---

## 6. Region Lifecycle

### Split

Driven by `SplitDriver` + `SplitCheckerScheduler`:

1. PD allocates new region IDs and peer IDs via `AskBatchSplit`.
2. Build `SplitRegionProposal` with shrunken parent + children. `epoch.version` is bumped on every resulting region.
3. Propose `ADMIN_SPLIT` through Raft. On apply: atomic `WriteBatch` persists all region descriptors, parent's in-memory descriptor is refreshed, split observer fires for each child.

Split is metadata-only: data stays in the shared RocksDB. The threshold is approximate size > 64 MB (`KvConfig.RegionConfig.splitRegionBytes`).

### Merge

Three-phase dance via `MergeProtocolImpl`:

1. **PrepareMerge**: Source region proposes `ADMIN_PREPARE_MERGE`, entering quiescence (all non-merge business writes are rejected). The target region's epoch at prepare time is recorded.
2. **CommitMerge**: Target region proposes `ADMIN_COMMIT_MERGE` with the merged descriptor (expanded range, bumped `epoch.version`).
3. **RollbackMerge**: Source region can propose `ADMIN_ROLLBACK_MERGE` to exit quiescence -- but only after querying PD for the target's current epoch. If `target.epoch.version > prepareTimeBaseline`, the merge was already committed and rollback is forbidden (Inv-5).

The merge process is epoch-idempotent: replaying the same proposal is safe because epoch checks prevent double-application.

### Snapshot Catch-up

When a follower falls too far behind, the leader sends a Raft snapshot:

1. `SnapshotEngineImpl.buildAndStream()` pins a RocksDB snapshot, iterates the three data CFs, emits CRC32C-chunked data.
2. `GrpcRaftTransport.sendViaSnapshotStream()` delivers chunks over the `sendSnapshot` gRPC streaming RPC.
3. `SnapshotEngineImpl.receiveAndInstall()` verifies CRC32C on every chunk, then applies a 3-CF atomic `WriteBatch` (delete stale range + insert fresh data) with `sync=true`.

---

## 7. CDC (Change Data Capture)

### CdcEventBus

Region-scoped pub/sub:

```java
ConcurrentHashMap<Long, CopyOnWriteArrayList<Consumer<CdcEvent>>>
```

- `subscribe(regionId, consumer)` -- Register per-region event consumer.
- `publish(event)` -- Fan out to all subscribers for the event's region. Exceptions in subscribers are caught and logged.

### MvccApplyHandler Integration

CDC events are published after commit/rollback operations are applied in the Raft apply loop. Events carry `regionId`, `type` (PUT, DELETE, ROLLBACK), `key`, `value`, `oldValue`, `startTs`, `commitTs`.

### ChangeDataServiceImpl

gRPC service implementing two bidi-streaming RPCs:

- **EventFeed**: Clients send `ChangeDataRequest` (register/deregister per region). Server subscribes to the `CdcEventBus` and streams `ChangeDataEvent` messages. One `sendLock` object serializes outbound writes.
- **ResolvedTs**: Periodic push of the current resolved timestamp to registered regions (1-second interval via `ScheduledExecutorService`).

### Current Limitations

- At-most-once delivery: events may be lost on stream errors.
- No sequence numbers for gap detection.
- No persistent event log: a new subscriber sees only events from the moment of subscription.

---

## 8. Operational Features

### Prometheus Metrics

`XKvMetrics` manages a singleton `PrometheusMeterRegistry`. All components register counters and histograms through it:

- `xkv_errors_total` with tags `component` and `operation`.
- gRPC interceptors (`GrpcServerMetricsInterceptor`, `GrpcClientMetricsInterceptor`) track request duration, count, and active requests.
- `MetricsHttpServer` exposes the `/metrics` endpoint for Prometheus scraping.

### Health / Readiness

- `/healthz` -- Liveness: the process is running and responsive.
- `/readyz` -- Readiness: the store has registered with PD and at least one region peer is active.
- `/metrics` -- Prometheus scrape endpoint.

### Structured Logging

`logstash-logback-encoder` produces JSON output. `MdcContextUtil` + `MdcServerInterceptor` inject MDC fields (`store_id`, `region_id`, `rpc_method`) into every gRPC handler's log context.

### TLS / mTLS

`SslContextFactory` builds Netty `SslContext` for three planes:

- **Client plane**: Between SDK clients and KV server (`KvConfig.clientTls`).
- **Raft plane**: Between KV peers for Raft messages (`KvConfig.raftTls`).
- **Metrics plane**: Optional TLS for the metrics HTTP endpoint.

`GrpcChannelFactory` wraps channel construction with optional TLS from `TlsConfig`.

### Authentication

`AuthServerInterceptor` validates a shared-secret token from the `x-auth-token` metadata header. `AuthClientInterceptor` attaches the token on every outbound RPC.

### Graceful Drain

`DrainingInterceptor` rejects new requests with `UNAVAILABLE` status (gRPC code) when `startDraining()` is called. Clients that receive this status retry against another store. The drain flag is an `AtomicBoolean` flipped once and never reset.

Drain timeout is configurable via `KvConfig.drainTimeoutMs` (default 10s).

### Rate Limiting

`ConcurrencyLimitInterceptor` uses a `Semaphore` with configurable max permits (default 10,000 via `KvConfig.maxConcurrentRequests`). Requests that cannot acquire a permit are rejected with `RESOURCE_EXHAUSTED` status. Permits are released on `onCancel`, `onComplete`, or `onHalfClose` exception.
