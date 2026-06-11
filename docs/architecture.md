# x-kv Architecture

## 1. System Overview

x-kv is a distributed transactional KV store targeting TiKV v8.x wire-protocol parity.
It is built with Java 17, RocksDB (via JNI), gRPC, and Multi-Raft (via x-raft-lib).

The system consists of three main components:

- **PD (Placement Driver)** -- cluster metadata, timestamp oracle, region scheduling.
- **KV Store** -- Multi-Raft storage engine with MVCC and Percolator-based transactions.
- **Client SDK** -- PD-aware Java client with region cache, lock resolution, and two-phase commit.

```
                        +-----------+
                        |  Client   |
                        |   SDK     |
                        +-----+-----+
                              |
                 +------------+------------+
                 |                         |
           TSO / routing            raw KV / txn RPCs
                 |                         |
           +-----v-----+            +-----v-----+
           |    PD      |            |  KV Store  |
           | (3-node HA)|            | (Multi-Raft)|
           +-----------+            +-----------+
```

---

## 2. Module Architecture

The project is organized into 7 Maven modules with no circular dependencies.

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

### proto (x-proto)

10 `.proto` files providing TiKV v8.x gRPC wire surface:

| Service       | RPCs |
|---------------|------|
| Tikv          | 29 + BatchCommands |
| PD            | 22   |
| KvRaft        | 2    |
| ChangeData    | 2    |
| Debug         | 9    |

### common (x-common)

Shared utilities consumed by all higher-level modules:

- TLS / mTLS configuration
- Auth interceptors (gRPC)
- Rate limiting
- Metrics (Micrometer + Prometheus)
- Structured logging
- Configuration loading

### pd (x-pd)

Placement Driver -- the cluster brain:

- **HlcTsoOracle** -- Hybrid Logical Clock timestamp oracle, strictly monotonic across leader changes.
- **RocksDbPdStateMachine** -- Raft state machine for PD metadata.
- **Schedulers** -- leader balance, region balance, split checker.
- **DeadlockDetector** -- distributed deadlock detection for pessimistic transactions.
- **SafePointService** -- GC safe point management.
- **PdRaftNode** -- 3-node HA via internal Raft consensus.

### kv (x-kv-store)

Multi-Raft KV storage engine:

- **RocksStorageEngine** -- single RocksDB instance, 4 column families (DEFAULT, LOCK, WRITE, RAFT).
- **RegionPeerImpl** -- per-region Raft peer lifecycle and snapshot management.
- **MvccTxn** -- MVCC transaction logic (Percolator model).
- **ConcurrencyManager** -- 32-stripe RWLock for concurrent access control.
- **GrpcRaftTransport** -- inter-store Raft message transport.
- **CDC** -- Change Data Capture event streaming.

### client (x-client)

PD-aware Java SDK:

- **PdClient** -- PD connection management, TSO fetching, cluster info.
- **RegionCache** -- `TreeMap` + epoch-based region routing cache.
- **TsoBatcher** -- bidi-stream coalescing for high-throughput TSO allocation.
- **TwoPhaseCommitter** -- Percolator two-phase commit orchestration.
- **LockResolver** -- stale lock detection and resolution.
- **Backoffer** -- exponential backoff with jitter.
- **RawKvClient / TxnClient** -- user-facing raw and transactional APIs.

### ctl (x-kv-ctl)

CLI tool for operational tasks: cluster inspection, store management, region operations, GC triggering.

### tests (x-tests)

Integration and correctness testing: 68 test files, 309 test methods, `ClusterHarness` for embedded multi-node clusters, linearizability checker.

### Dependency Graph

No circular dependencies exist. `proto` and `common` are leaf modules. `pd` and `kv` sit at the same level with no cross-dependency. `client` and `ctl` depend on `proto` + `common`. `tests` sits at the top, depending on all modules.

```
tests --> client, ctl, kv, pd, proto, common
client --> proto, common
ctl    --> proto, common
kv     --> proto, common
pd     --> proto, common
proto  --> common
common --> (none)
```

---

## 3. Component Interaction

### Client -- PD Interaction

```
+----------+                     +----------+
|  Client  | --- GetTimestamp --> |    PD    |
|          | --- GetRegion ----> |          |
|          | --- GetStore -----> |          |
|          | <-- TSO response -- |          |
|          | <-- region meta --- |          |
+----------+                     +----------+
```

The client contacts PD to:

1. Allocate timestamps (TSO) for transaction begin / commit.
2. Resolve region routing (key range to store address).
3. Discover store topology.

### Client -- KV Store Interaction

```
+----------+                     +----------+
|  Client  | --- RawGet/Put ---> | KV Store |
|          | --- Prewrite -----> |          |
|          | --- Commit -------> |          |
|          | --- Scan ---------->|          |
|          | --- CheckTxnStatus >|          |
|          | <-- response ------ |          |
+----------+                     +----------+
```

Raw KV operations bypass the transaction layer. Transactional operations use Percolator RPCs (Prewrite, Commit, Cleanup, CheckTxnStatus, ResolveLock).

### KV Store -- PD Interaction

```
+----------+                     +----------+
| KV Store | -- StoreHeartbeat ->|    PD    |
|          | -- RegionHeartbeat >|          |
|          | <-- schedule cmds --|          |
|          |   (split, merge,   |          |
|          |    transfer leader) |          |
+----------+                     +----------+
```

Each KV store periodically reports store-level and region-level heartbeats. PD responds with scheduling commands (split regions, merge regions, transfer leadership, add/remove peers).

### KV Store -- KV Store (Raft Replication)

```
+-----------+    GrpcRaftTransport    +-----------+
| KV Store  | <-------- Raft -------> | KV Store  |
| (Leader)  | --- AppendEntries ----> | (Follower)|
|           | <-- AppendResponse ---- |           |
|           | --- Snapshot ---------> |           |
+-----------+                         +-----------+
```

Raft messages (AppendEntries, votes, snapshots) flow between stores via `GrpcRaftTransport` on a dedicated port (20170).

### PD -- PD (Internal Raft HA)

```
+--------+     +--------+     +--------+
| PD-1   |<--->| PD-2   |<--->| PD-3   |
| (Leader)|    |(Follower)|   |(Follower)|
+--------+     +--------+     +--------+
      Internal Raft consensus
      for metadata replication
```

Three PD nodes form an internal Raft group. All metadata mutations go through Raft consensus, ensuring consistency even during PD leader failover.

---

## 4. Data Flow

### Write Path

```
Client                KV Store (Leader)        Followers
  |                        |                      |
  |--- RawPut/Prewrite --->|                      |
  |                        |-- Raft propose       |
  |                        |-- log replication --->|
  |                        |<-- ack --------------|
  |                        |                      |
  |                        |-- apply:             |
  |                        |   WriteBatch {       |
  |                        |     CF mutations,    |
  |                        |     appliedIndex     |
  |                        |   } -> flushWal      |
  |                        |                      |
  |<--- response ----------|                      |
```

All mutations in a single Raft entry apply are bundled into one RocksDB `WriteBatch` containing both business CF mutations and the `appliedIndex` update, flushed with a single `flushWal` call. This guarantees atomicity between state machine apply and raft progress tracking.

### Read Path

```
Client                KV Store (Leader)
  |                        |
  |--- RawGet/KvGet ------>|
  |                        |-- ReadIndex (linearizable)
  |                        |   or direct engine read (raw KV)
  |                        |-- read from RocksDB snapshot
  |<--- response ----------|
```

For transactional reads, the store performs a ReadIndex operation to ensure linearizability before reading from a point-in-time RocksDB snapshot. Raw KV reads may use a direct engine read path.

### Transaction Path

```
Client              PD            KV Store (primary)    KV Store (secondary)
  |                  |                  |                       |
  |-- GetTimestamp ->|                  |                       |
  |<- startTs -------|                  |                       |
  |                                     |                       |
  |-- read (check locks) ------------->|                       |
  |<- value / lock info ---------------|                       |
  |                                     |                       |
  |-- Prewrite (primary) ------------->|                       |
  |-- Prewrite (secondary) ---------------------------------------->|
  |<- prewrite ok ---------------------|                       |
  |<- prewrite ok --------------------------------------------------|
  |                                     |                       |
  |-- GetTimestamp ->|                  |                       |
  |<- commitTs ------|                  |                       |
  |                                     |                       |
  |-- Commit (primary) --------------->|                       |
  |<- commit ok -----------------------|                       |
  |                                     |                       |
  |-- Commit (secondary, async) ---------------------------------->|
```

1. **beginTxn**: acquire `startTs` from PD's TSO.
2. **read**: check for conflicting locks; resolve stale locks if found.
3. **prewrite**: two-pass, all-or-nothing lock acquisition on primary and all secondaries.
4. **commit**: probe-then-commit on the primary key; secondary commits are asynchronous.
5. **cleanup**: resolve any leftover locks on abort.

---

## 5. Load-Bearing Invariants

These invariants are critical to correctness. Violating any of them introduces data loss or consistency bugs.

### Inv-1: Single fsync per Raft entry apply

Each Raft entry apply produces exactly one RocksDB `WriteBatch` containing all CF mutations plus the `appliedIndex` update, committed with a single `flushWal`. This ensures that either all mutations and the progress marker are persisted, or none are. Without this, a crash between two separate writes could leave the applied index ahead of the actual state (data loss) or behind it (duplicate apply).

### Inv-2: Single snapshot view across CFs

All CF reads within a single request use one RocksDB `Snapshot` handle, providing a consistent point-in-time view across DEFAULT, LOCK, and WRITE column families. Atomic ingest operations also respect this boundary. Without this, a read could observe a WRITE record but miss the corresponding DEFAULT value, or vice versa.

### Inv-3: PD state CF is Raft-only

All mutations to PD's metadata column family go through the internal Raft state machine. There is no leader fast path that bypasses Raft. This ensures that PD metadata is always replicated before being acknowledged, preventing split-brain metadata divergence.

### Inv-4: TSO strictly monotonic across leader changes

When a new PD leader is elected, it initializes its TSO with `physicalBound + 1` and reloads state via `reloadAfterLeaderChange`. This guarantees that timestamps never go backward, even across leader transitions. Backward timestamps would violate snapshot isolation by making newer transactions invisible to older snapshots.

### Inv-5: Region merge is two-sided + epoch-idempotent

Region merge requires coordination between both the source and target regions. PD verifies region epochs before executing the merge and before any rollback. Epoch-idempotency ensures that stale merge commands (from delayed heartbeats) are safely rejected rather than applied twice.

### Inv-6: Commit has three states

Transaction commit resolution returns one of three states: `COMMITTED`, `ROLLED_BACK`, or `UNKNOWN`. The `UNKNOWN` state is never collapsed into either of the other two. Collapsing `UNKNOWN` into `COMMITTED` risks acknowledging uncommitted data; collapsing it into `ROLLED_BACK` risks rolling back a transaction that was actually committed on another node.

---

## 6. Storage Layout

x-kv uses a single RocksDB instance per KV store with four column families.

```
+---------------------------------------------------------------+
|                     RocksDB Instance                          |
+---------------+---------------+---------------+---------------+
|   DEFAULT CF  |    LOCK CF    |   WRITE CF    |   RAFT CF     |
+---------------+---------------+---------------+---------------+
```

### DEFAULT CF -- User Values

Stores raw user values keyed by MVCC-encoded keys.

```
Key:   userKey + bigEndian(~ts)     (descending timestamp order)
Value: raw user value bytes
```

The bitwise complement of the timestamp (`~ts`) ensures that newer versions sort before older versions under RocksDB's default byte-order comparator, making "read latest version" a simple prefix seek.

### LOCK CF -- Percolator Locks

Stores active transaction locks.

```
Key:   userKey
Value: Lock encoding {
         lockType,
         primaryKey,
         startTs,
         ttl,
         asyncCommit fields (minCommitTs, secondaries),
         pessimistic fields (forUpdateTs)
       }
```

### WRITE CF -- Percolator Write Records

Stores committed transaction write records.

```
Key:   userKey + bigEndian(~commitTs)
Value: Write encoding {
         writeType (Put, Delete, Rollback, Lock),
         startTs,
         shortValue (inlined if small enough)
       }
```

Short values (below a size threshold) are inlined directly in the WRITE record to avoid an extra DEFAULT CF lookup.

### RAFT CF -- Raft Metadata and Logs

Stores per-region Raft state with a compact key layout (9-17 bytes).

```
Contents:
  - Raft log entries (per region)
  - Hard state (term, vote, commit index)
  - Applied index
  - Deduplication map
```

---

## 7. Deployment Architecture

### Topology

```
                    +------ Network ------+
                    |                     |
   +--------+  +--------+  +--------+    |
   | PD-1   |  | PD-2   |  | PD-3   |   |   PD Cluster (3-node Raft HA)
   | :2379  |  | :2379  |  | :2379  |   |
   +--------+  +--------+  +--------+    |
                    |                     |
   +--------+  +--------+  +--------+    |
   | KV-1   |  | KV-2   |  | KV-3   |   |   KV Stores (Multi-Raft)
   | :20160 |  | :20160 |  | :20160 |   |   Each hosts multiple regions
   | :20170 |  | :20170 |  | :20170 |   |
   | :20180 |  | :20180 |  | :20180 |   |
   +--------+  +--------+  +--------+    |
                    |                     |
                    +---------------------+
```

### Ports

| Port  | Purpose                    |
|-------|----------------------------|
| 2379  | PD client API (gRPC)       |
| 20160 | KV client API (gRPC)       |
| 20170 | KV Raft transport (gRPC)   |
| 20180 | KV metrics (Prometheus)    |

### Docker Compose

The project includes Docker Compose support for a standard deployment of 3 PD nodes + 3 KV stores. This provides a production-representative environment for development and testing.

### Minimum Deployment

- **PD**: 3 nodes for HA (Raft quorum requires a majority).
- **KV**: 3 or more stores for 3-replica data safety.
- All nodes require local SSD storage for RocksDB and Raft log persistence.
