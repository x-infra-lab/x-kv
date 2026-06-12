# x-kv

[English](./README.md) | [中文](./README_zh.md)

Distributed transactional KV store. Java 17, RocksDB, gRPC, Multi-Raft via
[`x-raft-lib`](https://github.com/x-infra-lab/x-raft-lib). Targets parity
with TiKV v8.x at the wire-protocol level.

> **Status: v0.2.0** — Multi-Raft cluster with MVCC + Percolator
> transactions, PD-driven routing, client SDK, region auto-split/merge,
> Jepsen-style linearizability tests, and benchmarks.

---

## Features

- **Multi-Raft consensus** — each region is an independent Raft group; leader
  election, log replication, and snapshot transfer over gRPC
- **MVCC + Percolator transactions** — optimistic 2PC, pessimistic locking,
  async commit, 1PC short-circuit, distributed deadlock detection
- **Placement Driver (PD)** — 3-node HA Raft cluster for cluster metadata,
  monotonic TSO, region/leader/split/merge/hot-region scheduling, GC safe-point
  management, OperatorController with per-store concurrency limits
- **Client SDK** — PD-aware routing, region cache with epoch invalidation,
  TSO batcher (coalesces N callers into one RPC), lock resolver, automatic
  txn conflict retry with exponential backoff
- **Region auto-split** — PD-driven split scheduling based on approximate
  region size reported via heartbeat
- **Region merge** — two-sided epoch-idempotent merge protocol
- **Follower read** — stale reads served from followers to reduce leader load
- **Raw KV API** — get, put, delete, scan, batchGet, batchPut, batchDelete,
  deleteRange, compare-and-swap, TTL
- **CDC (Change Data Capture)** — gRPC bidi-stream EventFeed with
  region-scoped event bus, resolved timestamp push
- **Operational tooling** — CLI (`xkv-ctl`), Prometheus metrics, structured
  logging (logstash-logback-encoder), health checks, rate limiting, TLS/mTLS,
  auth tokens, graceful drain
- **Test suite** — 332 test methods: E2E, linearizability (Wing-Gong with
  chaos monkey), chaos (leader kill / follower kill / network partition),
  stress, bank transfer SI, crash recovery, leader failover,
  snapshot catch-up, benchmarks

---

## Modules

| Module | Artifact | Role |
|--------|----------|------|
| `proto/`   | `x-proto`     | gRPC + protobuf wire (10 `.proto` files; TiKV v8.x parity) |
| `common/`  | `x-common`    | Shared utilities — TLS, auth, metrics, config, rate limiting |
| `pd/`      | `x-pd`        | Placement Driver — cluster metadata, TSO, scheduler |
| `kv/`      | `x-kv-store`  | Multi-Raft KV store with MVCC + Percolator |
| `client/`  | `x-client`    | PD-aware Java SDK (raw KV + transactional) |
| `ctl/`     | `x-kv-ctl`    | CLI tool for cluster, store, region, GC operations |
| `tests/`   | `x-tests`     | E2E / chaos / linearizability / benchmark suite |

---

## Quick Start

### Docker Compose (3 PD + 3 KV)

```bash
# Build and start the cluster
cd docker
docker compose up -d

# Verify all services are healthy
docker compose ps

# Use the CLI to inspect the cluster
docker compose exec kv1 java -jar /opt/x-kv/x-kv-ctl.jar \
    --pd pd1:2379 cluster members
```

PD endpoints are exposed at `localhost:2379`, `localhost:2381`, `localhost:2383`.
KV stores listen on `localhost:20160`, `localhost:20161`, `localhost:20162`.

### Java SDK

Add the dependency (once published):

```xml
<dependency>
    <groupId>io.github.x-infra-lab</groupId>
    <artifactId>x-client</artifactId>
    <version>0.2.0</version>
</dependency>
```

**Raw KV:**

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

**Transactions:**

```java
var config = ClientConfig.builder()
        .pdEndpoints(List.of("127.0.0.1:2379"))
        .build();

try (var txnClient = TxnClient.create(config)) {
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
# Build the CLI
mvn package -pl ctl -am -DskipTests -q

# Cluster operations
java -jar ctl/target/x-kv-ctl.jar --pd 127.0.0.1:2379 cluster members
java -jar ctl/target/x-kv-ctl.jar --pd 127.0.0.1:2379 cluster health

# Store operations
java -jar ctl/target/x-kv-ctl.jar --pd 127.0.0.1:2379 store list

# Region operations
java -jar ctl/target/x-kv-ctl.jar --pd 127.0.0.1:2379 region list --limit 10

# GC safe-point
java -jar ctl/target/x-kv-ctl.jar --pd 127.0.0.1:2379 gc safepoint
```

---

## Build & Test

**Prerequisites:** JDK 17+, Maven 3.8+

```bash
# Compile everything
mvn install -DskipTests

# Run the full test suite (332 test methods, ~9 min)
mvn test

# Run a specific test module
mvn test -pl tests

# Run a single test class
mvn test -pl tests -Dtest=LinearizabilityE2ETest
```

---

## Documentation

| Document | Description |
|----------|-------------|
| [Architecture](./docs/architecture.md) ([中文](./docs/architecture_zh.md)) | System overview, module dependencies, load-bearing invariants, deployment |
| [Design](./docs/design.md) ([中文](./docs/design_zh.md)) | Storage engine, Raft integration, MVCC/Percolator, PD, client SDK, CDC |
| [Testing](./docs/testing.md) ([中文](./docs/testing_zh.md)) | Test strategy, categories, linearizability/chaos testing, benchmarks, CI |
| [Deployment](./docs/deployment.md) ([中文](./docs/deployment_zh.md)) | Docker Compose, bare metal, TLS, monitoring, operational notes |
| [Configuration](./docs/configuration.md) ([中文](./docs/configuration_zh.md)) | All KV and PD configuration fields with defaults |
| [Changelog](./CHANGELOG.md) | All notable changes (Keep a Changelog format) |
| [Contributing](./CONTRIBUTING.md) | Build setup, code style, PR workflow |

---

## Roadmap

| Phase | Theme | Status |
|-------|-------|--------|
| **0** | Proto + module skeleton                     | done |
| **1** | Raft persistence contract + raw KV          | done |
| **2** | MVCC + Percolator                           | done |
| **3** | PD + TSO (monotonic across leader change)   | done |
| **4** | Multi-Raft + region split/merge             | done |
| **5** | Client SDK (Backoffer / TsoBatcher / 2PC)   | done |
| **6** | Test infrastructure (linearizability / benchmark / chaos) | done |
| **7** | Advanced features                           | partial |

### Remaining work (Phase 7+)

- Backup / Restore (BR-style SST export + service safe-point)

---

## Contributing

See [CONTRIBUTING.md](./CONTRIBUTING.md) for build instructions, code style,
and PR workflow.

---

## License

[Apache License 2.0](./LICENSE)
