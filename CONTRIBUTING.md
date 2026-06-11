# Contributing to x-kv

Thank you for your interest in contributing to x-kv! This document covers
the build setup, coding conventions, and PR workflow.

## Prerequisites

- **JDK 17+** (we test with Eclipse Temurin)
- **Maven 3.8+**
- **Docker** (optional, for running the full cluster locally)

## Building

```bash
# Full compile + install to local .m2
mvn install -DskipTests

# Compile without installing
mvn compile
```

The project is a multi-module Maven build. Module dependencies flow one way:
`proto` -> `{pd, kv, client}` -> `tests`. There are no cyclic dependencies.

## Running Tests

```bash
# Full suite (~290 test methods, ~3 min)
mvn test

# Single module
mvn test -pl pd
mvn test -pl kv
mvn test -pl tests

# Single test class
mvn test -pl tests -Dtest=LinearizabilityE2ETest

# After changing code in pd/kv/client, re-install before running E2E tests
# (the tests module loads JARs from ~/.m2/repository)
mvn install -DskipTests && mvn test -pl tests
```

E2E tests start real PD and KV nodes in-process with gRPC transport.
They use `@TempDir` for data directories and dynamic port allocation
to avoid conflicts when tests run in parallel.

## Code Style

We don't use a formatter — just follow the patterns in existing code:

- **No Lombok.** Builders, records, and explicit getters throughout.
- **Builder pattern** for configuration objects (see `PdConfig`, `ClientConfig`).
- **Immutable after construction** — config objects, protobuf messages.
- **ReentrantReadWriteLock** for shared mutable state (not `synchronized`).
- **`Optional<T>`** for nullable returns, never null.
- **No comments unless the "why" is non-obvious.** Well-named identifiers
  carry the "what." Invariant comments (`// invariant:`) mark load-bearing
  correctness properties — treat those as contracts.
- **No multi-paragraph Javadoc.** One sentence or a short `<h3>` block.
- **`final` classes** by default; package-private by default.

## Commit Messages

One-line summary in imperative mood, under 72 characters:

```
add RocksDB-backed PD state machine
fix region epoch dominance check for split apply
```

If needed, add a blank line and a body explaining *why*, not *what*:

```
fix region epoch dominance check for split apply

The previous check compared (confVer, version) independently. A split
bumps version but not confVer, so a heartbeat from the pre-split epoch
could overwrite the post-split range index.
```

## Pull Request Workflow

1. **Fork and branch.** Branch from `main`. Name branches descriptively:
   `feat/auto-split`, `fix/tso-monotonicity`, `test/linearizability`.

2. **Keep PRs focused.** One logical change per PR. If a refactor is
   needed to support a feature, send the refactor first.

3. **Tests required.** Every behavioral change needs a test. Bug fixes
   need a test that would have caught the bug. New features need E2E
   coverage in the `tests/` module.

4. **All tests must pass.** `mvn test` green is the merge gate.

5. **No force-push after review.** Add fixup commits; we squash on merge.

## Architecture

Read [docs/architecture.md](./docs/architecture.md) and [docs/design.md](./docs/design.md) for:

- The six load-bearing invariants (Inv-1 through Inv-6)
- Module dependency graph
- Storage engine, Raft integration, MVCC/Percolator, PD, client SDK design

The invariants are the most important thing to understand before making
changes to the storage engine, raft apply loop, transaction protocol,
or PD state machine.

## Reporting Issues

Open an issue on GitHub with:

1. **What you expected** vs **what happened**
2. **Steps to reproduce** (ideally a failing test case)
3. **Environment** (JDK version, OS, Maven version)
4. **Logs** (set `logback` to DEBUG for the relevant package)

For security issues, please email the maintainers directly instead of
opening a public issue.
