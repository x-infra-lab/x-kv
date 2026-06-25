# Region 全生命周期

本文档梳理 x-kv 系统中一个 Region 从诞生到销毁的完整生命周期。

```
┌──────────────────────────────────────────────────────────────────────────────┐
│                          Region 全生命周期总览                                 │
│                                                                              │
│  ① Bootstrap 创建 → ② 新 Store 加入（空启动）→ ③ 副本扩展 (ChangePeer)        │
│  → ④ 按需创建 Peer (spawnOnDemand) → ⑤ Region 心跳上报                       │
│  → ⑥ Leader 选举与转移 → ⑦ Raft 日志压缩 → ⑧ 快照与追赶                      │
│  → ⑨ Region Split → ⑩ Region Balance → ⑪ Region Merge                       │
│  → ⑫ 热点调度 → ⑬ Peer 销毁与下线 → ⑭ Graceful Drain / Shutdown             │
│  → ⑮ Store 重启与 Region 恢复                                                │
└──────────────────────────────────────────────────────────────────────────────┘
```

---

## ① 集群 Bootstrap — 第一个 Region 的诞生

**触发时机**：第一个 KV Store 节点启动，发现 PD 集群尚未 bootstrap。

**流程**：
1. Store 调用 `checkClusterBootstrapped(pdStub)` 带重试地确认集群状态。
2. 确认未 bootstrap 后，调用 `prepareBootstrapRegion()` — 在本地 RAFT CF 中**先持久化** Region 1 描述符（防止后续崩溃丢失）。
3. 调用 `PD.Bootstrap(store, region)` 完成集群初始化。
4. 若 PD 返回 `ALREADY_BOOTSTRAPPED`（另一个 Store 抢先），则 `clearPreparedBootstrap()` 清理本地，退回 join 路径。
5. Bootstrap 成功后，从 RAFT CF 恢复并创建 Region 1 的 `RegionPeer`，启动 Raft 状态机。

**设计要点**：
- **先本地后远程**：对齐 TiKV 的 `prepare_bootstrap_cluster` 模式 — 先把 Region 持久化到本地，再调用 PD。即使 PD.Bootstrap 后 Store 崩溃，重启后仍可从 RAFT CF 恢复。
- **竞态安全**：多个 Store 同时 Bootstrap 时，只有一个能成功；其余收到 `ALREADY_BOOTSTRAPPED` 后自动退回 join。

**关键源码** — `KvServer.bootstrapOrJoin()`:
```java
private Optional<Metapb.Region> bootstrapOrJoin(PDGrpc.PDBlockingStub pdStub,
                                                 Metapb.Store storeMeta) {
    boolean bootstrapped = checkClusterBootstrapped(pdStub);

    if (bootstrapped) {
        log.info("Joining existing cluster, registering store {}", config.storeId());
        pdStub.putStore(Pdpb.PutStoreRequest.newBuilder()
                .setStore(storeMeta).build());
        return Optional.empty();   // ← 不返回任何 Region，空启动
    }

    // First store — prepare locally, then bootstrap via PD.
    var region = prepareBootstrapRegion();
    try {
        pdStub.bootstrap(Pdpb.BootstrapRequest.newBuilder()
                .setStore(storeMeta).setRegion(region).build());
    } catch (StatusRuntimeException e) {
        if (isAlreadyBootstrappedError(e)) {
            clearPreparedBootstrap();
            pdStub.putStore(...);
            return Optional.empty();
        }
        clearPreparedBootstrap();
        throw e;
    }
    return Optional.of(region);
}
```

**关键源码** — `KvServer.prepareBootstrapRegion()`:
```java
private Metapb.Region prepareBootstrapRegion() {
    var region = Metapb.Region.newBuilder()
            .setId(1)
            .setRegionEpoch(Metapb.RegionEpoch.newBuilder()
                    .setConfVer(1).setVersion(1))
            .addPeers(Metapb.Peer.newBuilder()
                    .setId(config.storeId())
                    .setStoreId(config.storeId())
                    .setRole(Metapb.PeerRole.Voter))
            .build();
    // 持久化到本地 RAFT CF
    try (var batch = engine.newWriteBatch()) {
        batch.put(StorageEngine.Cf.RAFT, RaftCfKeys.regionKey(1), region.toByteArray());
        engine.write(batch, true);
    }
    return region;
}
```

**关键源码** — `checkClusterBootstrapped()` (带重试):
```java
private boolean checkClusterBootstrapped(PDGrpc.PDBlockingStub pdStub) {
    for (int attempt = 0; attempt < BOOTSTRAP_CHECK_MAX_RETRIES; attempt++) {
        try {
            var resp = pdStub.isBootstrapped(
                    Pdpb.IsBootstrappedRequest.newBuilder().build());
            return resp.getBootstrapped();
        } catch (StatusRuntimeException e) {
            if (isTransientError(e) && attempt < BOOTSTRAP_CHECK_MAX_RETRIES - 1) {
                long backoffMs = 100L * (1L << attempt);
                Thread.sleep(backoffMs);
                continue;
            }
            throw new IllegalStateException("Cannot determine cluster state", e);
        }
    }
}
```

---

## ② 新 Store 加入（空启动）

**触发时机**：后续 KV Store 节点启动，发现 PD 已 bootstrap。

**流程**：
1. `checkClusterBootstrapped()` 返回 `true`。
2. 调用 `PD.PutStore()` 注册自身元数据（storeId、客户端地址、Raft 地址、标签）。
3. `bootstrapOrJoin()` 返回 `Optional.empty()` — **不主动从 PD 获取任何 Region**。
4. Store 以 **0 个 Region** 的状态启动，包括：
   - Raft gRPC Server（接收 Raft 消息）
   - `spawnOnDemand` handler（按需创建 Peer）
   - StoreHeartbeater（向 PD 上报容量/Region 数）
   - 客户端 gRPC Server
5. 等待 PD 调度器（`RuleCheckerScheduler` / `RegionBalanceScheduler`）发现需要给本 Store 添加副本。

**设计要点**：
- **不再从 PD 扫描 Region**：旧代码会 `getRegionByID(1)` / `scanRegions`，容易出问题（Region 1 不包含本 Store Peer、limit=100 遗漏等）。新设计完全依赖本地恢复 + 按需创建。
- **新 Store 的第一个 Region 从哪来？** → PD 调度器在心跳中发现 Region 副本不足 → 下发 `AddPeer` Operator → Leader 提议 ConfChange → Leader 发 Raft 消息给新 Store → 触发 `spawnOnDemand`。

**关键源码** — `KvServer.start()` 片段:
```java
// 3) Bootstrap or join the cluster.
Optional<Metapb.Region> bootstrapRegion = bootstrapOrJoin(pdStub, storeMeta);

// 6) Recover ALL persisted regions from RAFT CF (primary recovery path).
recoverPersistedRegions(pdStub, snapshotEngine);

// 6b) If this is the bootstrap store and no regions were recovered
//     (first-ever boot), create the initial Region 1 peer.
if (bootstrapRegion.isPresent() && peers.isEmpty()) {
    var region = bootstrapRegion.get();
    var regionPeer = createRegionPeer(region, ...);
    store.registerPeer(regionPeer);
    peers.add(regionPeer);
}

// ...

if (peers.isEmpty()) {
    log.info("KV store {} started with 0 regions, waiting for PD scheduling");
} else {
    log.info("KV store {} started: regions={}", peers.size());
}
```

---

## ③ 副本扩展 (ChangePeer) — 多 Store 加入 Region

**触发时机**：PD 调度器（`RuleCheckerScheduler` / `RegionBalanceScheduler`）发现 Region 副本数不足（低于 `maxPeerCount`），或需要平衡各 Store 的 Region 数。

**流程**：
1. **PD 调度**：调度器产出 `AddNode` Operator（含 `AddPeerStep`），通过心跳响应流下发给 Region Leader。
2. **Leader 收到指令**：`RegionHeartbeater.dispatchOperator()` 解析 `change_peer_v2` 字段，调用 `proposeChangePeers()`。
3. **Raft ConfChange**：Leader 将 `ConfChangeV2` 提议通过 Raft 复制到多数派。
4. **Apply**：`applyConfChangeOne()` 更新 Region 描述符（新增 Peer、`conf_ver++`），触发 `ChangePeerObserver`。
5. **目标 Store 生成 Peer**：Leader 的 Raft 消息（`MsgAppend`）到达目标 Store → 触发 `RaftMessageDispatcher` 的 `MissingHandler` → 按需创建 `RegionPeer`（见阶段④）。
6. **PD 完成确认**：下一次心跳时 `OperatorControllerImpl.dispatch()` 调用 `AddPeerStep.satisfied(hb)` 检查新 Peer 是否出现在 Region 的 Peer 列表中 → 标记 Operator 完成。

**关键源码** — `RegionHeartbeater.proposeChangePeers()`:
```java
private void proposeChangePeers(List<Pdpb.ChangePeer> changes) {
    var v2 = Eraftpb.ConfChangeV2.newBuilder();
    var ctx = KvServerpb.ConfChangeContext.newBuilder();
    for (var cp : changes) {
        v2.addChanges(Eraftpb.ConfChangeSingle.newBuilder()
                .setType(mapConfChangeType(cp.getChangeType()))
                .setNodeId(cp.getPeer().getId()));
        ctx.addPeers(cp.getPeer());
    }
    v2.setContext(ByteString.copyFrom(ctx.build().toByteArray()));
    peer.proposeConfChange(v2.build());
}
```

**关键源码** — `RegionPeerImpl.applyConfChangeOne()`:
```java
switch (ch.getType()) {
    case ConfChangeAddNode -> peerMap.put(pe.getId(),
            pe.toBuilder().setRole(Metapb.PeerRole.Voter).build());
    case ConfChangeAddLearnerNode -> peerMap.put(pe.getId(),
            pe.toBuilder().setRole(Metapb.PeerRole.Learner).build());
    case ConfChangeRemoveNode -> peerMap.remove(ch.getNodeId());
}
// ConfChange bumps conf_ver
b.setRegionEpoch(region.getRegionEpoch().toBuilder()
        .setConfVer(region.getRegionEpoch().getConfVer() + 1));
```

**关键源码** — PD 侧 `RuleCheckerScheduler`（添加副本）:
```java
long newPeerId = state.allocId(1);
var newPeer = Metapb.Peer.newBuilder()
        .setId(newPeerId).setStoreId(bestStore)
        .setRole(Metapb.PeerRole.Voter).build();
var resp = Pdpb.RegionHeartbeatResponse.newBuilder()
        .setRegionId(region.getId())
        .addChangePeerV2(Pdpb.ChangePeer.newBuilder()
                .setPeer(newPeer).setChangeType(Pdpb.ConfChangeType.AddNode))
        .build();
var op = new SimpleOperator(System.nanoTime(), region.getId(),
        Operator.Kind.RULE_FIX,
        "rule-checker: add peer on store " + bestStore,
        resp, Set.of(bestStore),
        List.of(new OperatorSteps.AddPeerStep(newPeer)),
        Operator.PRIORITY_RULE_FIX);
controller.addOperator(op);
```

---

## ④ 按需创建 Peer (On-Demand Spawn)

**触发时机**：Raft Leader 向目标 Store 发送消息（如 `MsgAppend`），但目标 Store 尚未持有该 Region 的 Peer。

**流程**：
1. `RaftMessageDispatcher.deliver()` 找不到目标 Region 的 Transport，触发 `MissingHandler`。
2. Handler 异步启动 `spawnOnDemand(regionId)` 线程。
3. 查询 PD 获取 Region 描述符，确认自身在 Peer 列表中。
4. 调用 `spawnChildPeer()` 创建新的 `BatchRegionPeer`，注册到本地 Store。
5. 新 Peer 通过 Raft 日志复制或 Snapshot 追赶 Leader 进度。
6. 子 Peer 使用 Store 级别的 `storeCm`（ConcurrencyManager），保证跨 Region 时间戳一致性。

**关键源码** — `RaftMessageDispatcher.deliver()`:
```java
public void deliver(long regionId, Eraftpb.Message msg) {
    var t = byRegion.get(regionId);
    if (t != null) { t.deliver(msg); return; }
    // 没有本地 transport → 触发按需创建
    var h = missingHandler;
    if (h != null && spawnInFlight.putIfAbsent(regionId, Boolean.TRUE) == null) {
        h.onMissing(regionId, msg);
    }
}
```

**关键源码** — `KvServer.spawnOnDemand()`:
```java
private void spawnOnDemand(long regionId) {
    var resp = pdManager.blockingStub().getRegionByID(...);
    if (!resp.hasRegion()) return;
    var regionDesc = resp.getRegion();
    var selfPeer = regionDesc.getPeersList().stream()
            .filter(p -> p.getStoreId() == config.storeId())
            .findFirst().orElse(null);
    if (selfPeer == null) return;
    if (store.peerForRegion(regionId).isPresent()) return;  // 已存在则跳过

    var peerAddrs = resolvePeerAddresses(pdManager.blockingStub(), regionDesc);
    var snapshotEngine = new SnapshotEngineImpl(engine, config.dataDir().resolve("snap"));
    spawnChildPeer(regionDesc, selfPeer, peerAddrs, snapshotEngine);
}
```

---

## ⑤ Region 心跳上报

**触发时机**：`RegionHeartbeater` 周期性触发（仅 Leader 发送，默认 500ms）。

**流程**：
1. Leader Peer 定期发送 `RegionHeartbeatRequest`，携带 Region 描述符、Leader 信息、`approximate_size`。
2. PD 接收后：
   - 通过 Raft 复制 `updateRegion`（Region 描述符变更时）
   - 更新 RegionStats（approximate_size / keys）
   - 更新 Leader 路由信息
3. PD 调用 `OperatorControllerImpl.dispatch(hb)` — 检查是否有该 Region 的 in-flight Operator：
   - 若有，调用 `operator.observe(hb)` 检查各 Step 是否完成（FINISHED / FAILED / PENDING）
   - PENDING 时返回 `operator.next(hb)`（即预构建的心跳响应）给 Leader 继续执行
   - FINISHED/FAILED 时移除 Operator 并记入历史
4. `RegionHeartbeater.dispatchOperator()` 分发执行各调度指令。

**关键源码** — `RegionHeartbeater.tick()`:
```java
private void tick() {
    if (closed || peer.isDestroyed()) return;
    if (!peer.isLeader()) return;

    long approxSize = engine.approximateSize(StorageEngine.Cf.DEFAULT,
            region.getStartKey().toByteArray(),
            region.getEndKey().toByteArray());

    outbound.onNext(Pdpb.RegionHeartbeatRequest.newBuilder()
            .setRegion(peer.region())
            .setLeader(peer.self())
            .setApproximateSize(approxSize)
            .build());
}
```

**关键源码** — `RegionHeartbeater.dispatchOperator()`:
```java
private void dispatchOperator(Pdpb.RegionHeartbeatResponse resp) {
    // 1. TransferLeader
    if (resp.hasTransferLeader()) { peer.transferLeader(target); }
    // 2. SplitRegion
    if (resp.hasSplitRegion() && splitTrigger != null) { splitTrigger.split(peer, keys); }
    // 3. ChangePeer (v2 or legacy)
    if (!resp.getChangePeerV2List().isEmpty()) { proposeChangePeers(changes); }
}
```

**关键源码** — `OperatorControllerImpl.dispatch()`:
```java
public synchronized Optional<Pdpb.RegionHeartbeatResponse> dispatch(Pdpb.RegionHeartbeatRequest hb) {
    long regionId = hb.hasRegion() ? hb.getRegion().getId() : 0L;
    if (regionId == 0) return Optional.empty();

    evictExpired();

    var record = inFlight.get(regionId);
    if (record == null) return Optional.empty();

    var outcome = record.operator().observe(hb);
    switch (outcome) {
        case FINISHED -> {
            doRemoveOperator(regionId, record);
            addHistory(record.operator());
            opsSuccess.incrementAndGet();
            return Optional.empty();
        }
        case FAILED -> {
            doRemoveOperator(regionId, record);
            addHistory(record.operator());
            opsFailed.incrementAndGet();
            return Optional.empty();
        }
        case PENDING -> {
            return Optional.of(record.operator().next(hb));
        }
    }
    return Optional.empty();
}
```

---

## ⑥ Leader 选举与转移 (Leader Transfer)

**触发时机**：
- Raft 自动选举（Leader 宕机、网络分区恢复后）
- PD `LeaderBalanceScheduler`（各 Store 间 Leader 数不均衡）
- PD `HotRegionScheduler`（热点 Region Leader 迁移）
- Graceful Drain（节点下线前主动转移 Leader）

**流程**：
1. PD 调度器在心跳响应中下发 `transfer_leader` 指令，指定目标 Peer。
2. `RegionHeartbeater.dispatchOperator()` 调用 `peer.transferLeader(targetPeerId)`。
3. Raft 库发送 `MsgTimeoutNow` 给目标 Peer，目标立即发起选举。
4. PD 通过 `TransferLeaderStep.satisfied(hb)` 确认 Leader 已转移：检查心跳中的 Leader 是否为目标 Peer。

**关键源码** — `LeaderBalanceScheduler.runOnce()`:
```java
var resp = Pdpb.RegionHeartbeatResponse.newBuilder()
        .setRegionId(move.region().getId())
        .setTransferLeader(target)
        .build();
var op = new SimpleOperator(
        System.nanoTime(), move.region().getId(), Operator.Kind.BALANCE_LEADER,
        "leader-balance: transfer leader to store " + target.getStoreId(),
        resp, Set.of(target.getStoreId()),
        List.of(new OperatorSteps.TransferLeaderStep(target)),
        Operator.PRIORITY_BALANCE);
controller.addOperator(op);
```

---

## ⑦ Raft 日志压缩 (Log Compaction)

**触发时机**：`LogCompactionWorker` 周期检查，当 `applied_index - first_index > gapThreshold`。

**流程**：
1. Worker 计算 `compactTarget = applied_index - safetyMargin`。
2. 在压缩前先生成快照（`maybeGenerateSnapshot`），确保慢 Follower 仍可追赶。
3. Leader 提议 `ADMIN_COMPACT_LOG` 通过 Raft 复制。
4. Apply 时调用 `raftEngine.compactLog(targetIndex)`，删除 `[firstIndex, targetIndex]` 范围的日志条目。

**关键源码** — `AdminApplyHandler.applyCompactLog()`:
```java
private Result applyCompactLog(byte[] payload, StorageEngine.WriteBatch batch) {
    long targetIndex = ByteBuffer.wrap(payload).getLong();
    if (targetIndex >= applied) return Result.err("compact target >= applied_index");
    raftEngine.compactLog(targetIndex, batch);
}
```

**关键源码** — `PerRegionRaftEngine.compactLog()`:
```java
public void compactLog(long uptoIndex, StorageEngine.WriteBatch batch) {
    batch.deleteRange(StorageEngine.Cf.RAFT,
            logKey(regionId, firstIndex),
            logKey(regionId, uptoIndex + 1));
    this.firstIndex = uptoIndex + 1;
}
```

---

## ⑧ Raft 快照与 Follower 追赶 (Snapshot & Catch-up)

**触发时机**：Follower 的 `nextIndex < Leader.firstIndex`（日志已被压缩，无法通过日志追赶）。

**流程**：
1. Leader 的 Raft 库调用 `Storage.snapshot()`，获取预先生成的快照。
2. 快照包含完整的用户数据（default/lock/write 三个 CF）+ Raft 元数据。
3. Leader 通过 `MsgSnapshot` 发送给 Follower。
4. Follower 的 `RegionRaftStorage.applySnapshot()` 安装快照：先写入用户数据 CF，再更新 Raft 元数据。

**关键源码** — `RegionRaftStorage.applySnapshot()`:
```java
public void applySnapshot(Eraftpb.Snapshot snap) throws RaftException {
    // Step 1: install user-data CFs from snapshot
    byte[] dataBytes = snap.getData().toByteArray();
    if (snapshotEngine != null && dataBytes.length > 0) {
        var chunks = decodeChunkEnvelope(dataBytes);
        snapshotEngine.receiveAndInstall(regionId(), chunks);
    }
    // Step 2: install raft meta (applied index, conf state, etc.)
    try (var b = storage.newWriteBatch()) {
        b.deleteRange(StorageEngine.Cf.RAFT, logKey(regionId(), 0), logKey(regionId(), Long.MAX_VALUE));
        raft.saveSnapshotMeta(...);
        raft.saveAppliedIndex(snapIndex, b);
        storage.write(b, true);
    }
}
```

---

## ⑨ Region Split — 数据增长触发分裂

**触发时机**：Region 的 `approximate_size` 超过 PD 配置的 `regionSplitBytes` 阈值。

**流程概览**：
```
PD SplitCheckerScheduler 检测 → 心跳响应下发 APPROXIMATE split 指令
→ Leader 计算中点 key → SplitDriver 向 PD 申请新 ID
→ 构建 SplitRegionProposal → Raft propose ADMIN_SPLIT
→ Apply: 原子持久化 parent + children 描述符
→ SplitObserver: 刷新 parent 描述符 + 为每个 child 创建新 RegionPeer
→ 下一次心跳 PD 通过 SplitRegionStep 检查 epoch.version 是否已 bump → 标记完成
```

### 9.1 PD 侧 — SplitCheckerScheduler

```java
for (var entry : state.allRegionStats().entrySet()) {
    long regionId = entry.getKey();
    var stats = entry.getValue();
    if (stats.approximateSize() < splitThresholdBytes) continue;
    if (controller.getOperator(regionId).isPresent()) continue;

    var sr = Pdpb.SplitRegion.newBuilder()
            .setPolicy(Pdpb.SplitRegion.Policy.APPROXIMATE).build();
    var resp = Pdpb.RegionHeartbeatResponse.newBuilder()
            .setRegionId(regionId).setSplitRegion(sr).build();
    long currentVersion = region.getRegionEpoch().getVersion();
    var op = new SimpleOperator(System.nanoTime(), regionId, Operator.Kind.SPLIT,
            "split-checker: approxSize=" + stats.approximateSize(),
            resp, storeIds,
            List.of(new OperatorSteps.SplitRegionStep(currentVersion + 1)),
            Operator.PRIORITY_DEFAULT);
    controller.addOperator(op);
}
```

### 9.2 Leader 侧 — 收到 split 指令并计算中点

```java
// RegionHeartbeater.dispatchOperator()
if (resp.hasSplitRegion() && splitTrigger != null && peer.isLeader()) {
    var sr = resp.getSplitRegion();
    if (keys.isEmpty() && sr.getPolicy() == APPROXIMATE) {
        byte[] mid = computeApproximateMidKey(peer.region());
        if (mid != null) keys.add(mid);
    }
    if (!keys.isEmpty()) splitTrigger.split(peer, keys);
}
```

### 9.3 SplitDriver — 向 PD 申请 ID 并构建 Proposal

```java
// 1) PD 分配新 region_id 和 peer_id
var idsResp = pd.askBatchSplit(AskBatchSplitRequest.newBuilder()
        .setRegion(parent).setSplitCount(splitKeys.size()).build());

// 2) 构建 proposal: 缩小 parent + 创建 children
//    parent: [start, splitKey)   child: [splitKey, end)
//    所有 region 的 epoch.version 都 bump

// 3) Raft propose ADMIN_SPLIT
var envelope = ProposalCodec.encode(ProposalCodec.Kind.ADMIN_SPLIT, 0, proposal.toByteArray());
parentPeer.propose(new RegionPeer.Proposal(envelope, 0, 0));
```

### 9.4 Apply 侧 — 原子持久化

```java
// AdminApplyHandler.applySplit()
raftEngine.saveRegion(req.getUpdatedParent(), batch);   // parent 缩小范围
for (var child : req.getChildrenList()) {
    batch.put(StorageEngine.Cf.RAFT, regionKey(child.getId()), child.toByteArray());
}
// postFlush: 触发 SplitObserver
splitObserver.onSplit(parent, children);
```

### 9.5 Store 侧 — 生成子 Peer

```java
// SplitObserver callback
(updatedParent, children) -> {
    peerHolder.updateRegion(updatedParent);
    for (var child : children) {
        var childSelf = child.getPeersList().stream()
                .filter(pe -> pe.getStoreId() == config.storeId()).findFirst();
        if (childSelf.isPresent()) {
            spawnChildPeer(child, childSelf.get(), peerAddrs, snapshotEngine);
        }
    }
};
```

> **重要**：Split 是**纯元数据操作**，不移动任何数据。子 Region 的数据已经在同一个 RocksDB 引擎中（key 范围是不相交的子集）。子 Peer 使用 Store 级别的 `storeCm` 保证时间戳一致性。

---

## ⑩ Region Balance — 跨 Store 迁移副本

**触发时机**：`RegionBalanceScheduler` 检测到 Store 间 Region 数差异超过阈值。

**流程**：
1. PD 为负载最少的 Store 上的 Region 生成 `AddPeer` Operator（含 `AddPeerStep`，优先级 `PRIORITY_BALANCE`）。
2. 通过心跳下发 → Leader 提议 `ConfChangeAddNode` → Apply 后目标 Store 按需创建 Peer。
3. 后续再通过 `RemovePeer` 从负载最高的 Store 移除旧副本。

**关键源码** — `RegionBalanceScheduler.runOnce()`:
```java
long newPeerId = state.allocId(1);
var newPeer = Metapb.Peer.newBuilder()
        .setId(newPeerId).setStoreId(minStore).setRole(Metapb.PeerRole.Voter).build();
var resp = Pdpb.RegionHeartbeatResponse.newBuilder()
        .setRegionId(target.getId())
        .addChangePeerV2(Pdpb.ChangePeer.newBuilder()
                .setPeer(newPeer).setChangeType(Pdpb.ConfChangeType.AddNode))
        .build();
var op = new SimpleOperator(System.nanoTime(), target.getId(), Operator.Kind.BALANCE_REGION,
        "region-balance: add peer on store " + minStore,
        resp, Set.of(minStore),
        List.of(new OperatorSteps.AddPeerStep(newPeer)),
        Operator.PRIORITY_BALANCE);
controller.addOperator(op);
```

---

## ⑪ Region Merge — 数据减少后合并

**触发时机**：大量 key 删除后，相邻 Region 的 `approximate_size` 都低于 `mergeThresholdBytes`。

**流程概览**：
```
PD MergeCheckerScheduler 检测相邻小 Region → 心跳下发 Merge 指令
→ MergeDriver 3 阶段协议:
  Phase 1: PrepareMerge on SOURCE → 冻结写入
  Phase 2: CommitMerge on TARGET → 吸收 source range
  Phase 3: Finalize → 销毁 source peer
```

### 11.1 PD 侧 — MergeCheckerScheduler

```java
for (var region : state.allRegions()) {
    if (stats.approximateSize() >= mergeThresholdBytes) continue;
    // 查找右邻居 (startKey == thisRegion.endKey)
    var neighbor = byStartKey.get(region.getEndKey());
    if (neighbor == null) continue;
    if (neighborStats.approximateSize() >= mergeThresholdBytes) continue;
    if (!sameStores(region, neighbor)) continue;

    var resp = Pdpb.RegionHeartbeatResponse.newBuilder()
            .setRegionId(region.getId())
            .setMerge(Pdpb.Merge.newBuilder().setTarget(neighbor))
            .build();
    var op = new SimpleOperator(System.nanoTime(), region.getId(), Operator.Kind.MERGE,
            "merge-checker: merge region " + region.getId() + " into " + neighbor.getId(),
            resp, storeIds,
            List.of(new OperatorSteps.MergeRegionStep(region.getId())),
            Operator.PRIORITY_MERGE);
    controller.addOperator(op);
}
```

### 11.2 MergeDriver — 3 阶段协议

```java
// === Phase 1: PrepareMerge on SOURCE ===
// 冻结 source 的业务写入
var prepare = KvServerpb.PrepareMergeProposal.newBuilder().setTarget(t).build();
source.propose(ProposalCodec.encode(ADMIN_PREPARE_MERGE, ...));

// === Phase 2: CommitMerge on TARGET ===
// target 吸收 source 的 key 范围
var mergedTarget = t.toBuilder()
        .setStartKey(left.getStartKey())
        .setEndKey(right.getEndKey())
        .setRegionEpoch(t.getRegionEpoch().toBuilder().setVersion(bumpedVersion))
        .build();
target.propose(ProposalCodec.encode(ADMIN_COMMIT_MERGE, ...));
```

### 11.3 Apply 侧 — 原子持久化

```java
// AdminApplyHandler.applyCommitMerge()
raftEngine.saveRegion(req.getMergedTarget(), batch);     // target 新范围
batch.delete(StorageEngine.Cf.RAFT, regionKey(req.getSourceRegion().getId()));
// postFlush: MergeObserver
mergeObserver.onMerge(mergedTarget, sourceRegion);
```

### 11.4 Merge 安全保证

- **写冻结**：PrepareMerge 后 source 拒绝所有业务写入：
```java
if (raftEngine.isMerging() && !isMergeAdminKind(decoded.kind())) {
    // 拒绝写入："region merging — write rejected"
}
```

- **回滚保护**：Rollback 前必须检查 target epoch：
```java
long targetVersionNow = resp.getRegion().getRegionEpoch().getVersion();
if (targetVersionNow > targetVersionAtPrepare) {
    throw t2; // target 已提交 merge, 禁止回滚！
}
source.propose(ADMIN_ROLLBACK_MERGE);
```

> **重要**：与 Split 相同，Merge 也是**纯元数据操作**。合并只需更新 target 的 key 范围描述符并删除 source 的描述符。

---

## ⑫ 热点调度 (Hot Region Scheduling)

**触发时机**：`HotRegionScheduler` 检测到某些 Region 的负载超过集群平均值 2 倍。

**流程**：
1. 统计各 Store 上的热点 Region Leader 数。
2. 从热点最密集的 Store 选择一个热点 Region。
3. 将其 Leader 转移到热点最少的 Follower 所在的 Store。
4. 通过 `TransferLeaderStep` 追踪完成情况。

**关键源码** — `HotRegionScheduler.runOnce()`:
```java
if (controller.getOperator(region.getId()).isPresent()) continue;

// 找该 region 的 follower 中 hotCount 最少的 peer 作为 target
var resp = Pdpb.RegionHeartbeatResponse.newBuilder()
        .setRegionId(region.getId())
        .setTransferLeader(bestTarget)
        .build();
var op = new SimpleOperator(
        System.nanoTime(), region.getId(), Operator.Kind.HOT_REGION,
        "hot-region: transfer leader from store " + leaderStore + " to " + bestTarget.getStoreId(),
        resp, Set.of(bestTarget.getStoreId()),
        List.of(new OperatorSteps.TransferLeaderStep(bestTarget)),
        Operator.PRIORITY_HOT_REGION);
controller.addOperator(op);
```

---

## ⑬ Peer 销毁与下线

**触发时机**：
- `ConfChange RemoveNode`（PD 调度移除多余副本，或 Store 下线/Tombstone）
- Merge 后 source peer 被销毁
- Store 节点宕机后 PD 标记为 Down/Tombstone

**流程**：
1. PD `RuleCheckerScheduler` 检测到 Down/Tombstone 的 Store，生成 `RemoveNode` Operator（含 `RemovePeerStep`，优先级 `PRIORITY_RULE_FIX`）。
2. Leader 提议 `ConfChange RemoveNode` → Apply 后 peer 列表缩减，`conf_ver++`。
3. `ChangePeerObserver` 通知 Store 销毁对应 Peer。
4. PD 通过 `RemovePeerStep.satisfied(hb)` 确认目标 Peer 已从列表中消失 → 标记完成。

**关键源码** — `StoreImpl.destroyPeer()`:
```java
public void destroyPeer(long regionId) {
    var peer = byId.remove(regionId);
    if (peer != null) {
        byStartKey.remove(peer.region().getStartKey().toByteArray());
        peer.shutdown();
    }
}
```

**关键源码** — `PerRegionRaftEngine.destroy()`:
```java
public void destroy() {
    try (var b = storage.newWriteBatch()) {
        for (byte type : new byte[] {
                TYPE_LOG, TYPE_META, TYPE_APPLIED, TYPE_DEDUP,
                TYPE_SNAPMETA, TYPE_CONFSTATE, TYPE_REGION,
                TYPE_MAX_TS, TYPE_MERGE_STATE }) {
            b.deleteRange(StorageEngine.Cf.RAFT,
                    regionTypePrefix(regionId, type),
                    regionTypePrefix(regionId + 1, type));
        }
        storage.write(b, true);
    }
}
```

**关键源码** — `RuleCheckerScheduler`（清理 Down/Tombstone）:
```java
if (storeState == Metapb.StoreState.Down || storeState == Metapb.StoreState.Tombstone) {
    var resp = Pdpb.RegionHeartbeatResponse.newBuilder()
            .setRegionId(region.getId())
            .addChangePeerV2(Pdpb.ChangePeer.newBuilder()
                    .setPeer(peer).setChangeType(Pdpb.ConfChangeType.RemoveNode))
            .build();
    var op = new SimpleOperator(System.nanoTime(), region.getId(), Operator.Kind.RULE_FIX,
            "rule-checker: remove " + storeState + " peer " + peer.getId(),
            resp, Set.of(peer.getStoreId()),
            List.of(new OperatorSteps.RemovePeerStep(peer)),
            Operator.PRIORITY_RULE_FIX);
    controller.addOperator(op);
}
```

---

## ⑭ Graceful Drain & Shutdown

**触发时机**：KV Store 节点计划下线（运维操作）。

**流程**：
1. **Drain**：
   - `DrainingInterceptor` 拒绝新客户端请求
   - 通知 PD 标记本 Store 为 `Offline`
   - 为本节点上的每个 Leader Region 发起 `transferLeader`，等待所有 Leader 迁走
2. **Shutdown**（顺序）：
   - 停止 StoreHeartbeater + 所有 RegionHeartbeater
   - 停止 CDC Service
   - 停止后台 Worker（LogCompaction、GC）
   - 关闭 gRPC 服务器（客户端 + Raft）
   - 停止所有 RegionPeer
   - 停止 BatchSystem（ApplyWorker → RaftPoller → TickDriver）
   - 关闭 PD 连接
   - 关闭 RocksDB 存储引擎

**关键源码** — `KvServer.drain()`:
```java
public void drain() {
    drainingInterceptor.startDraining();

    // Notify PD: mark store as Offline
    pdManager.blockingStub().putStore(Pdpb.PutStoreRequest.newBuilder()
            .setStore(offlineStore).build());

    // Transfer leadership of all leader regions
    for (var peer : peers) {
        if (!peer.isLeader()) continue;
        var otherPeer = peer.region().getPeersList().stream()
                .filter(p -> p.getStoreId() != config.storeId())
                .findFirst().orElse(null);
        if (otherPeer != null) peer.transferLeader(otherPeer.getId());
    }

    // Wait for leadership to migrate away (with timeout)
    while (System.currentTimeMillis() < deadline) {
        if (peers.stream().noneMatch(RegionPeer::isLeader)) break;
        Thread.sleep(100);
    }
}
```

---

## ⑮ Store 重启与 Region 恢复

**触发时机**：KV Store 进程重启（正常或崩溃恢复）。

**流程**：
1. 打开本地 RocksDB 存储引擎。
2. 调用 `bootstrapOrJoin()` — 由于集群已 bootstrap，走 join 路径 → `putStore()` + 返回 empty。
3. **主恢复路径** `recoverPersistedRegions()`：扫描本地 RAFT CF 中所有 Region 描述符。
4. 对每个找到的 Region：检查 peer 列表中是否包含自身 → 创建 `RegionPeer`。
5. 构建 store-level `ConcurrencyManager`（取所有 Region 的 maxTs 中的最大值）。
6. 启动所有 Heartbeater，恢复 Raft 通信。

**设计要点**：
- **恢复不依赖 PD**：Region 信息从本地 RAFT CF 恢复，不需要从 PD 查询"我有哪些 Region"。
- **与 TiKV 对齐**：TiKV 同样在启动时扫描本地 RocksDB 中的 Region 描述符来恢复所有 Peer。

**关键源码** — `KvServer.recoverPersistedRegions()`:
```java
private void recoverPersistedRegions(PDGrpc.PDBlockingStub pdStub,
                                     SnapshotEngineImpl snapshotEngine) {
    byte[] prefix = RaftCfKeys.allRegionKeysPrefix();
    byte[] end = RaftCfKeys.allRegionKeysEnd();
    try (var ro = engine.newReadOptions()
            .iterateLowerBound(prefix).iterateUpperBound(end);
         var it = engine.newIterator(StorageEngine.Cf.RAFT, ro)) {
        for (it.seek(prefix); it.isValid(); it.next()) {
            long regionId = RaftCfKeys.regionIdFromKey(it.key());
            if (store.peerForRegion(regionId).isPresent()) continue;

            Metapb.Region region = Metapb.Region.parseFrom(it.value());
            var selfPeer = region.getPeersList().stream()
                    .filter(p -> p.getStoreId() == config.storeId())
                    .findFirst().orElse(null);
            if (selfPeer == null) continue;

            var peerAddrs = resolvePeerAddresses(pdStub, region);
            var peer = createRegionPeer(region, peerAddrs, snapshotEngine, ...);
            store.registerPeer(peer);
            peers.add(peer);
        }
    }
}
```

---

## RegionEpoch 版本控制

贯穿整个生命周期的核心机制：

| 字段 | 变更时机 | 作用 |
|------|---------|------|
| `conf_ver` | ConfChange（AddNode/RemoveNode） | 防止过期的成员变更操作 |
| `version` | Split / Merge | 防止过期的 range 操作（读写路由到已过期的 range） |

---

## PD 调度器一览

PD Leader 启动时注册以下调度器，它们共同驱动 Region 生命周期的各阶段：

| 调度器 | 周期 | 每轮上限 | 驱动的生命周期阶段 |
|--------|------|---------|------------------|
| **LeaderBalanceScheduler** | 5s | 4 | ⑥ Leader Transfer |
| **RegionBalanceScheduler** | 10s | 4 | ⑩ Region Balance |
| **HotRegionScheduler** | 5s | 2 | ⑫ 热点调度 |
| **SplitCheckerScheduler** | 心跳驱动 | — | ⑨ Region Split |
| **MergeCheckerScheduler** | 10s | 2 | ⑪ Region Merge |
| **RuleCheckerScheduler** | 10s | 4 | ③ 副本扩展 / ⑬ Peer 销毁 |

---

## Operator 调度通道

所有调度指令通过统一的 Operator 机制下发。旧的 `OperatorQueue`（纯 FIFO 缓冲）已被移除，`OperatorControllerImpl` 现在直接管理 Operator 的全生命周期：

```
Scheduler → SimpleOperator(steps, priority)
    → OperatorControllerImpl.addOperator() ← per-store 并发控制 + 优先级抢占
    → dispatch(heartbeat):
        ├─ observe(hb) → Step.satisfied()? → PENDING: 返回 next(hb) 给 Leader
        ├─ 所有 Step 完成 → FINISHED → 移入 history
        └─ 超时/失败 → TIMEOUT/FAILED → 移入 history
    → RegionHeartbeatResponse → RegionHeartbeater.dispatchOperator()
    → peer.transferLeader() / peer.proposeConfChange() / splitTrigger.split() / ...
```

### Operator 优先级

高优先级 Operator 可以抢占同 Region 的低优先级 Operator：

| 优先级 | 值 | 使用场景 |
|--------|---|---------|
| `PRIORITY_DEFAULT` | 0 | SplitChecker（自动 split） |
| `PRIORITY_MERGE` | 5 | MergeChecker |
| `PRIORITY_BALANCE` | 10 | LeaderBalance / RegionBalance |
| `PRIORITY_HOT_REGION` | 30 | HotRegionScheduler |
| `PRIORITY_RULE_FIX` | 50 | RuleChecker（副本修复） |
| `PRIORITY_ADMIN` | 100 | Admin RPC（手动 split 等） |

### OperatorSteps — 完成条件检查

每个 Operator 由一组 Step 组成，Step 按顺序执行，通过心跳确认完成：

| Step 类型 | 完成条件 (satisfied) |
|----------|---------------------|
| `AddPeerStep` | 目标 Peer 出现在心跳 Region 的 Peer 列表中 |
| `AddLearnerStep` | 同 AddPeerStep |
| `RemovePeerStep` | 目标 Peer 不在心跳 Region 的 Peer 列表中 |
| `TransferLeaderStep` | 心跳中的 Leader 为目标 Peer |
| `PromoteLearnerStep` | 目标 Peer 在列表中且 Role == Voter |
| `SplitRegionStep` | 心跳 Region 的 epoch.version >= 期望值 |
| `MergeRegionStep` | 需外部确认（始终返回 false，由超时驱动） |

```java
// SimpleOperator.observe() — 顺序检查所有 Step
public Outcome observe(Pdpb.RegionHeartbeatRequest hb) {
    if (steps.isEmpty()) return Outcome.FINISHED;
    while (currentStep < steps.size()) {
        if (steps.get(currentStep).satisfied(hb)) {
            currentStep++;
        } else {
            return Outcome.PENDING;
        }
    }
    return Outcome.FINISHED;
}
```

### OperatorControllerImpl — 优先级抢占

```java
// addOperator() — 高优先级可替换低优先级
var existing = inFlight.get(regionId);
if (existing != null) {
    if (op.priority() > existing.operator().priority()) {
        doRemoveOperator(regionId, existing);
        opsReplaced.incrementAndGet();
    } else {
        return false;  // 拒绝：低优先级无法替换
    }
}
```

### 诊断指标

`OperatorControllerImpl` 内建操作计数器，支持运维观测：

| 指标 | 含义 |
|------|------|
| `opsCreated` | 成功添加的 Operator 总数 |
| `opsSuccess` | 所有 Step 完成（FINISHED）的 Operator 数 |
| `opsFailed` | 失败（FAILED）的 Operator 数 |
| `opsTimeout` | 超时驱逐的 Operator 数 |
| `opsReplaced` | 被高优先级抢占的 Operator 数 |

---

## Store 级别 ConcurrencyManager

Store 启动时从所有 Region 聚合全局 maxTs：

```java
// 7) Build store-level ConcurrencyManager from max across all regions.
long globalMaxTs = 0;
for (var peer : peers) {
    var re = new PerRegionRaftEngine(engine, peer.regionId());
    globalMaxTs = Math.max(globalMaxTs, re.persistedMaxTs());
}
storeCm = new ConcurrencyManager(new MaxTsTracker(globalMaxTs));
```

`storeCm` 被 `TransactionService` 和 `CdcService` 共享，保证跨 Region 事务的时间戳一致性。新创建的子 Peer（split/spawnOnDemand）也使用 `storeCm`：

```java
// spawnChildPeer: 使用 store-level CM
var childCm = storeCm != null ? storeCm
        : new ConcurrencyManager(new MaxTsTracker(childRaftEngine.persistedMaxTs()));
```

---

## 关键源码文件索引

| 生命周期阶段 | 核心源码文件 |
|------------|------------|
| Bootstrap | `KvServer.java` (`bootstrapOrJoin`, `prepareBootstrapRegion`, `checkClusterBootstrapped`, `clearPreparedBootstrap`) |
| Store 注册 | `KvServer.java`, `PdServiceImpl.java` |
| 副本扩展 | `RegionHeartbeater.java`, `RegionPeerImpl.java`, `RuleCheckerScheduler.java` |
| 按需创建 Peer | `RaftMessageDispatcher.java`, `KvServer.spawnOnDemand()` |
| 心跳上报 | `RegionHeartbeater.java`, `StoreHeartbeater.java`, `PdServiceImpl.regionHeartbeat()` |
| Leader 转移 | `LeaderBalanceScheduler.java`, `HotRegionScheduler.java` |
| 日志压缩 | `LogCompactionWorker.java`, `AdminApplyHandler.java`, `PerRegionRaftEngine.java` |
| 快照追赶 | `RegionRaftStorage.java`, `SnapshotEngineImpl.java`, `KvRaftServiceImpl.java` |
| Region Split | `SplitCheckerScheduler.java`, `SplitDriver.java`, `AdminApplyHandler.applySplit()` |
| Region Balance | `RegionBalanceScheduler.java` |
| Region Merge | `MergeCheckerScheduler.java`, `MergeDriver.java`, `AdminApplyHandler.applyCommitMerge()` |
| 热点调度 | `HotRegionScheduler.java` |
| Peer 销毁 | `StoreImpl.destroyPeer()`, `PerRegionRaftEngine.destroy()` |
| Graceful Shutdown | `KvServer.stop()`, `KvServer.drain()` |
| Store 重启恢复 | `KvServer.recoverPersistedRegions()` |
| Operator 调度 | `Operator.java`, `SimpleOperator.java`, `OperatorSteps.java`, `OperatorControllerImpl.java` |
