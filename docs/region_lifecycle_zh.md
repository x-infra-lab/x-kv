# Region 全生命周期

本文档梳理 x-kv 系统中一个 Region 从诞生到销毁的完整生命周期。

```
┌──────────────────────────────────────────────────────────────────────────────┐
│                          Region 全生命周期总览                                 │
│                                                                              │
│  ① Bootstrap 创建 → ② 新 Store 加入（空启动）→ ③ 副本扩展 (learner-first)      │
│  → ④ 未初始化 Peer 创建 (从 Raft 消息就地创建) → ⑤ Region 心跳上报            │
│  → ⑥ Leader 选举与转移 → ⑦ Raft 日志压缩 → ⑧ 快照与追赶 (携带 Region 描述符)  │
│  → ⑨ Region Split → ⑩ Region Balance → ⑪ Region Merge                       │
│  → ⑫ 热点调度 → ⑬ Peer 销毁与下线 → ⑭ Graceful Drain / Shutdown             │
│  → ⑮ Store 重启与 Region 恢复 → ⑯ PD Leadership 与故障切换                    │
└──────────────────────────────────────────────────────────────────────────────┘
```

> **副本变更机制对齐 TiKV（重要）**：③④⑤⑧ 已全面对齐 TiKV 的实现——
> **副本扩展走 learner-first**（先加 Learner，追上后再 promote 为 Voter，避免
> 幽灵投票者拖垮 quorum）；**新 Peer 从 Raft 消息就地创建为“未初始化 Peer”**
> （不查 PD 的最终一致视图），由 Leader 的快照携带完整 Region 描述符完成初始化；
> **Region 心跳只由 PD raft leader 服务**，KV 侧在流错误时自动切主。旧的
> `spawnOnDemand`（轮询 PD）路径已删除。详见 ③④⑤⑧⑯。

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
   - **未初始化 Peer handler**（`dispatcher.setMissingHandler`，见阶段④）
   - StoreHeartbeater（向 PD 上报容量/Region 数）
   - 客户端 gRPC Server
5. 等待 PD 调度器（`RuleCheckerScheduler` / `RegionBalanceScheduler`）发现需要给本 Store 添加副本。

**设计要点**：
- **不再从 PD 扫描 Region**：旧代码会 `getRegionByID(1)` / `scanRegions`，容易出问题（Region 1 不包含本 Store Peer、limit=100 遗漏等）。新设计完全依赖本地恢复 + 未初始化 Peer 就地创建。
- **新 Store 的第一个 Region 从哪来？** → PD 调度器在心跳中发现 Region 副本不足 → 下发 `AddLearnerNode` Operator → Leader 提议 ConfChange → Leader 发 Raft 消息给新 Store → 触发 `createUninitializedPeer`（见阶段④）→ 快照初始化 → learner 追上后 promote 为 Voter（见阶段③）。

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

## ③ 副本扩展 (learner-first) — 多 Store 加入 Region

**触发时机**：PD 调度器（`RuleCheckerScheduler` / `RegionBalanceScheduler`）发现 Region 副本数不足（低于 `maxPeerCount`），或需要平衡各 Store 的 Region 数。

**为什么是 learner-first（对齐 TiKV）**：直接加一个 `Voter` 会立即把新 Peer 计入 quorum，但新 Peer 的 `Match=0`（还在收快照），于是多数派从 N/2+1 升到包含一个永远落后的“幽灵投票者”，导致 commit 停滞。因此先以 **Learner**（不计入 quorum）加入，追上后再提升为 Voter。

**流程**：
```
① PD RuleChecker 发现副本不足 → 下发 AddLearnerNode Operator (AddLearnerStep)
② Leader dispatchOperator → proposeChangePeers (ConfChangeV2, AddLearnerNode)
③ Raft 复制 ConfChange → applyConfChangeOne: 新 Peer role=Learner, conf_ver++
④ Leader 发 MsgAppend/MsgSnapshot → 目标 Store 无此 Region → createUninitializedPeer (④)
⑤ 未初始化 Peer 收快照→完成初始化；learner 追上后从 leader 的 pending_peers 中消失
⑥ PD RuleChecker 下一轮：learner 不在 pending_peers 且 voterCount<max
     → 下发 AddNode Operator (PromoteLearnerStep) → raft 将 learner 提升为 Voter
⑦ applyConfChangeOne: role=Voter, conf_ver++ → 副本达标
```

**关键设计**：
- **计数语义**：promote 判定看 **voterCount**（`role != Learner`），而在途 learner 计入 `peerCount`，避免重复下发。
- **一次 conf-change/region/tick**：promote pass 一旦下发一个就 `return`，下一轮再处理。
- **promote 的前提**：learner 已从 leader 上报的 `pending_peers` 中消失（已追上），见“KV 上报 pending_peers”。

**关键源码** — `RegionHeartbeater.proposeChangePeers()`:
```java
private void proposeChangePeers(List<Pdpb.ChangePeer> changes) {
    var v2 = Eraftpb.ConfChangeV2.newBuilder();
    var ctx = KvServerpb.ConfChangeContext.newBuilder();
    for (var cp : changes) {
        v2.addChanges(Eraftpb.ConfChangeSingle.newBuilder()
                .setType(mapConfChangeType(cp.getChangeType()))   // AddLearnerNode / AddNode / RemoveNode
                .setNodeId(cp.getPeer().getId()));
        ctx.addPeers(cp.getPeer());
    }
    v2.setContext(ByteString.copyFrom(ctx.build().toByteArray()));
    peer.proposeConfChange(v2.build());
}
```

**关键源码** — `RegionPeerImpl.applyConfChangeOne()`（同时处理 learner / promote）:
```java
switch (ch.getType()) {
    case ConfChangeAddNode -> peerMap.put(pe.getId(),        // 新增或 promote 都是 AddNode
            pe.toBuilder().setRole(Metapb.PeerRole.Voter).build());
    case ConfChangeAddLearnerNode -> peerMap.put(pe.getId(),
            pe.toBuilder().setRole(Metapb.PeerRole.Learner).build());
    case ConfChangeRemoveNode -> peerMap.remove(ch.getNodeId());
}
// ConfChange bumps conf_ver
b.setRegionEpoch(region.getRegionEpoch().toBuilder()
        .setConfVer(region.getRegionEpoch().getConfVer() + 1));
```
> 对已存在 learner 的 `AddNode` 就是标准 etcd/raft 语义的“提升为 voter”，无需额外处理。

**关键源码** — PD 侧 `RuleCheckerScheduler.checkByCount()`（promote pass + 加 learner）:
```java
int voterCount = 0;
for (var p : region.getPeersList())
    if (p.getRole() != Metapb.PeerRole.Learner) voterCount++;

// 0. 把已追上的 learner 提升为 voter（learner-first 方案）
if (voterCount < maxPeerCount) {
    var pendingIds = new HashSet<Long>();
    for (var pp : state.getPendingPeers(region.getId())) pendingIds.add(pp.getId());
    for (var peer : region.getPeersList()) {
        if (peer.getRole() != Metapb.PeerRole.Learner) continue;
        if (pendingIds.contains(peer.getId())) continue;   // 仍在追赶
        var promoted = peer.toBuilder().setRole(Metapb.PeerRole.Voter).build();
        var resp = Pdpb.RegionHeartbeatResponse.newBuilder()
                .setRegionId(region.getId()).setChangePeer(promoted)
                .addChangePeerV2(Pdpb.ChangePeer.newBuilder()
                        .setPeer(promoted).setChangeType(Pdpb.ConfChangeType.AddNode))
                .build();
        controller.addOperator(new SimpleOperator(..., region.getId(), Operator.Kind.RULE_FIX,
                "promote learner " + peer.getId(), resp, Set.of(peer.getStoreId()),
                List.of(new OperatorSteps.PromoteLearnerStep(promoted)), Operator.PRIORITY_RULE_FIX));
        return scheduled;   // one conf-change per region per tick
    }
}

// 1. 副本不足：先加 LEARNER（不计入 quorum，Match=0 也不会拖垮 commit）
if (peerCount < maxPeerCount && bestStore >= 0) {
    long newPeerId = state.allocId(1);
    var newPeer = Metapb.Peer.newBuilder().setId(newPeerId).setStoreId(bestStore)
            .setRole(Metapb.PeerRole.Learner).build();
    var resp = Pdpb.RegionHeartbeatResponse.newBuilder()
            .setRegionId(region.getId()).setChangePeer(newPeer)
            .addChangePeerV2(Pdpb.ChangePeer.newBuilder()
                    .setPeer(newPeer).setChangeType(Pdpb.ConfChangeType.AddLearnerNode))
            .build();
    controller.addOperator(new SimpleOperator(..., region.getId(), Operator.Kind.RULE_FIX,
            "add learner on store " + bestStore, resp, Set.of(bestStore),
            List.of(new OperatorSteps.AddLearnerStep(newPeer)), Operator.PRIORITY_RULE_FIX));
}
```

---

## ④ 未初始化 Peer 创建 (从 Raft 消息就地创建，TiKV 式)

**触发时机**：Raft Leader 向目标 Store 发送消息（如 `MsgAppend`/`MsgSnapshot`），但目标 Store 尚未持有该 Region 的 Peer。

**设计变更（对齐 TiKV，已删除 `spawnOnDemand`）**：旧实现会去查 PD 拿 Region 描述符再建 Peer，但 PD 视图是最终一致的，在心跳滞后窗口内会把一个“幽灵投票者”遗留在 leader 上。现在**不查 PD**，直接用消息里的 `region_id`、`msg.to`（= 本 store 的 peer id）创建一个 **未初始化** Peer（range 未知、epoch=0、peers 仅含 self），随后由 leader 的**快照**把真实 Region 元数据（range/epoch/peers）与数据一并装好。

**流程**：
```
KvRaftServiceImpl 收到 RaftMessage(region_id, from_peer.store_id)
  → dispatcher.deliver(regionId, msg, fromStoreId)
  → 无本地 transport 且 spawnInFlight 首次 → missingHandler.onMissing(regionId, msg, fromStoreId)
  → 异步 createUninitializedPeer(regionId, firstMsg, fromStoreId):
      · 构造占位 Region{id, peers=[self]}（epoch=0、range 空）
      · transport 仅按 msg.from 用 resolveStoreRaftAddr 连回 leader（路由解析允许查 PD.getStore）
      · BatchRegionPeer(..., uninitialized=true) + setInitObserver
      · dispatcher.register(regionId, transport)（但不注册 key-range 路由、不启心跳）
  → raft 回复 leader、拉取日志/快照→ 收快照后完成初始化（见 ⑧/⑤）
```

**未初始化 Peer 的约束（对齐 TiKV）**：初始化前不注册到 `StoreImpl` 的 key-range 路由（不服务客户端读写）、不上报心跳、不参与 split/merge，仅参与 raft 消息收发与快照接收。

**关键源码** — `RaftMessageDispatcher.deliver()`（传递 `fromStoreId`）:
```java
public void deliver(long regionId, Eraftpb.Message msg, long fromStoreId) {
    var t = byRegion.get(regionId);
    if (t != null) { t.deliver(msg); return; }
    // 无本地 transport — 每个 region 在 spawn 未完成前只 fire 一次（避免 MsgAppend 重试风暴）
    var h = missingHandler;
    if (h != null && spawnInFlight.putIfAbsent(regionId, Boolean.TRUE) == null) {
        h.onMissing(regionId, msg, fromStoreId);
    }
}
```
> `fromStoreId` 取自 wire 层 `RaftMessage.from_peer`：裸 `Eraftpb.Message` 只有 peer id，创建未初始化 Peer 时需要它才能把回程链接到 leader。

**关键源码** — `KvServer.createUninitializedPeer()`:
```java
private void createUninitializedPeer(long regionId, Eraftpb.Message firstMsg, long fromStoreId) {
    if (store.peerForRegion(regionId).isPresent()) return;
    long selfPeerId = firstMsg.getTo();
    long fromPeerId = firstMsg.getFrom();
    var self = Metapb.Peer.newBuilder().setId(selfPeerId).setStoreId(config.storeId()).build();
    // 占位：只有 self、空 range、epoch 0。初始化快照会填入真实 range/epoch/peers。
    var placeholder = Metapb.Region.newBuilder().setId(regionId).addPeers(self).build();
    var transport = new GrpcRaftTransport(regionId, selfPeerId, config.storeId(), config.raftTls());
    if (fromPeerId != 0 && fromStoreId != 0) {
        String leaderAddr = resolveStoreRaftAddr(fromStoreId);   // 路由解析（允许查 PD）
        if (leaderAddr != null && !leaderAddr.isEmpty())
            transport.addPeer(fromPeerId, leaderAddr, fromStoreId);
    }
    var peer = new BatchRegionPeer(engine, raftEngine, placeholder, self, raftPeers,
            transport, handler, settings, cm, snapshotEngine,
            raftPoller, tickDriver, applyWorker, /* uninitialized= */ true);
    peer.setInitObserver(region -> completeUninitializedInit(peer, transport, region));
    dispatcher.register(regionId, transport);   // 仅 raft 路由；不注册 key-range、不启心跳
    peers.add(peer);
}
```

**关键源码** — `KvServer.completeUninitializedInit()`（快照装完后完成初始化）:
```java
// 由 RegionMailbox 的 initObserver 回调（见 ⑧），拿到真实 region 描述符后：
for (var p : region.getPeersList()) {              // 1) 装配其他 peer 的回程地址
    if (p.getStoreId() == config.storeId()) continue;
    String addr = resolveStoreRaftAddr(p.getStoreId());
    if (addr != null && !addr.isEmpty()) transport.addPeer(p.getId(), addr, p.getStoreId());
}
store.registerPeer(peer);                          // 2) key-range 路由生效（开始服务读写）
if (pdManager != null) startHeartbeater(peer);     // 3) 启动 region 心跳
if (copService != null) copService.invalidateCache();
```

---

## ⑤ Region 心跳上报（仅 PD leader 服务 + KV 故障切换）

**触发时机**：`RegionHeartbeater` 周期性触发（仅 Leader 发送，默认 500ms）。

**Layer 1：心跳只由 PD raft leader 服务**（对齐 TiKV，修复“conf-change 静默丢弃”）：Region 心跳携带 operator 分发，而只有 PD raft leader 才拥有带活跃 operator 的 `operatorController`；follower 的 controller 是空的，心跳发到那里会静默丢弃 conf-change。因此：
- **PD 侧**：`PdServiceImpl.regionHeartbeat` 在 `onNext` 开头拒绝非-leader 心跳（`UNAVAILABLE: not PD leader`）。
- **KV 侧**：`RegionHeartbeater` 从 `PdEndpointManager` 动态取 stub，流 `onError` 时先 `resetStream()` 再 `pdManager.switchLeader()` 重新发现真正的 raft leader，下个 tick 用新 leader 重连。

**流程**：
1. Leader Peer 定期发送 `RegionHeartbeatRequest`，携带 Region 描述符、Leader 信息、`approximate_size`、**`pending_peers`**（还在追赶的 peer）。
2. PD（leader）接收后：通过 Raft 复制 `updateRegion`（描述符变更时）、更新 RegionStats、存储 `pending_peers`、更新 Leader 路由。
3. PD 调用 `OperatorControllerImpl.dispatch(hb)` 驱动 operator 生命周期（observe/next）。
4. `RegionHeartbeater.dispatchOperator()` 分发执行各调度指令。

**关键源码** — `RegionHeartbeater.tick()`（携带 pending_peers）:
```java
private void tick() {
    if (closed || peer.isDestroyed()) return;
    ensureStream();                       // 用 pdManager.asyncStub() 绑定当前 PD leader
    if (outbound == null) return;
    if (!peer.isLeader()) return;         // 只有 region leader 发心跳

    long approxSize = engine.approximateSize(StorageEngine.Cf.DEFAULT,
            region.getStartKey().toByteArray(), region.getEndKey().toByteArray());

    outbound.onNext(Pdpb.RegionHeartbeatRequest.newBuilder()
            .setRegion(peer.region())
            .setLeader(peer.self())
            .addAllPendingPeers(pendingPeers())   // learner/落后 voter 的 metapb.Peer
            .setApproximateSize(approxSize)
            .build());
}
```

**关键源码** — `RegionHeartbeater.ensureStream()`（流错误时切主）:
```java
outbound = stub.regionHeartbeat(new StreamObserver<>() {
    @Override public void onNext(Pdpb.RegionHeartbeatResponse v) { dispatchOperator(v); }
    @Override public void onError(Throwable t) {
        resetStream();
        // 流到的 PD 节点可能因不再是 raft leader 而拒绝我们——重新发现真正的 leader
        if (pdManager != null) pdManager.switchLeader();
    }
    @Override public void onCompleted() { resetStream(); }
});
```

**关键源码** — `PdServiceImpl.regionHeartbeat`（leader 守卫 + 存 pending_peers）:
```java
public void onNext(RegionHeartbeatRequest hb) {
    // 只有 PD raft leader 拥有带活跃 operator 的 controller；拒绝非-leader 心跳
    var leaderRn = raftNode;
    if (leaderRn != null && !leaderRn.isLeader()) {
        obs.onError(Status.UNAVAILABLE.withDescription("not PD leader").asRuntimeException());
        return;
    }
    if (hb.hasRegion()) {
        state.updateRegionStats(hb.getRegion().getId(),
                hb.getApproximateSize(), hb.getApproximateKeys());
        // 记录仍在追赶的 peer（learner 收快照 / 落后 voter），供 RuleChecker 判定 promote 时机
        state.updatePendingPeers(hb.getRegion().getId(), hb.getPendingPeersList());
    }
    // ... updateLeader + operatorController.dispatch(hb) ...
}
```

### KV 上报 pending_peers（promote 判定所需）

Leader 从 `rawNode` 的 per-peer 进度（`Match` / 快照状态）计算落后集合，填入心跳的 `pending_peers`；PD 存储后供 `RuleCheckerScheduler` 判定 learner 是否已追上。

```java
// RegionMailbox.refreshLaggingPeers()（poller 线程，rawNode.status() 非线程安全）
var status = rawNode.status();
long lastIndex = raftEngine.lastIndex();
var lagging = new HashSet<Long>();
for (var entry : status.progress().entrySet()) {
    long peerId = entry.getKey();
    if (peerId == selfPeerId) continue;
    var pr = entry.getValue();
    if (pr.state() == ProgressState.SNAPSHOT || pr.match() < lastIndex) {
        lagging.add(peerId);      // 仍在装快照或 Match 未追上 leader 最后日志
    }
}
laggingPeers = lagging;
```

**关键源码** — `RegionHeartbeater.pendingPeers()`（映射为 metapb.Peer）:
```java
private List<Metapb.Peer> pendingPeers() {
    var lagging = peer.laggingPeerIds();
    if (lagging.isEmpty()) return List.of();
    var out = new ArrayList<Metapb.Peer>(lagging.size());
    for (var p : peer.region().getPeersList())
        if (lagging.contains(p.getId())) out.add(p);
    return out;
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

## ⑧ Raft 快照与 Follower 追赶（携带 Region 描述符）

**触发时机**：Follower 的 `nextIndex < Leader.firstIndex`（日志已被压缩，无法通过日志追赶）；或一个**未初始化 Peer**（阶段④）需要首次装载 Region 元数据与数据。

**设计变更（对齐 TiKV）**：
- **快照携带完整 Region 描述符**：`SnapshotMeta` 新增 `metapb.Region region = 10`（range + epoch + peers 全量），让从裸 raft 消息创建的未初始化 Peer 无需查 PD 就能学到自己的 Region 元数据。
- **快照走正常 raft 流**：取消了旧的 `MsgSnapshot` 旁路，快照由 `RegionMailbox.applyReady` 驱动 `rawNode` 统一处理（持久化 snapshot meta → `installPendingSnapshot` 装 KV 数据 → 采纳 region 描述符）。

**流程**：
```
Leader: createSnapshot(applied, confState)
  → buildAndStream(regionId, term, index, startKey, endKey, regionDesc, chunks::add)
  → SnapshotMeta.setRegion(regionDesc)  // 携带 range/epoch/peers
  → 通过正常 raft Ready.messages 发送给 Follower
Follower: RegionMailbox.applyReady(ready) 遇到 ready.snapshot():
  Phase A: raftStorage.saveSnapshotMeta(snap)  // 持久化 pending snapshot
  Apply:   Metapb.Region r = raftStorage.installPendingSnapshot()  // 装 KV 数据 + 取出 region
  → region = r; 更新 self
  → 若 uninitialized: uninitialized=false; initObserver.onInitialized(r)  → completeUninitializedInit (④)
```

**关键源码** — `proto/kv_serverpb.proto` 的 `SnapshotMeta`:
```proto
message SnapshotMeta {
    uint64 region_id = 1;
    metapb.RegionEpoch region_epoch = 2;
    uint64 raft_term = 3;
    uint64 raft_index = 4;
    // ... start_key/end_key/cf_names/total_size/generated_at_ms ...
    // 完整 region 描述符（range + epoch + peers）。未初始化 Peer 从此学到自己的 region 元数据。
    metapb.Region region = 10;
}
```

**关键源码** — `RegionRaftStorage.createSnapshot()`（生成侧写入 region）:
```java
Metapb.Region regionDesc = raft.region();
byte[] startKey = regionDesc != null && !regionDesc.getStartKey().isEmpty()
        ? regionDesc.getStartKey().toByteArray() : new byte[]{0};
byte[] endKey = regionDesc != null && !regionDesc.getEndKey().isEmpty()
        ? regionDesc.getEndKey().toByteArray() : null;
// buildAndStream 把 regionDesc 写入每个 chunk 的 SnapshotMeta.region
snapshotEngine.buildAndStream(regionId(), term, appliedIndex, startKey, endKey, regionDesc, chunks::add);
```

**关键源码** — `RegionRaftStorage.installPendingSnapshot()`（安装侧取出 region）:
```java
public Metapb.Region installPendingSnapshot() {
    if (!raft.hasPendingSnapshot()) return null;
    byte[] dataBytes = raft.loadPendingSnapshot();
    Metapb.Region region = null;
    var chunks = decodeChunkEnvelope(dataBytes);
    for (var chunk : chunks) {
        if (chunk.hasMeta() && chunk.getMeta().hasRegion()) { region = chunk.getMeta().getRegion(); break; }
    }
    snapshotEngine.receiveAndInstall(regionId(), chunks);   // 装 default/lock/write 三个 CF
    try (var b = storage.newWriteBatch()) {
        if (region != null) raft.saveRegion(region, b);      // 持久化快照携带的 region 描述符
        raft.clearPendingSnapshot(b);
        storage.write(b, true);
    }
    return region;
}
```

**关键源码** — `RegionMailbox.applyReady()`（安装快照后驱动初始化）:
```java
if (haveSnap) {
    Metapb.Region snapRegion = raftStorage.installPendingSnapshot();
    if (snapRegion != null) {
        region = snapRegion;                       // 采纳 leader 发来的 range/epoch/peers
        for (var p : snapRegion.getPeersList())
            if (p.getId() == selfPeerId) { self = p; break; }
        if (uninitialized) {                       // 未初始化 Peer 首次拿到真实描述符
            uninitialized = false;
            var obs = initObserver;
            if (obs != null) obs.onInitialized(snapRegion);   // → KvServer.completeUninitializedInit
        }
    }
}
```

> **Leader 为什么会发快照？** 新 learner 的 `Next` 落在 leader `firstIndex` 之前（日志已压缩），raft 库会请求 `Storage.snapshot()`；`maybeGenerateSnapshot` / `RegionRaftStorage.snapshot()` 支持异步生成 pending snapshot。

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
2. 通过心跳下发 → Leader 提议 `ConfChangeAddNode` → Apply 后目标 Store 从 raft 消息创建未初始化 Peer（见④）。
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

## ⑯ PD Leadership 与故障切换（可靠性）

上述 Layer 1（心跳只由 leader 服务）依赖一个前提：**“谁是 PD leader”的判断必须与驱动 scheduler/operator 所有权的状态一致**。实现中有两个可靠性修复。

### ⑯.1 Leadership 判断：以 observer-tracked 状态为权威

`PdRaftNode` 有两个 leadership 来源：
- **`node.basicStatus()`**：实时查询，但从 gRPC 线程调用时观察到会报**陈旧的 `StateLeader`**（该节点 leadership 实际已被取代）。
- **observer-tracked `leader` / `currentLeaderId`**：由 raft SoftState leader observer 驱动，正是启动/停止 scheduler、持有 `operatorController` 的权威来源。

若 `isLeader()` 优先信任 `basicStatus()`，非-leader 节点会给出 false-positive，KV region 心跳连到该节点却从不被拒绝，learner operator 永不被 dispatch（表现为副本 2→3 hang）。因此修正为**优先信任 observer-tracked 状态**，`basicStatus()` 仅作最后回退：

```java
public boolean isLeader() {
    // 优先信任 observer flag：它由 raft SoftState 驱动，与“谁实际服务 conf-change”一致。
    if (leader.get()) return true;
    if (currentLeaderId != 0) return false;   // 已观察到其他 leader
    try {                                     // 尚未观察到任何 leadership 变更时才回退
        var st = node.basicStatus();
        return st != null && st.state == RaftStateType.StateLeader;
    } catch (Throwable t) { return false; }
}

public long leaderNodeId() {
    long id = currentLeaderId;
    if (id != 0) return id;
    if (leader.get()) return nodeId;
    try { var st = node.basicStatus(); if (st != null && st.lead != 0) return st.lead; }
    catch (Throwable e) { /* debug */ }
    return 0;
}
```

### ⑯.2 KV 端发现：PdEndpointManager.switchLeader() 健壮化

`switchLeader()` 探测所有 endpoint 调 `getMembers` 发现 leader。遇到“可达但无 leader 信息”的节点（选举中 / follower 拒答）时，**不再 pin 到该非-leader**，而是继续探测其余 endpoint，只在全部无 leader 时才回退到 `firstReachable`（pin 到非-leader 会静默丢弃心跳上的 operator 分发）。

```java
String firstReachable = null;
for (var ep : endpoints) {
    var resp = probeStub.getMembers(...);
    if (firstReachable == null) firstReachable = ep;
    if (resp.hasLeader() && resp.getLeader().getClientUrlsCount() > 0) {
        connectTo(resp.getLeader().getClientUrls(0)); return;   // 连到真正的 leader
    }
    // 可达但 leader 未知 → 继续探测，不 pin 到非-leader
}
if (firstReachable != null) connectTo(firstReachable);   // 全部无 leader 时的回退
```

### ⑯.3 客户端 TSO 故障切换（TsoBatcherImpl）

Leadership 严格化后，旧 leader 会正确拒绝 TSO。非-leader PD 对 `getTimestamp` 不关流，而是回一个带 error header、timestamp=0 的响应。若客户端不检查 error header，会用 `ts=(0<<18)|1 = 1` 完成 future，导致 `txn.begin()` 拿到 `startTs=1`，MVCC 读在 start_ts=1 看不到任何真实提交的数据（空读）。`TsoBatcherImpl.onNext` 因此检测 error header 并透明重试：

```java
if (resp.getHeader().hasError()) {
    newS.broken = true;
    var toRetry = new ArrayList<Pending>(pr.batch);      // 本批 + 同流其余 pending 全部重入队
    for (PendingResp o = newS.pending.pollFirst(); o != null; o = newS.pending.pollFirst())
        toRetry.addAll(o.batch);
    pdClient.switchLeader();     // 重新发现 leader
    requeue(toRetry);            // 加到队列前部，dispatcher 在新 leader 上重试
    return;
}
```
> `TxnClientImpl.begin()` 是单次 `tso.getTimestamp().get()` 无重试，故重试必须在 TSO 层透明完成（future 阻塞直到新 leader 返回真实 ts）。

---

## RegionEpoch 版本控制

贯穿整个生命周期的核心机制：

| 字段 | 变更时机 | 作用 |
|------|---------|------|
| `conf_ver` | ConfChange（AddNode / AddLearnerNode / RemoveNode） | 防止过期的成员变更操作 |
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
| **RuleCheckerScheduler** | 10s | 4 | ③ 副本扩展 (learner-first + promote) / ⑬ Peer 销毁 |

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

`storeCm` 被 `TransactionService` 和 `CdcService` 共享，保证跨 Region 事务的时间戳一致性。新创建的子 Peer（split / createUninitializedPeer）也优先使用 `storeCm`：

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
| 未初始化 Peer 创建 | `RaftMessageDispatcher.java`, `KvServer.createUninitializedPeer()` / `completeUninitializedInit()`, `BatchRegionPeer.java`, `RegionMailbox.java` |
| 心跳上报 / pending_peers | `RegionHeartbeater.java`, `StoreHeartbeater.java`, `PdServiceImpl.regionHeartbeat()`, `RegionMailbox.refreshLaggingPeers()` |
| PD Leadership / 故障切换 | `PdRaftNode.java` (`isLeader`/`leaderNodeId`), `PdEndpointManager.java`, `TsoBatcherImpl.java` |
| Leader 转移 | `LeaderBalanceScheduler.java`, `HotRegionScheduler.java` |
| 日志压缩 | `LogCompactionWorker.java`, `AdminApplyHandler.java`, `PerRegionRaftEngine.java` |
| 快照追赶 (携带 Region) | `RegionRaftStorage.java` (`createSnapshot`/`installPendingSnapshot`), `SnapshotEngineImpl.java`, `RegionMailbox.applyReady()`, `kv_serverpb.proto` (SnapshotMeta) |
| Region Split | `SplitCheckerScheduler.java`, `SplitDriver.java`, `AdminApplyHandler.applySplit()` |
| Region Balance | `RegionBalanceScheduler.java` |
| Region Merge | `MergeCheckerScheduler.java`, `MergeDriver.java`, `AdminApplyHandler.applyCommitMerge()` |
| 热点调度 | `HotRegionScheduler.java` |
| Peer 销毁 | `StoreImpl.destroyPeer()`, `PerRegionRaftEngine.destroy()` |
| Graceful Shutdown | `KvServer.stop()`, `KvServer.drain()` |
| Store 重启恢复 | `KvServer.recoverPersistedRegions()` |
| Operator 调度 | `Operator.java`, `SimpleOperator.java`, `OperatorSteps.java`, `OperatorControllerImpl.java` |
