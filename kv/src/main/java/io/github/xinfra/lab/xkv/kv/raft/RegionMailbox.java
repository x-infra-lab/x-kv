package io.github.xinfra.lab.xkv.kv.raft;

import io.github.xinfra.lab.raft.Config;
import io.github.xinfra.lab.raft.Peer;
import io.github.xinfra.lab.raft.RaftException;
import io.github.xinfra.lab.raft.RaftStateType;
import io.github.xinfra.lab.raft.RawNode;
import io.github.xinfra.lab.raft.Ready;
import io.github.xinfra.lab.raft.SoftState;
import io.github.xinfra.lab.raft.Transport;
import io.github.xinfra.lab.raft.proto.Eraftpb;
import io.github.xinfra.lab.xkv.common.metrics.XKvMetrics;
import io.github.xinfra.lab.xkv.kv.engine.PerRegionRaftEngine;
import io.github.xinfra.lab.xkv.kv.engine.StorageEngine;
import io.github.xinfra.lab.xkv.kv.mvcc.ConcurrencyManager;
import io.github.xinfra.lab.xkv.proto.Metapb;
import io.micrometer.core.instrument.Counter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Per-region lightweight state holder for the BatchSystem.
 *
 * <p>Replaces the per-region threads in {@link RegionPeerImpl}. Instead of
 * a dedicated readyThread blocking on {@code node.ready()}, the mailbox
 * queues inbound events (propose, step, tick, readIndex) and the shared
 * {@link RaftPoller} drives the {@link RawNode} state machine.
 *
 * <p><b>Thread safety</b>: inbound event queues are lock-free
 * ({@link ConcurrentLinkedQueue}). All {@code RawNode} operations happen
 * exclusively on the poller thread that picks this mailbox — no concurrent
 * access to the raft state machine.
 */
public final class RegionMailbox {
    private static final Logger log = LoggerFactory.getLogger(RegionMailbox.class);

    // --- Event types ---
    sealed interface Event {}
    record TickEvent() implements Event {}
    record ProposeEvent(byte[] data, long proposeSeq) implements Event {}
    record StepEvent(Eraftpb.Message msg) implements Event {}
    record ReadIndexEvent(byte[] ctx) implements Event {}
    record ConfChangeEvent(Eraftpb.ConfChangeV2 cc) implements Event {}
    record TransferLeaderEvent(long transferee) implements Event {}

    // --- Core state (owned by poller thread, not accessed concurrently) ---
    final RawNode rawNode;
    final RegionRaftStorage raftStorage;
    final PerRegionRaftEngine raftEngine;
    final StorageEngine storage;
    final ApplyHandler applyHandler;
    final Transport transport;
    final ConcurrencyManager cm;
    final long selfPeerId;
    final long regionId;
    volatile Metapb.Region region;
    Metapb.Peer self;

    // --- Scheduling ---
    private final AtomicBoolean scheduled = new AtomicBoolean(false);
    private volatile RaftPoller poller;
    private volatile ApplyWorker applyWorker;
    private volatile boolean applyBusy = false;
    private final java.util.ArrayDeque<List<Eraftpb.Entry>> bufferedApply = new java.util.ArrayDeque<>();
    volatile boolean running = true;
    volatile boolean destroyed = false;

    // --- Inbound event queue (producers: gRPC threads, tick driver) ---
    final Queue<Event> events = new ConcurrentLinkedQueue<>();

    // --- Proposal tracking (concurrent: gRPC threads write, poller reads) ---
    final ConcurrentMap<Long, CompletableFuture<RegionPeer.ApplyResult>> pendingProposals =
            new ConcurrentHashMap<>();
    final Queue<CompletableFuture<RegionPeer.ApplyResult>> pendingConfChanges =
            new ConcurrentLinkedQueue<>();
    final ConcurrentMap<ByteBuffer, CompletableFuture<Void>> pendingReadIndices =
            new ConcurrentHashMap<>();
    final ConcurrentSkipListMap<Long, List<CompletableFuture<Void>>> readIndexWaiters =
            new ConcurrentSkipListMap<>();
    final AtomicLong proposeSeq = new AtomicLong(1);

    // --- Leader state ---
    final AtomicBoolean leader = new AtomicBoolean(false);
    private long prevLeaderId = 0;

    private final Counter snapshotGenErrors = XKvMetrics.errorCounter("region_peer", "snapshot_gen");

    // --- Change-peer observer ---
    volatile RegionPeerImpl.ChangePeerObserver changePeerObserver;

    public RegionMailbox(StorageEngine storage,
                         PerRegionRaftEngine raftEngine,
                         Metapb.Region region,
                         Metapb.Peer self,
                         List<Peer> peers,
                         Transport transport,
                         ApplyHandler applyHandler,
                         RegionPeerImpl.Settings settings,
                         ConcurrencyManager cm,
                         io.github.xinfra.lab.xkv.kv.engine.SnapshotEngine snapshotEngine) {
        this.storage = storage;
        this.raftEngine = raftEngine;
        this.region = region;
        this.self = self;
        this.selfPeerId = self.getId();
        this.regionId = region.getId();
        this.transport = transport;
        this.applyHandler = applyHandler;
        this.cm = cm;
        this.raftStorage = new RegionRaftStorage(storage, raftEngine, snapshotEngine);

        // Persist region descriptor.
        try (var b = storage.newWriteBatch()) {
            raftEngine.saveRegion(region, b);
            storage.write(b, false);
        }

        // Build RawNode.
        var configBuilder = Config.builder()
                .id(self.getId())
                .electionTick(settings.electionTick())
                .heartbeatTick(settings.heartbeatTick())
                .storage(raftStorage)
                .applied(raftEngine.appliedIndex())
                .maxSizePerMsg(1L << 20)
                .maxInflightMsgs(256)
                .maxUncommittedEntriesSize(1L << 26)
                .checkQuorum(true)
                .preVote(true);
        if (settings.leaseBasedRead()) {
            configBuilder.readOnlyOption(io.github.xinfra.lab.raft.ReadOnlyOption.ReadOnlyLeaseBased);
        }
        var config = configBuilder.build();

        boolean fresh = raftEngine.lastIndex() == 0
                && raftEngine.appliedIndex() == 0
                && raftEngine.lastSnapshotMeta() == null;

        this.rawNode = RawNode.newRawNode(config);
        if (fresh) {
            rawNode.bootstrap(peers);
        }

        // Wire transport receiver: step messages enqueue to the mailbox.
        transport.setReceiver(msg -> {
            events.offer(new StepEvent(msg));
            wakeup();
        });
        transport.start();
    }

    long regionId() { return regionId; }

    boolean isRunning() { return running && !destroyed; }

    boolean isLeader() {
        return leader.get();
    }

    void setPoller(RaftPoller poller) { this.poller = poller; }

    void setApplyWorker(ApplyWorker worker) { this.applyWorker = worker; }

    void wakeup() {
        if (scheduled.compareAndSet(false, true)) {
            var p = poller;
            if (p != null) p.schedule(this);
        }
    }

    void enqueueTick() {
        events.offer(new TickEvent());
        wakeup();
    }

    // ================================================================
    // Poller-thread processing (single-threaded per mailbox)
    // ================================================================

    void processOnce(RaftPoller poller) {
        try {
            // 1. Drain inbound events into RawNode.
            drainEvents();

            // 2. Check if RawNode has a Ready to process.
            if (rawNode.hasReady()) {
                Ready ready = rawNode.readyWithoutAccept();
                applyReady(ready);
                rawNode.acceptReady(ready);
                rawNode.advance(ready);
            }
        } catch (Throwable t) {
            if (!running) return;
            log.error("region={} mailbox process failure", regionId, t);
        } finally {
            scheduled.set(false);
            // Re-schedule if more events arrived during processing.
            if (!events.isEmpty() && running) {
                wakeup();
            }
        }
    }

    private void drainEvents() {
        Event e;
        while ((e = events.poll()) != null) {
            try {
                switch (e) {
                    case TickEvent t -> rawNode.tick();
                    case ProposeEvent p -> rawNode.propose(p.data());
                    case StepEvent s -> rawNode.step(s.msg());
                    case ReadIndexEvent r -> rawNode.readIndex(r.ctx());
                    case ConfChangeEvent cc -> rawNode.proposeConfChange(cc.cc());
                    case TransferLeaderEvent tl ->
                            rawNode.transferLeader(tl.transferee());
                }
            } catch (RaftException ex) {
                log.warn("region={} event {} failed: {}", regionId, e.getClass().getSimpleName(),
                        ex.getMessage());
            }
        }
    }

    // ================================================================
    // Apply logic (mirrors RegionPeerImpl.applyReadyLocked)
    // ================================================================

    private void applyReady(Ready ready) throws Exception {
        // Leader-change detection via SoftState.
        if (ready.softState() != null) {
            var ss = ready.softState();
            boolean isLeader = ss.lead() == selfPeerId
                    && ss.raftState() == RaftStateType.StateLeader;
            boolean was = leader.getAndSet(isLeader);
            if (was && !isLeader) {
                abortPending();
            }
            if (was != isLeader || ss.lead() != prevLeaderId) {
                prevLeaderId = ss.lead();
                log.info("region={} peer={} isLeader={} (leader={})",
                        regionId, selfPeerId, isLeader, ss.lead());
            }
        }

        boolean wroteAnything = false;

        // Phase 0: install snapshot.
        if (ready.snapshot() != null && ready.snapshot().hasMetadata()
                && ready.snapshot().getMetadata().getIndex() > 0) {
            raftStorage.applySnapshot(ready.snapshot());
            log.info("region={} applied snapshot at index={} term={}",
                    regionId, ready.snapshot().getMetadata().getIndex(),
                    ready.snapshot().getMetadata().getTerm());
            wroteAnything = true;
        }

        // Phase A: stage log entries + hard state.
        boolean haveEntries = ready.entries() != null && !ready.entries().isEmpty();
        boolean haveHs = ready.hardState() != null
                && !ready.hardState().equals(Eraftpb.HardState.getDefaultInstance());
        if (haveEntries || haveHs) {
            try (var logBatch = storage.newWriteBatch()) {
                raftStorage.appendFused(ready.entries(), ready.hardState(), logBatch);
                storage.write(logBatch, false);
                wroteAnything = true;
            }
        }

        // Fsync for log entries (Phase 0 + A) before sending messages.
        if (wroteAnything) {
            storage.flushWal(true);
        }

        raftStorage.postApply(ready.hardState());

        // Send raft messages promptly — don't wait for apply.
        if (ready.messages() != null) {
            for (var msg : ready.messages()) {
                if (msg.getTo() == selfPeerId) continue;
                transport.send(msg.getTo(), msg);
            }
        }

        // Register ReadIndex waiters (they will be drained after apply completes).
        if (ready.readStates() != null) {
            for (var rs : ready.readStates()) {
                var key = ByteBuffer.wrap(rs.requestCtx());
                var readFut = pendingReadIndices.remove(key);
                if (readFut == null || readFut.isDone()) continue;
                long targetIndex = rs.index();
                if (raftEngine.appliedIndex() >= targetIndex) {
                    readFut.complete(null);
                } else {
                    readIndexWaiters.computeIfAbsent(targetIndex, k -> new ArrayList<>())
                            .add(readFut);
                }
            }
        }
        drainReadIndexWaiters();

        // Phase B: apply committed entries — async if ApplyWorker is available.
        if (ready.committedEntries() != null && !ready.committedEntries().isEmpty()) {
            var entries = ready.committedEntries();
            var worker = this.applyWorker;
            if (worker != null) {
                submitAsyncApply(entries);
            } else {
                applySynchronously(entries);
            }
        }
    }

    private void submitAsyncApply(List<Eraftpb.Entry> entries) {
        var worker = this.applyWorker;
        if (worker == null) {
            applySynchronously(entries);
            return;
        }
        var task = new ApplyWorker.ApplyTask(
                regionId, entries, applyHandler, raftEngine, storage, cm,
                new ApplyWorker.ApplyCallback() {
                    @Override
                    public void onApplied(List<PendingDispatch> pending,
                                          List<Eraftpb.ConfChangeV2> confChanges) {
                        for (var pd : pending) {
                            var fut = pendingProposals.remove(pd.proposeSeq);
                            if (fut != null) fut.complete(pd.result);
                        }
                        drainReadIndexWaiters();
                        for (var cc : confChanges) {
                            applyConfChangeOne(cc);
                        }
                    }

                    @Override
                    public void onError(Throwable t) {
                        log.error("region={} async apply error", regionId, t);
                    }

                    @Override
                    public void onComplete() {
                        applyBusy = false;
                        synchronized (bufferedApply) {
                            var next = bufferedApply.poll();
                            if (next != null) {
                                submitAsyncApply(next);
                            }
                        }
                    }
                });

        if (applyBusy) {
            synchronized (bufferedApply) {
                bufferedApply.offer(entries);
            }
            return;
        }
        applyBusy = true;
        if (!worker.submit(task)) {
            applyBusy = false;
            applySynchronously(entries);
        }
    }

    private void applySynchronously(List<Eraftpb.Entry> entries) {
        var pending = new ArrayList<PendingDispatch>();
        var confChanges = new ArrayList<Eraftpb.ConfChangeV2>();
        var postFlushCallbacks = new ArrayList<Runnable>();
        boolean wroteAnything = false;

        for (var entry : entries) {
            long idx = entry.getIndex();
            if (idx <= raftEngine.appliedIndex()) continue;
            if (applyOneEntry(entry, pending, confChanges, postFlushCallbacks, idx)) {
                wroteAnything = true;
            }
        }

        if (cm != null) {
            long inMemory = cm.maxTs().current();
            if (inMemory > raftEngine.persistedMaxTs()) {
                try (var mtsBatch = storage.newWriteBatch()) {
                    raftEngine.saveMaxTs(inMemory, mtsBatch);
                    storage.write(mtsBatch, false);
                    wroteAnything = true;
                }
            }
        }

        if (wroteAnything) {
            storage.flushWal(true);
        }

        for (var cb : postFlushCallbacks) {
            try { cb.run(); }
            catch (Throwable t) { log.warn("region={} postFlush callback failed", regionId, t); }
        }

        for (var pd : pending) {
            var fut = pendingProposals.remove(pd.proposeSeq);
            if (fut != null) fut.complete(pd.result);
        }

        for (var cc : confChanges) {
            applyConfChangeOne(cc);
        }
    }

    private boolean applyOneEntry(Eraftpb.Entry entry,
                                   List<PendingDispatch> pending,
                                   List<Eraftpb.ConfChangeV2> confChanges,
                                   List<Runnable> postFlushCallbacks,
                                   long idx) {
        if (entry.getEntryType() == Eraftpb.EntryType.EntryConfChange) {
            try {
                var cc = Eraftpb.ConfChange.parseFrom(entry.getData());
                var ccv2 = Eraftpb.ConfChangeV2.newBuilder()
                        .addChanges(Eraftpb.ConfChangeSingle.newBuilder()
                                .setType(cc.getChangeType())
                                .setNodeId(cc.getNodeId()))
                        .build();
                confChanges.add(ccv2);
            } catch (Throwable t) {
                log.warn("region={} conf-change parse failed: {}", regionId, t.getMessage());
            }
            try (var batch = storage.newWriteBatch()) {
                raftEngine.saveAppliedIndex(idx, batch);
                storage.write(batch, false);
            }
            return true;
        }
        if (entry.getEntryType() == Eraftpb.EntryType.EntryConfChangeV2) {
            try {
                confChanges.add(Eraftpb.ConfChangeV2.parseFrom(entry.getData()));
            } catch (Throwable t) {
                log.warn("region={} conf-change-v2 parse failed: {}", regionId, t.getMessage());
            }
            try (var batch = storage.newWriteBatch()) {
                raftEngine.saveAppliedIndex(idx, batch);
                storage.write(batch, false);
            }
            return true;
        }
        if (entry.getData().size() == 0) {
            try (var batch = storage.newWriteBatch()) {
                raftEngine.saveAppliedIndex(idx, batch);
                storage.write(batch, false);
            }
            return true;
        }

        ProposalCodec.Decoded decoded;
        try { decoded = ProposalCodec.decode(entry.getData().toByteArray()); }
        catch (Throwable t) {
            log.warn("region={} decode failed: {}", regionId, t.getMessage());
            try (var batch = storage.newWriteBatch()) {
                raftEngine.saveAppliedIndex(idx, batch);
                storage.write(batch, false);
            }
            return true;
        }

        if (raftEngine.isMerging() && !isMergeAdminKind(decoded.kind())) {
            try (var batch = storage.newWriteBatch()) {
                raftEngine.saveAppliedIndex(idx, batch);
                storage.write(batch, false);
            }
            if (decoded.proposeSeq() != 0) {
                pending.add(new PendingDispatch(decoded.proposeSeq(),
                        RegionPeer.ApplyResult.err("region merging — write rejected")));
            }
            return true;
        }

        var keys = ProposalKeyScope.peekKeys(decoded);
        java.util.function.Supplier<Void> work = () -> {
            try (var batch = storage.newWriteBatch()) {
                applyDecoded(decoded, batch, pending, postFlushCallbacks);
                raftEngine.saveAppliedIndex(idx, batch);
                storage.write(batch, false);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
            return null;
        };
        if (cm == null) {
            work.get();
        } else if (keys.isEmpty()) {
            cm.withWriter(work);
        } else {
            cm.withWriter(keys.get(), work);
        }
        return true;
    }

    private void applyDecoded(ProposalCodec.Decoded decoded,
                               StorageEngine.WriteBatch batch,
                               List<PendingDispatch> pending,
                               List<Runnable> postFlushCallbacks) {
        ApplyHandler.Result r = applyHandler.apply(decoded, batch);
        var outcome = r.success()
                ? RegionPeer.ApplyResult.ok(r.response())
                : RegionPeer.ApplyResult.err(r.errorMessage());
        if (decoded.proposeSeq() != 0) {
            pending.add(new PendingDispatch(decoded.proposeSeq(), outcome));
        }
        if (r.postFlush() != null) {
            postFlushCallbacks.add(r.postFlush());
        }
    }

    private void applyConfChangeOne(Eraftpb.ConfChangeV2 cc) {
        Eraftpb.ConfState newCs;
        try { newCs = rawNode.applyConfChange(cc); }
        catch (Exception e) {
            log.warn("region={} applyConfChange failed: {}", regionId, e.getMessage());
            var fut = pendingConfChanges.poll();
            if (fut != null) fut.complete(RegionPeer.ApplyResult.err(e.getMessage()));
            return;
        }

        java.util.List<Metapb.Peer> ctxPeers = java.util.List.of();
        if (cc.getContext().size() > 0) {
            try {
                ctxPeers = io.github.xinfra.lab.xkv.proto.KvServerpb.ConfChangeContext
                        .parseFrom(cc.getContext()).getPeersList();
            } catch (Throwable t) {
                log.warn("region={} conf-change context parse failed: {}", regionId, t.getMessage());
            }
        }

        var updatedRegion = region;
        if (!ctxPeers.isEmpty() && ctxPeers.size() == cc.getChangesCount()) {
            var b = region.toBuilder().clearPeers();
            var peerMap = new java.util.LinkedHashMap<Long, Metapb.Peer>();
            for (var p : region.getPeersList()) peerMap.put(p.getId(), p);
            for (int i = 0; i < cc.getChangesCount(); i++) {
                var ch = cc.getChanges(i);
                var pe = ctxPeers.get(i);
                switch (ch.getType()) {
                    case ConfChangeAddNode -> peerMap.put(pe.getId(),
                            pe.toBuilder().setRole(Metapb.PeerRole.Voter).build());
                    case ConfChangeAddLearnerNode -> peerMap.put(pe.getId(),
                            pe.toBuilder().setRole(Metapb.PeerRole.Learner).build());
                    case ConfChangeRemoveNode -> peerMap.remove(ch.getNodeId());
                    default -> { }
                }
            }
            for (var p : peerMap.values()) b.addPeers(p);
            b.setRegionEpoch(region.getRegionEpoch().toBuilder()
                    .setConfVer(region.getRegionEpoch().getConfVer() + 1));
            updatedRegion = b.build();
        }

        try (var batch2 = storage.newWriteBatch()) {
            raftStorage.postApplyConfState(newCs, batch2);
            if (updatedRegion != region) {
                raftEngine.saveRegion(updatedRegion, batch2);
            }
            storage.write(batch2, true);
        }
        if (updatedRegion != region) {
            region = updatedRegion;
        }

        if (changePeerObserver != null) {
            for (int i = 0; i < cc.getChangesCount() && i < ctxPeers.size(); i++) {
                try {
                    changePeerObserver.onChangePeer(
                            cc.getChanges(i).getType(), ctxPeers.get(i), updatedRegion);
                } catch (Throwable t) {
                    log.warn("region={} changePeer observer failed: {}", regionId, t.getMessage());
                }
            }
        }

        var fut = pendingConfChanges.poll();
        if (fut != null) fut.complete(RegionPeer.ApplyResult.ok(new byte[0]));
    }

    private void drainReadIndexWaiters() {
        long applied = raftEngine.appliedIndex();
        var head = readIndexWaiters.headMap(applied, true);
        for (var entry : head.entrySet()) {
            for (var f : entry.getValue()) {
                f.complete(null);
            }
        }
        head.clear();
    }

    private void abortPending() {
        for (var e : pendingProposals.entrySet()) {
            pendingProposals.remove(e.getKey());
            e.getValue().complete(RegionPeer.ApplyResult.err("leader stepped down"));
        }
        CompletableFuture<RegionPeer.ApplyResult> ccFut;
        while ((ccFut = pendingConfChanges.poll()) != null) {
            ccFut.complete(RegionPeer.ApplyResult.err("leader stepped down"));
        }
        var notLeader = RaftException.ErrStopped;
        for (var e : pendingReadIndices.values()) {
            e.completeExceptionally(notLeader);
        }
        pendingReadIndices.clear();
        for (var waiters : readIndexWaiters.values()) {
            for (var f : waiters) f.completeExceptionally(notLeader);
        }
        readIndexWaiters.clear();
    }

    void shutdown() {
        destroyed = true;
        running = false;
        try { transport.close(); } catch (Throwable t) {
            log.warn("region={} transport close failed: {}", regionId, t.getMessage(), t);
        }
        abortPending();
    }

    void maybeGenerateSnapshot() {
        long applied = raftEngine.appliedIndex();
        if (applied == 0) return;
        try {
            var cs = raftStorage.initialState().confState();
            raftStorage.createSnapshot(applied, cs, null);
        } catch (Throwable t) {
            snapshotGenErrors.increment();
            log.warn("region={} maybeGenerateSnapshot at applied={} failed: {}",
                    regionId, applied, t.getMessage());
        }
    }

    private static boolean isMergeAdminKind(ProposalCodec.Kind kind) {
        return kind == ProposalCodec.Kind.ADMIN_PREPARE_MERGE
                || kind == ProposalCodec.Kind.ADMIN_COMMIT_MERGE
                || kind == ProposalCodec.Kind.ADMIN_ROLLBACK_MERGE;
    }

    record PendingDispatch(long proposeSeq, RegionPeer.ApplyResult result) {}
}
