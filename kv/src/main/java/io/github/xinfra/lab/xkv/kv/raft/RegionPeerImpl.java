package io.github.xinfra.lab.xkv.kv.raft;

import io.github.xinfra.lab.raft.Config;
import io.github.xinfra.lab.raft.Node;
import io.github.xinfra.lab.raft.Peer;
import io.github.xinfra.lab.raft.RaftException;
import io.github.xinfra.lab.raft.RaftStateType;
import io.github.xinfra.lab.raft.Ready;
import io.github.xinfra.lab.raft.Transport;
import io.github.xinfra.lab.raft.internal.DefaultNode;
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
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Default {@link RegionPeer} implementation.
 *
 * <h3>Apply loop = the load-bearing v2 invariant</h3>
 *
 * <p>Each iteration of {@link #readyLoop} pulls one {@link Ready} from
 * x-raft-lib and writes it to disk in TWO phases:
 *
 * <ol>
 *   <li><b>Phase A — log durability.</b> One batch holding the new entries
 *       (from {@code ready.entries()}) and hard state (from
 *       {@code ready.hardState()}); one {@code write(batch, sync=true)}.
 *       This satisfies the raft library's persistence contract before any
 *       MsgAppResp goes out.</li>
 *   <li><b>Phase B — apply.</b> For EACH committed entry: one batch
 *       containing that entry's business CF mutations AND its
 *       {@code appliedIndex} bump; one {@code write(batch, sync=true)}.</li>
 * </ol>
 *
 * <p><b>Why per-entry fsync in Phase B</b>: the MVCC apply path reads
 * RocksDB state (via {@code engine.get}) when checking prewrite conflicts.
 * If two committed entries shared a batch, entry N's reader would see
 * pre-Ready RocksDB state and miss entry N-1's staged-but-unflushed lock CF
 * writes — two concurrent prewrites for the same key could both pass their
 * conflict checks. Per-entry fsync flushes entry N-1 before entry N's
 * MvccTxn runs, restoring the conflict-detection invariant.
 *
 * <p>Inv-1 ("appliedIndex paired atomically with business data") still
 * holds at the entry granularity — they share one batch, one fsync. A crash
 * mid-Ready cannot leave appliedIndex out of sync with the data.
 *
 * <p>v1 split appliedIndex and business data into two RocksDB instances and
 * two fsyncs; a crash between them reported a committed transaction as not
 * committed. The unified-CF design here makes that class of bug impossible
 * by construction.
 */
public final class RegionPeerImpl implements RegionPeer {
    private static final Logger log = LoggerFactory.getLogger(RegionPeerImpl.class);

    private final long regionId;
    private volatile Metapb.Region region;
    private final Metapb.Peer self;
    private final StorageEngine storage;
    private final PerRegionRaftEngine raftEngine;
    private final RegionRaftStorage raftStorage;
    private final Node node;
    private final Transport transport;
    private final ApplyHandler applyHandler;

    private final AtomicLong proposeSeq = new AtomicLong(1);
    private final ConcurrentMap<Long, CompletableFuture<ApplyResult>> pendingProposals = new ConcurrentHashMap<>();
    /**
     * FIFO of futures for in-flight conf-change proposals — raft's
     * {@code proposeConfChange} doesn't carry a propose_seq we can match
     * back to a future, so we rely on the ordering raft preserves
     * (proposals from one leader apply in order). Followers' queue stays
     * empty since they don't propose.
     */
    private final java.util.concurrent.ConcurrentLinkedQueue<CompletableFuture<ApplyResult>> pendingConfChanges =
            new java.util.concurrent.ConcurrentLinkedQueue<>();

    /**
     * In-flight readIndex requests. Key = unique requestCtx (UUID bytes wrapped
     * in ByteBuffer for correct equals/hashCode). gRPC threads insert; the
     * ready-loop thread removes + resolves when {@code Ready.readStates()} arrives.
     */
    private final ConcurrentMap<ByteBuffer, CompletableFuture<Void>> pendingReadIndices = new ConcurrentHashMap<>();

    /**
     * ReadIndex requests whose {@code readState.index()} exceeds the current
     * {@code appliedIndex} at the time the ReadState was received. Keyed by
     * the target apply-index; drained after each committed-entry batch apply.
     * Accessed only from the ready-loop thread (single-writer), so a
     * ConcurrentSkipListMap is used solely for O(log n) headMap draining.
     */
    private final ConcurrentSkipListMap<Long, List<CompletableFuture<Void>>> readIndexWaiters =
            new ConcurrentSkipListMap<>();

    private final AtomicBoolean leader = new AtomicBoolean(false);
    private volatile boolean destroyed = false;
    private volatile boolean running = true;
    private final Counter snapshotGenErrors = XKvMetrics.errorCounter("region_peer", "snapshot_gen");

    private final Thread readyThread;
    private final ScheduledExecutorService tickTimer;
    private final long heartbeatTickMs;

    /**
     * Optional: when present, the apply loop's batch flush is gated by the
     * region's ConcurrencyManager write-lock. This lets reader RPCs (which
     * grab the read-lock) be strictly serialized with prewrite-applies for
     * SI correctness — see ConcurrencyManager javadoc.
     */
    private final ConcurrencyManager cm;

    /** Optional callback that fires after every conf-change apply. */
    private volatile ChangePeerObserver changePeerObserver;

    /**
     * Notified after a conf-change has been applied locally. Use to spawn
     * a new RegionPeer (for AddNode targeting this store) or destroy an
     * existing one (for RemoveNode of this peer).
     */
    @FunctionalInterface
    public interface ChangePeerObserver {
        void onChangePeer(io.github.xinfra.lab.raft.proto.Eraftpb.ConfChangeType type,
                          Metapb.Peer peer,
                          Metapb.Region updatedRegion);
    }

    /** Wire the change-peer observer after construction (cyclic refs). */
    public void setChangePeerObserver(ChangePeerObserver obs) { this.changePeerObserver = obs; }

    public RegionPeerImpl(StorageEngine storage,
                          PerRegionRaftEngine raftEngine,
                          Metapb.Region region,
                          Metapb.Peer self,
                          List<Peer> peers,
                          Transport transport,
                          ApplyHandler applyHandler,
                          Settings settings) {
        this(storage, raftEngine, region, self, peers, transport, applyHandler, settings, null, null);
    }

    public RegionPeerImpl(StorageEngine storage,
                          PerRegionRaftEngine raftEngine,
                          Metapb.Region region,
                          Metapb.Peer self,
                          List<Peer> peers,
                          Transport transport,
                          ApplyHandler applyHandler,
                          Settings settings,
                          ConcurrencyManager cm) {
        this(storage, raftEngine, region, self, peers, transport, applyHandler, settings, cm, null);
    }

    public RegionPeerImpl(StorageEngine storage,
                          PerRegionRaftEngine raftEngine,
                          Metapb.Region region,
                          Metapb.Peer self,
                          List<Peer> peers,
                          Transport transport,
                          ApplyHandler applyHandler,
                          Settings settings,
                          ConcurrencyManager cm,
                          io.github.xinfra.lab.xkv.kv.engine.SnapshotEngine snapshotEngine) {
        this.storage = storage;
        this.raftEngine = raftEngine;
        this.region = region;
        this.self = self;
        this.regionId = region.getId();
        this.transport = transport;
        this.applyHandler = applyHandler;
        this.raftStorage = new RegionRaftStorage(storage, raftEngine, snapshotEngine);
        this.heartbeatTickMs = settings.heartbeatTickMs;
        this.cm = cm;

        // Persist the region descriptor and an initial conf state if absent.
        try (var b = storage.newWriteBatch()) {
            raftEngine.saveRegion(region, b);
            storage.write(b, false);
        }

        // Build raft Config + Node first; the transport receiver below
        // captures `node`, which must be initialised by then.
        var config = Config.builder()
                .id(self.getId())
                .electionTick(settings.electionTick)
                .heartbeatTick(settings.heartbeatTick)
                .storage(raftStorage)
                .applied(raftEngine.appliedIndex())
                .maxSizePerMsg(1L << 20)            // 1 MiB per raft message
                .maxInflightMsgs(256)
                .maxUncommittedEntriesSize(1L << 26)   // 64 MiB
                .checkQuorum(true)
                .preVote(true)
                .build();

        boolean fresh = raftEngine.lastIndex() == 0
                && raftEngine.appliedIndex() == 0
                && raftEngine.lastSnapshotMeta() == null;

        this.node = fresh
                ? Node.startNode(config, peers)
                : Node.restartNode(config);

        // Wire transport receiver now that `node` exists.
        transport.setReceiver(msg -> {
            try {
                this.node.step(msg);
            } catch (Exception e) {
                log.warn("region={} step failed", regionId, e);
            }
        });
        transport.start();

        // Track leader state. The observer is called as (oldLeaderId, newLeaderId).
        node.registerLeaderObserver((oldLeader, newLeader) -> {
            boolean isLeader = newLeader == self.getId();
            boolean was = leader.getAndSet(isLeader);
            if (was && !isLeader) {
                for (var e : pendingProposals.entrySet()) {
                    pendingProposals.remove(e.getKey());
                    e.getValue().complete(ApplyResult.err("leader stepped down"));
                }
                CompletableFuture<ApplyResult> ccFut;
                while ((ccFut = pendingConfChanges.poll()) != null) {
                    ccFut.complete(ApplyResult.err("leader stepped down"));
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
            if (was != isLeader) {
                log.info("region={} peer={} isLeader={} (newLeader={})",
                        regionId, self.getId(), isLeader, newLeader);
            }
        });

        // Reader loop.
        this.readyThread = new Thread(this::readyLoop, "region-" + regionId + "-ready");
        this.readyThread.setDaemon(true);
        this.readyThread.start();

        // Tick driver.
        this.tickTimer = Executors.newSingleThreadScheduledExecutor(r -> {
            var t = new Thread(r, "region-" + regionId + "-tick");
            t.setDaemon(true);
            return t;
        });
        this.tickTimer.scheduleAtFixedRate(this::tickSafely,
                heartbeatTickMs, heartbeatTickMs, TimeUnit.MILLISECONDS);
    }

    public RegionRaftStorage raftStorage() { return raftStorage; }

    @Override public long regionId() { return regionId; }
    @Override public Metapb.Region region() { return region; }
    @Override public Metapb.Peer self() { return self; }
    @Override public boolean isLeader() {
        // Query the node directly — more reliable than the observer-driven
        // flag (the observer may not have fired yet for the very first
        // leader election).
        try {
            var st = node.basicStatus();
            return st != null && st.state == RaftStateType.StateLeader;
        } catch (Throwable t) {
            return leader.get();
        }
    }
    @Override public boolean isDestroyed() { return destroyed; }
    @Override public long firstIndex() { return raftEngine.firstIndex(); }
    @Override public long appliedIndex() { return raftEngine.appliedIndex(); }
    @Override public long maxTs() {
        if (cm == null) return 0;
        return Math.max(cm.maxTs().current(), cm.safeTs());
    }
    @Override public long currentTerm() { return raftEngine.currentTerm(); }
    @Override public long votedFor() { return raftEngine.votedFor(); }
    @Override public long commitIndex() { return raftEngine.commitIndex(); }
    @Override public long lastIndex() { return raftEngine.lastIndex(); }

    @Override
    public void maybeGenerateSnapshot() {
        long applied = raftEngine.appliedIndex();
        if (applied == 0) return;       // nothing applied yet — no snapshot to take
        try {
            // ConfState from the raft library's view (cached on RegionRaftStorage).
            var cs = raftStorage.initialState().confState();
            raftStorage.createSnapshot(applied, cs, /* data= */ null);
        } catch (Throwable t) {
            snapshotGenErrors.increment();
            log.warn("region={} maybeGenerateSnapshot at applied={} failed: {}",
                    regionId, applied, t.getMessage());
        }
    }

    @Override
    public void updateRegion(Metapb.Region newRegion) {
        if (newRegion.getId() != regionId) {
            throw new IllegalArgumentException(
                    "updateRegion: id=" + newRegion.getId() + " != peer regionId=" + regionId);
        }
        // The volatile write makes the new descriptor visible to subsequent
        // reads from any thread (e.g. the Store's peerForKey).
        this.region = newRegion;
        log.info("region={} updated descriptor: start={} end={} epoch.version={}",
                regionId,
                newRegion.getStartKey().toStringUtf8(),
                newRegion.getEndKey().toStringUtf8(),
                newRegion.getRegionEpoch().getVersion());
    }

    @Override
    public void transferLeader(long targetPeerId) {
        try {
            // x-raft-lib: transferLeadership(currentLeaderId, transfereeId)
            node.transferLeadership(self.getId(), targetPeerId);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            log.warn("region={} transferLeader to {} interrupted", regionId, targetPeerId);
        } catch (Throwable t) {
            log.warn("region={} transferLeader to {} failed: {}",
                    regionId, targetPeerId, t.getMessage());
        }
    }

    @Override
    public CompletableFuture<ApplyResult> propose(Proposal p) {
        if (destroyed) {
            return CompletableFuture.completedFuture(ApplyResult.err("region destroyed"));
        }
        if (!isLeader()) {
            return CompletableFuture.completedFuture(ApplyResult.err("not leader"));
        }
        long seq = proposeSeq.getAndIncrement();
        // The proposal kind is encoded inside p.payload() — caller already
        // wrapped the kind tag using ProposalCodec.encode. Here we add the
        // proposeSeq envelope.
        byte[] envelope = withSeq(p.payload(), seq);
        var fut = new CompletableFuture<ApplyResult>();
        pendingProposals.put(seq, fut);
        try {
            node.propose(envelope);
        } catch (RaftException | InterruptedException e) {
            pendingProposals.remove(seq);
            fut.complete(ApplyResult.err(e.getMessage()));
            if (e instanceof InterruptedException) Thread.currentThread().interrupt();
        }
        return fut;
    }

    @Override
    public CompletableFuture<ApplyResult> proposeAdmin(AdminProposal p) {
        // Admin proposals follow the same envelope; kind tags differ.
        return propose(new Proposal(p.payload(), 0, 0));
    }

    @Override
    public CompletableFuture<ApplyResult> proposeConfChange(Eraftpb.ConfChangeV2 cc) {
        if (destroyed) {
            return CompletableFuture.completedFuture(ApplyResult.err("region destroyed"));
        }
        var fut = new CompletableFuture<ApplyResult>();
        pendingConfChanges.add(fut);
        try {
            node.proposeConfChange(cc);
        } catch (RaftException | InterruptedException e) {
            pendingConfChanges.remove(fut);
            fut.complete(ApplyResult.err(e.getMessage()));
            if (e instanceof InterruptedException) Thread.currentThread().interrupt();
        }
        return fut;
    }

    @Override
    public CompletableFuture<Void> readIndex() {
        var fut = new CompletableFuture<Void>();
        byte[] ctx = java.util.UUID.randomUUID().toString().getBytes(java.nio.charset.StandardCharsets.UTF_8);
        var key = ByteBuffer.wrap(ctx);
        pendingReadIndices.put(key, fut);
        try {
            node.readIndex(ctx);
        } catch (RaftException | InterruptedException e) {
            pendingReadIndices.remove(key);
            fut.completeExceptionally(e);
            if (e instanceof InterruptedException) Thread.currentThread().interrupt();
        }
        fut.whenComplete((v, ex) -> {
            pendingReadIndices.remove(key);
        });
        return fut;
    }

    public boolean readyThreadAlive() {
        if (readyThread.isAlive()) return true;
        if (node instanceof DefaultNode dn) {
            Thread el = dn.getEventLoop();
            if (el != null && el.isAlive()) return true;
        }
        return false;
    }

    @Override
    public void shutdown() {
        destroyed = true;
        running = false;
        try {
            tickTimer.shutdownNow();
        } catch (Throwable e) {
            log.warn("region={} tickTimer shutdown failed: {}", regionId, e.getMessage(), e);
        }
        try {
            if (!node.stop(10, TimeUnit.SECONDS)) {
                log.warn("region={} raft node did not stop within 10s, forcing", regionId);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        if (node instanceof DefaultNode dn) {
            Thread el = dn.getEventLoop();
            if (el != null && el.isAlive()) {
                try { el.join(5_000); } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
        }
        readyThread.interrupt();
        try {
            readyThread.join(5_000);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        try { transport.close(); } catch (Throwable e) {
            log.warn("region={} transport close failed: {}", regionId, e.getMessage(), e);
        }
        // Pending proposals get aborted.
        for (var e : pendingProposals.entrySet()) {
            e.getValue().complete(ApplyResult.err("region peer shutdown"));
        }
        pendingProposals.clear();
        // Abort pending readIndex requests.
        var shutdownEx = RaftException.ErrStopped;
        for (var e : pendingReadIndices.values()) {
            e.completeExceptionally(shutdownEx);
        }
        pendingReadIndices.clear();
        for (var waiters : readIndexWaiters.values()) {
            for (var f : waiters) f.completeExceptionally(shutdownEx);
        }
        readIndexWaiters.clear();
    }

    // ============================================================
    // Apply loop
    // ============================================================

    private void readyLoop() {
        while (running) {
            try {
                Ready ready = node.ready();
                applyReady(ready);
                node.advance();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            } catch (Throwable t) {
                if (!running) return;
                log.error("region={} ready loop failure", regionId, t);
                // Brief backoff so a hard error doesn't pin a CPU.
                try { Thread.sleep(50); } catch (InterruptedException ie) { return; }
            }
        }
    }

    private void applyReady(Ready ready) throws Exception {
        // We DON'T take the coarse writer lock here anymore. Each committed
        // entry takes per-stripe writer locks for ONLY the keys it mutates
        // (see {@link ProposalKeyScope#peekKeys}), so reads on disjoint keys
        // run concurrently with apply — the main throughput win versus the
        // coarse-lock design that preceded this.
        //
        // The SI invariant ("reader's observe HAPPENS-BEFORE the prewrite's
        // max_ts read") is still preserved per key: a reader on key K holds
        // stripe(K)'s read lock for the observe; a subsequent prewrite on K
        // takes stripe(K)'s write lock and so cannot read max_ts until that
        // observe has retired.
        //
        // Region-wide entries (GC, full-range resolve, delete-range, snapshot
        // installs) fall back to the coarse writer lock — see
        // {@link #applyOneEntry} for the dispatch.
        applyReadyLocked(ready);
    }

    private void applyReadyLocked(Ready ready) throws Exception {
        var pending = new ArrayList<PendingDispatch>();
        var confChanges = new ArrayList<Eraftpb.ConfChangeV2>();
        var postFlushCallbacks = new ArrayList<Runnable>();
        boolean wroteAnything = false;

        // === Phase 0: install incoming snapshot (if any) ===
        //
        // When the leader sends a snapshot (MsgSnapshot), x-raft-lib surfaces
        // it through ready.snapshot(). We must install user-data CFs + raft
        // meta BEFORE processing log entries or advancing the raft state.
        if (ready.snapshot() != null && ready.snapshot().hasMetadata()
                && ready.snapshot().getMetadata().getIndex() > 0) {
            raftStorage.applySnapshot(ready.snapshot());
            log.info("region={} applied snapshot at index={} term={}",
                    regionId, ready.snapshot().getMetadata().getIndex(),
                    ready.snapshot().getMetadata().getTerm());
            wroteAnything = true;
        }

        // === Phase A: stage new log entries + hard state ===
        //
        // We write with sync=false: the entries land in memtable + WAL buffer
        // immediately and become visible to subsequent reads, but the WAL
        // fsync is deferred to the final flushWal at the end of this round.
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

        // === Phase B: apply committed entries — one batch PER entry ===
        //
        // Critical for SI under concurrency: the MvccApplyHandler reads
        // RocksDB state (via engine.get) when checking conflicts. If we
        // packed multiple entries into one batch, entry N's reader would see
        // RocksDB state from BEFORE this Ready cycle and miss entry N-1's
        // pending lock CF writes — two concurrent prewrites on the same key
        // could both pass their checkPrewrite.
        //
        // sync=false per entry is sufficient: RocksDB applies the batch to
        // its memtable on return from write(), so entry N+1's get() sees
        // entry N's writes. Durability is provided by the single flushWal
        // call below.
        //
        // Inv-1 still holds at the ENTRY level: applied_index is bumped in
        // the SAME batch as that entry's business mutations. The flushWal
        // fsyncs them as a unit at end-of-round, so a crash either persists
        // the whole batch (entry data + applied_index advance) or nothing.
        //
        // Per-entry per-stripe writer locks: we look up the entry's key
        // footprint with {@link ProposalKeyScope#peekKeys} and acquire ONLY
        // the corresponding stripe write-locks. Region-wide footprints
        // (GC / range delete / unscoped resolve) take the coarse lock.
        if (ready.committedEntries() != null) {
            for (var entry : ready.committedEntries()) {
                long idx = entry.getIndex();
                if (idx <= raftEngine.appliedIndex()) continue; // already applied (replay)

                if (applyOneEntry(entry, pending, confChanges, postFlushCallbacks, idx)) {
                    wroteAnything = true;
                }
            }
        }

        // === ReadIndex processing ===
        //
        // Each ReadState returned by the raft library represents a completed
        // readIndex quorum check. Its index() is the commit index at the time
        // the leader confirmed its authority. We can serve the read once
        // appliedIndex >= that index.
        if (ready.readStates() != null) {
            for (var rs : ready.readStates()) {
                var key = ByteBuffer.wrap(rs.requestCtx());
                var readFut = pendingReadIndices.remove(key);
                if (readFut == null || readFut.isDone()) continue;
                long targetIndex = rs.index();
                if (raftEngine.appliedIndex() >= targetIndex) {
                    readFut.complete(null);
                } else {
                    readIndexWaiters.computeIfAbsent(targetIndex, k -> new ArrayList<>()).add(readFut);
                }
            }
        }

        // Drain readIndex waiters whose target index is now satisfied.
        drainReadIndexWaiters();

        // === Opportunistic max_ts persistence ===
        //
        // The apply round runs under cm.withWriter() — ALL stripe write
        // locks are held, so NO reader can hold a read lock and bump max_ts
        // concurrently. The current value therefore reflects every observe()
        // call that completed before this round started. Persisting it now
        // (in this round's fsync) ensures a restarted leader resumes with
        // at least this floor and won't serve a prewrite whose commit_ts
        // sinks below an in-flight reader's read_ts.
        //
        // Reads that arrive AFTER this round are NOT captured here; they
        // get the next round's flush, or — for an idle leader — the leader
        // change refresh path (PD TSO). The window of loss is bounded by
        // the inter-apply interval, which is sub-second under normal load.
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

        // === Single fsync for the entire Ready round (durability barrier) ===
        //
        // Must complete BEFORE this method returns so that node.advance() in
        // the caller doesn't tell raft "persisted" until the data is on disk.
        if (wroteAnything) {
            storage.flushWal(true);
        }

        for (var cb : postFlushCallbacks) {
            try { cb.run(); }
            catch (Throwable t) { log.warn("region={} postFlush callback failed", regionId, t); }
        }

        // === Post-write fan-out ===

        raftStorage.postApply(ready.hardState());

        for (var pd : pending) {
            var fut = pendingProposals.remove(pd.proposeSeq);
            if (fut != null) fut.complete(pd.result);
        }

        // Conf-change apply (after the batch flush).
        for (var cc : confChanges) {
            applyConfChangeOne(cc);
        }

        // 5) Send out raft messages.
        if (ready.messages() != null) {
            for (var msg : ready.messages()) {
                if (msg.getTo() == self.getId()) continue;
                transport.send(msg.getTo(), msg);
            }
        }
    }

    /**
     * Apply one committed entry under the right writer-lock scope.
     *
     * <ul>
     *   <li>Normal data entries → peek key set, acquire the per-key stripes'
     *       writer locks (or coarse if footprint is region-wide).</li>
     *   <li>Conf-change entries → no business writes; coarse lock not needed,
     *       handler runs lock-free (the raft library serializes conf changes
     *       at the propose layer).</li>
     *   <li>Empty data → heartbeat / leader-elected marker, no work.</li>
     * </ul>
     *
     * <p>Returns true if any bytes were written (used to decide whether to
     * fsync at end-of-round).
     */
    private boolean applyOneEntry(Eraftpb.Entry entry,
                                   List<PendingDispatch> pending,
                                   List<Eraftpb.ConfChangeV2> confChanges,
                                   List<Runnable> postFlushCallbacks,
                                   long idx) throws Exception {
        if (entry.getEntryType() == Eraftpb.EntryType.EntryConfChange) {
            var cc = Eraftpb.ConfChange.parseFrom(entry.getData());
            var ccv2 = Eraftpb.ConfChangeV2.newBuilder()
                    .addChanges(Eraftpb.ConfChangeSingle.newBuilder()
                            .setType(cc.getChangeType())
                            .setNodeId(cc.getNodeId()))
                    .build();
            confChanges.add(ccv2);
            // Persist applied_index in its own batch (no business writes).
            try (var batch = storage.newWriteBatch()) {
                raftEngine.saveAppliedIndex(idx, batch);
                storage.write(batch, false);
            }
            return true;
        }
        if (entry.getEntryType() == Eraftpb.EntryType.EntryConfChangeV2) {
            confChanges.add(Eraftpb.ConfChangeV2.parseFrom(entry.getData()));
            try (var batch = storage.newWriteBatch()) {
                raftEngine.saveAppliedIndex(idx, batch);
                storage.write(batch, false);
            }
            return true;
        }
        // Normal entry
        if (entry.getData().size() == 0) {
            // Empty-data marker (heartbeat); still bump applied_index so the
            // log doesn't replay it endlessly on restart.
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

        // Merge-quiescence gate: while this region is mid-merge (PrepareMerge
        // applied), reject any new business write so concurrent writes can't
        // sneak past the merge boundary. Only RollbackMerge / CommitMerge
        // (admin) and PrepareMerge replays are allowed through.
        if (raftEngine.isMerging() && !isMergeAdminKind(decoded.kind())) {
            try (var batch = storage.newWriteBatch()) {
                raftEngine.saveAppliedIndex(idx, batch);
                storage.write(batch, false);
            }
            if (decoded.proposeSeq() != 0) {
                pending.add(new PendingDispatch(decoded.proposeSeq(),
                        ApplyResult.err("region merging — write rejected")));
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
            // Tests that skip the concurrency manager — fall back to raw apply.
            work.get();
        } else if (keys.isEmpty()) {
            // Region-wide footprint → coarse lock.
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
        ApplyResult outcome = r.success() ? ApplyResult.ok(r.response()) : ApplyResult.err(r.errorMessage());
        if (decoded.proposeSeq() != 0) {
            pending.add(new PendingDispatch(decoded.proposeSeq(), outcome));
        }
        if (r.postFlush() != null) {
            postFlushCallbacks.add(r.postFlush());
        }
    }

    private void tickSafely() {
        if (!running) return;
        try { node.tick(); }
        catch (Throwable t) { log.warn("region={} tick failed", regionId, t); }
    }

    // ============================================================
    // Envelope: prepend the proposeSeq onto whatever the caller built.
    // ============================================================

    private static byte[] withSeq(byte[] inner, long seq) {
        // ProposalCodec.encode already produces [version=1B][kind=1B][seq=8B][len=4B][payload]
        // The caller passed the inner-payload-only (kind + payload bytes). Re-wrap with seq.
        // For simplicity v2: caller MUST hand us the full envelope minus seq;
        // we rewrite bytes 2..9 with our seq. Done by reconstructing.
        if (inner == null || inner.length < 6) {
            throw new IllegalArgumentException("malformed proposal payload");
        }
        // inner[0] = VERSION, inner[1] = kind, inner[2..9] = seq (caller-stamped 0), inner[10..]
        var copy = inner.clone();
        copy[2] = (byte) (seq >>> 56);
        copy[3] = (byte) (seq >>> 48);
        copy[4] = (byte) (seq >>> 40);
        copy[5] = (byte) (seq >>> 32);
        copy[6] = (byte) (seq >>> 24);
        copy[7] = (byte) (seq >>> 16);
        copy[8] = (byte) (seq >>> 8);
        copy[9] = (byte) (seq);
        return copy;
    }

    /**
     * One conf-change entry has committed; apply it locally:
     * <ol>
     *   <li>Tell raft library so its peer list updates (returns new ConfState).</li>
     *   <li>Persist the new ConfState.</li>
     *   <li>Update this region's descriptor with the resulting peer list.
     *       Context carries the {@code metapb.Peer}s being added / removed so
     *       we know each one's store_id, which the raft-level
     *       ConfChangeSingle alone doesn't tell us.</li>
     *   <li>Fire {@link ChangePeerObserver} so the host {@code Store} can
     *       spawn a new RegionPeer (for AddNode on its store) or destroy an
     *       existing one (for RemoveNode of its peer).</li>
     *   <li>Complete the head of {@link #pendingConfChanges} so the
     *       caller's future resolves.</li>
     * </ol>
     */
    private void applyConfChangeOne(Eraftpb.ConfChangeV2 cc) {
        Eraftpb.ConfState newCs;
        try { newCs = node.applyConfChange(cc); }
        catch (Exception e) {
            log.warn("region={} applyConfChange failed: {}", regionId, e.getMessage());
            var fut = pendingConfChanges.poll();
            if (fut != null) fut.complete(ApplyResult.err(e.getMessage()));
            return;
        }

        // Parse context to recover the affected metapb.Peer entries — one per
        // ConfChangeSingle in change-order. If the context is missing/malformed
        // we still persist the new ConfState but can't update the region
        // descriptor (best-effort, never lose on disk).
        java.util.List<Metapb.Peer> ctxPeers = java.util.List.of();
        if (cc.getContext().size() > 0) {
            try {
                ctxPeers = io.github.xinfra.lab.xkv.proto.KvServerpb.ConfChangeContext
                        .parseFrom(cc.getContext()).getPeersList();
            } catch (Throwable t) {
                log.warn("region={} conf-change context parse failed: {}", regionId, t.getMessage());
            }
        }

        // Compute updated region descriptor by replaying each change against
        // the current peer list.
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
                    default -> { /* UpdateNode / UNRECOGNIZED — no-op */ }
                }
            }
            for (var p : peerMap.values()) b.addPeers(p);
            // ConfChange bumps conf_ver (NOT version — that's split's domain).
            b.setRegionEpoch(region.getRegionEpoch().toBuilder()
                    .setConfVer(region.getRegionEpoch().getConfVer() + 1));
            updatedRegion = b.build();
        }

        // Persist new ConfState + updated region descriptor in one batch.
        try (var batch2 = storage.newWriteBatch()) {
            raftStorage.postApplyConfState(newCs, batch2);
            if (updatedRegion != region) {
                raftEngine.saveRegion(updatedRegion, batch2);
            }
            storage.write(batch2, true);
        }
        if (updatedRegion != region) {
            updateRegion(updatedRegion);
        }

        // Fire the change-peer observer so the host {@code Store} can
        // spawn / destroy peers for the changes that target it.
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
        if (fut != null) fut.complete(ApplyResult.ok(new byte[0]));
    }

    /**
     * Complete all pending readIndex futures whose target apply-index has been
     * reached. Called from the ready loop after committed entries are applied.
     */
    private void drainReadIndexWaiters() {
        long applied = raftEngine.appliedIndex();
        var satisfied = readIndexWaiters.headMap(applied, true);
        for (var entry : satisfied.entrySet()) {
            for (var fut : entry.getValue()) {
                if (!fut.isDone()) fut.complete(null);
            }
        }
        satisfied.clear();
    }

    private static boolean isMergeAdminKind(ProposalCodec.Kind k) {
        return k == ProposalCodec.Kind.ADMIN_PREPARE_MERGE
                || k == ProposalCodec.Kind.ADMIN_ROLLBACK_MERGE
                || k == ProposalCodec.Kind.ADMIN_COMMIT_MERGE;
    }

    private record PendingDispatch(long proposeSeq, ApplyResult result) {}

    /** Per-region runtime tunables. Defaults align with KvConfig.RaftConfig. */
    public record Settings(int electionTick, int heartbeatTick, long heartbeatTickMs) {
        public static Settings defaults() { return new Settings(10, 1, 100); }
    }
}
