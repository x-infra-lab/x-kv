package io.github.xinfra.lab.xkv.pd.raft;

import io.github.xinfra.lab.raft.Config;
import io.github.xinfra.lab.raft.Node;
import io.github.xinfra.lab.raft.Peer;
import io.github.xinfra.lab.raft.RaftException;
import io.github.xinfra.lab.raft.RaftStateType;
import io.github.xinfra.lab.raft.Ready;
import io.github.xinfra.lab.raft.Transport;
import io.github.xinfra.lab.raft.proto.Eraftpb;
import io.github.xinfra.lab.raft.storage.rocksdb.RocksDbStorage;
import io.github.xinfra.lab.xkv.pd.state.PdStateMachine;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Wraps x-raft-lib's {@link Node} for the PD consensus group.
 *
 * <p>Much simpler than the KV-side {@code RegionPeerImpl}: no per-CF apply,
 * no MVCC, no split/merge. The PD state is small and all mutations are
 * serialized through raft; the state machine is deterministic.
 *
 * <p>Uses x-raft-lib's {@link RocksDbStorage} for persistent raft log
 * and state. On restart, the state machine is restored from the latest
 * snapshot + replaying committed entries above the applied index.
 *
 * <h3>Propose → Apply contract</h3>
 *
 * <p>All PD mutations are serialized as {@code PdCommand} protobuf messages,
 * proposed through raft, and applied on every node's {@link PdStateMachine}.
 * The proposer's future resolves once the entry has been applied locally.
 *
 * <h3>Leader semantics</h3>
 *
 * <p>TSO and schedulers run only on the PD leader. {@link PdStateMachine#onBecomeLeader()}
 * and {@link PdStateMachine#onLoseLeader()} are called on leadership transitions.
 */
public final class PdRaftNode implements AutoCloseable {
    private static final Logger log = LoggerFactory.getLogger(PdRaftNode.class);

    private static final int SNAPSHOT_INTERVAL = 100;

    private final long nodeId;
    private final Node node;
    private final RocksDbStorage storage;
    private final PdStateMachine stateMachine;
    private final Transport transport;

    private final AtomicLong proposeSeq = new AtomicLong(1);
    private final ConcurrentMap<Long, CompletableFuture<byte[]>> pendingProposals = new ConcurrentHashMap<>();

    private final AtomicBoolean leader = new AtomicBoolean(false);
    private volatile long currentLeaderId;
    private volatile boolean running = true;
    private long appliedIndex;
    private volatile java.util.function.Consumer<Boolean> externalLeaderObserver;

    private final Thread readyThread;
    private final ScheduledExecutorService tickTimer;

    public PdRaftNode(long nodeId,
                      List<Peer> peers,
                      PdStateMachine stateMachine,
                      Transport transport,
                      long heartbeatTickMs,
                      Path dataDir) throws IOException {
        this.nodeId = nodeId;
        this.stateMachine = stateMachine;
        this.transport = transport;

        Path storagePath = dataDir.resolve("pd-raft-storage");
        Files.createDirectories(storagePath);
        try {
            this.storage = new RocksDbStorage(storagePath);
        } catch (org.rocksdb.RocksDBException e) {
            throw new IOException("failed to open PD raft storage at " + storagePath, e);
        }

        this.appliedIndex = storage.getApplied();
        boolean restart = storage.lastIndex() > 0;

        var config = Config.builder()
                .id(nodeId)
                .electionTick(10)
                .heartbeatTick(1)
                .storage(storage)
                .applied(appliedIndex)
                .maxSizePerMsg(1L << 20)
                .maxInflightMsgs(256)
                .checkQuorum(true)
                .preVote(true)
                .build();

        if (restart) {
            log.info("pd-raft node={} restarting from persistent state (applied={})",
                    nodeId, appliedIndex);
            this.node = Node.restartNode(config);
            restoreStateMachine();
        } else {
            log.info("pd-raft node={} fresh start with {} peers", nodeId, peers.size());
            this.node = Node.startNode(config, peers);
        }

        transport.setReceiver(msg -> {
            try { this.node.step(msg); }
            catch (Exception e) { log.warn("pd-raft step failed", e); }
        });
        transport.start();

        node.registerLeaderObserver((oldLeader, newLeader) -> {
            currentLeaderId = newLeader;
            boolean isLeader = newLeader == nodeId;
            boolean was = leader.getAndSet(isLeader);
            if (was != isLeader) {
                log.info("pd-raft node={} isLeader={} (newLeader={})", nodeId, isLeader, newLeader);
                if (isLeader) {
                    stateMachine.onBecomeLeader();
                } else {
                    stateMachine.onLoseLeader();
                }
                var cb = externalLeaderObserver;
                if (cb != null) {
                    try { cb.accept(isLeader); }
                    catch (Throwable t) { log.warn("external leader observer failed", t); }
                }
            }
        });

        this.readyThread = new Thread(this::readyLoop, "pd-raft-ready");
        this.readyThread.setDaemon(true);
        this.readyThread.start();

        this.tickTimer = Executors.newSingleThreadScheduledExecutor(r -> {
            var t = new Thread(r, "pd-raft-tick");
            t.setDaemon(true);
            return t;
        });
        this.tickTimer.scheduleAtFixedRate(this::tickSafely,
                heartbeatTickMs, heartbeatTickMs, TimeUnit.MILLISECONDS);
    }

    private void restoreStateMachine() {
        long replayFrom = storage.firstIndex();

        try {
            var snap = storage.snapshot();
            if (snap.hasMetadata() && snap.getMetadata().getIndex() > 0) {
                stateMachine.installSnapshot(snap.getData().toByteArray());
                replayFrom = snap.getMetadata().getIndex() + 1;
                log.info("pd-raft state machine restored from snapshot at index={}",
                        snap.getMetadata().getIndex());
            }
        } catch (RaftException e) {
            log.info("pd-raft no snapshot available — rebuilding from entry replay");
        }

        if (replayFrom <= appliedIndex) {
            int replayed = 0;
            try {
                var entries = storage.entries(replayFrom, appliedIndex + 1, Long.MAX_VALUE);
                for (var entry : entries) {
                    if (entry.getData().size() == 0) continue;
                    replayEntry(entry);
                    replayed++;
                }
            } catch (RaftException e) {
                log.warn("pd-raft entry replay from {} to {} failed: {}",
                        replayFrom, appliedIndex, e.getMessage());
            }
            log.info("pd-raft replayed {} entries [{}, {}]", replayed, replayFrom, appliedIndex);
        }
    }

    private void replayEntry(Eraftpb.Entry entry) {
        if (entry.getEntryType() == Eraftpb.EntryType.EntryConfChange
                || entry.getEntryType() == Eraftpb.EntryType.EntryConfChangeV2) {
            return;
        }
        byte[] data = entry.getData().toByteArray();
        if (data.length < 8) return;
        byte[] command = new byte[data.length - 8];
        System.arraycopy(data, 8, command, 0, command.length);
        try {
            stateMachine.applyCommand(command);
        } catch (Throwable t) {
            log.warn("pd-raft replay entry at index={} failed: {}",
                    entry.getIndex(), t.getMessage());
        }
    }

    public void setLeaderObserver(java.util.function.Consumer<Boolean> observer) {
        this.externalLeaderObserver = observer;
    }

    public boolean isLeader() {
        try {
            var st = node.basicStatus();
            return st != null && st.state == RaftStateType.StateLeader;
        } catch (Throwable t) {
            return leader.get();
        }
    }

    public long leaderNodeId() {
        try {
            var st = node.basicStatus();
            if (st != null && st.lead != 0) return st.lead;
        } catch (Throwable ignored) {}
        long id = currentLeaderId;
        return id != 0 ? id : (leader.get() ? nodeId : 0);
    }

    /**
     * Propose a PD command through raft. Returns a future that resolves
     * with the applied result (the serialized PdCommand, potentially enriched
     * by the apply path — e.g. allocId fills in base_id).
     */
    public CompletableFuture<byte[]> propose(byte[] command) {
        if (!isLeader()) {
            return CompletableFuture.failedFuture(
                    new RaftException(RaftException.Code.PROPOSAL_DROPPED, "not leader"));
        }
        long seq = proposeSeq.getAndIncrement();
        byte[] envelope = withSeq(command, seq);
        var fut = new CompletableFuture<byte[]>();
        pendingProposals.put(seq, fut);
        try {
            node.propose(envelope);
        } catch (RaftException | InterruptedException e) {
            pendingProposals.remove(seq);
            fut.completeExceptionally(e);
            if (e instanceof InterruptedException) Thread.currentThread().interrupt();
        }
        return fut;
    }

    // ---- Ready loop ----

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
                log.error("pd-raft ready loop failure", t);
                try { Thread.sleep(50); } catch (InterruptedException ie) { return; }
            }
        }
    }

    private void applyReady(Ready ready) {
        // Persist entries + hard state + snapshot atomically via writeBatched.
        var entries = ready.entries() != null ? ready.entries() : List.<Eraftpb.Entry>of();
        var hs = ready.hardState();
        if (hs != null && hs.equals(Eraftpb.HardState.getDefaultInstance())) hs = null;
        var snap = ready.snapshot();
        if (snap != null && (!snap.hasMetadata() || snap.getMetadata().getIndex() == 0)) snap = null;

        if (!entries.isEmpty() || hs != null || snap != null) {
            try {
                storage.writeBatched(entries, hs, snap);
            } catch (Exception e) {
                log.error("pd-raft storage writeBatched failed", e);
            }
        }

        // Install snapshot into state machine.
        if (snap != null) {
            try {
                stateMachine.installSnapshot(snap.getData().toByteArray());
                appliedIndex = snap.getMetadata().getIndex();
                storage.setApplied(appliedIndex);
                log.info("pd-raft snapshot installed at index={}", appliedIndex);
            } catch (Exception e) {
                log.error("pd-raft snapshot install failed", e);
            }
        }

        // Apply committed entries.
        if (ready.committedEntries() != null) {
            for (var entry : ready.committedEntries()) {
                if (entry.getIndex() <= appliedIndex) continue;
                if (entry.getData().size() == 0) {
                    appliedIndex = entry.getIndex();
                    storage.setApplied(appliedIndex);
                    continue;
                }
                applyEntry(entry);
                appliedIndex = entry.getIndex();
                storage.setApplied(appliedIndex);
                maybeSnapshot();
            }
        }

        // Send raft messages.
        if (ready.messages() != null) {
            for (var msg : ready.messages()) {
                if (msg.getTo() == nodeId) continue;
                transport.send(msg.getTo(), msg);
            }
        }
    }

    private void maybeSnapshot() {
        if (appliedIndex % SNAPSHOT_INTERVAL != 0) return;
        try {
            var confState = storage.initialState().confState();
            byte[] snapData = stateMachine.dumpSnapshot();
            storage.createSnapshot(appliedIndex, confState, snapData);
            if (appliedIndex > SNAPSHOT_INTERVAL) {
                storage.compact(appliedIndex - SNAPSHOT_INTERVAL);
            }
            log.debug("pd-raft snapshot created at index={}", appliedIndex);
        } catch (Exception e) {
            log.warn("pd-raft snapshot creation failed: {}", e.getMessage());
        }
    }

    private void applyEntry(Eraftpb.Entry entry) {
        if (entry.getEntryType() == Eraftpb.EntryType.EntryConfChange
                || entry.getEntryType() == Eraftpb.EntryType.EntryConfChangeV2) {
            try {
                Eraftpb.ConfState cs;
                if (entry.getEntryType() == Eraftpb.EntryType.EntryConfChange) {
                    var cc = Eraftpb.ConfChange.parseFrom(entry.getData());
                    var ccv2 = Eraftpb.ConfChangeV2.newBuilder()
                            .addChanges(Eraftpb.ConfChangeSingle.newBuilder()
                                    .setType(cc.getChangeType())
                                    .setNodeId(cc.getNodeId()))
                            .build();
                    cs = node.applyConfChange(ccv2);
                } else {
                    var ccv2 = Eraftpb.ConfChangeV2.parseFrom(entry.getData());
                    cs = node.applyConfChange(ccv2);
                }
                if (cs != null) {
                    storage.setConfState(cs);
                }
            } catch (Exception e) {
                log.warn("pd-raft conf-change apply failed: {}", e.getMessage());
            }
            return;
        }

        // Normal entry: decode seq + PdCommand.
        byte[] data = entry.getData().toByteArray();
        if (data.length < 8) return;
        long seq = decodeSeq(data);
        byte[] command = new byte[data.length - 8];
        System.arraycopy(data, 8, command, 0, command.length);

        try {
            stateMachine.applyCommand(command);
        } catch (Throwable t) {
            log.warn("pd-raft applyCommand failed: {}", t.getMessage());
        }

        // Resolve the proposer's future (only on the node that proposed).
        var fut = pendingProposals.remove(seq);
        if (fut != null) fut.complete(command);
    }

    private void tickSafely() {
        if (!running) return;
        try { node.tick(); }
        catch (Throwable t) { log.warn("pd-raft tick failed", t); }
    }

    @Override
    public void close() {
        running = false;
        try { tickTimer.shutdownNow(); } catch (Throwable ignored) {}
        try { node.stop(2, TimeUnit.SECONDS); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
        try { readyThread.join(2_000); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
        try { transport.close(); } catch (Throwable ignored) {}
        try { storage.close(); } catch (Throwable ignored) {}
        var shutdownEx = RaftException.ErrStopped;
        for (var e : pendingProposals.values()) e.completeExceptionally(shutdownEx);
        pendingProposals.clear();
    }

    // ---- Envelope: 8-byte seq prefix ----

    private static byte[] withSeq(byte[] command, long seq) {
        byte[] envelope = new byte[8 + command.length];
        envelope[0] = (byte) (seq >>> 56);
        envelope[1] = (byte) (seq >>> 48);
        envelope[2] = (byte) (seq >>> 40);
        envelope[3] = (byte) (seq >>> 32);
        envelope[4] = (byte) (seq >>> 24);
        envelope[5] = (byte) (seq >>> 16);
        envelope[6] = (byte) (seq >>> 8);
        envelope[7] = (byte) (seq);
        System.arraycopy(command, 0, envelope, 8, command.length);
        return envelope;
    }

    private static long decodeSeq(byte[] data) {
        return ((long)(data[0] & 0xFF) << 56)
                | ((long)(data[1] & 0xFF) << 48)
                | ((long)(data[2] & 0xFF) << 40)
                | ((long)(data[3] & 0xFF) << 32)
                | ((long)(data[4] & 0xFF) << 24)
                | ((long)(data[5] & 0xFF) << 16)
                | ((long)(data[6] & 0xFF) << 8)
                | ((long)(data[7] & 0xFF));
    }
}
