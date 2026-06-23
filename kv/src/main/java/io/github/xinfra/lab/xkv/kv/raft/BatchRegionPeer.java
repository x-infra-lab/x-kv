package io.github.xinfra.lab.xkv.kv.raft;

import io.github.xinfra.lab.raft.Peer;
import io.github.xinfra.lab.raft.Transport;
import io.github.xinfra.lab.raft.proto.Eraftpb;
import io.github.xinfra.lab.xkv.kv.engine.PerRegionRaftEngine;
import io.github.xinfra.lab.xkv.kv.engine.StorageEngine;
import io.github.xinfra.lab.xkv.kv.mvcc.ConcurrencyManager;
import io.github.xinfra.lab.xkv.proto.Metapb;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.concurrent.CompletableFuture;

/**
 * BatchSystem-backed {@link RegionPeer} implementation.
 *
 * <p>Delegates all work to a {@link RegionMailbox} processed by a shared
 * {@link RaftPoller}. No per-region threads are created — the mailbox
 * queues events and the poller drives the underlying {@code RawNode}.
 *
 * <p>Implements the same {@code RegionPeer} interface as
 * {@link RegionPeerImpl}, so callers (KvServer, TransactionService, etc.)
 * are unaware of the threading model.
 */
public final class BatchRegionPeer implements RegionPeer {

    private final RegionMailbox mailbox;
    private final PerRegionRaftEngine raftEngine;

    public BatchRegionPeer(StorageEngine storage,
                           PerRegionRaftEngine raftEngine,
                           Metapb.Region region,
                           Metapb.Peer self,
                           List<Peer> peers,
                           Transport transport,
                           ApplyHandler applyHandler,
                           RegionPeerImpl.Settings settings,
                           ConcurrencyManager cm,
                           io.github.xinfra.lab.xkv.kv.engine.SnapshotEngine snapshotEngine,
                           RaftPoller poller,
                           TickDriver tickDriver) {
        this.raftEngine = raftEngine;
        this.mailbox = new RegionMailbox(storage, raftEngine, region, self, peers,
                transport, applyHandler, settings, cm, snapshotEngine);
        this.mailbox.setPoller(poller);
        tickDriver.register(mailbox);
        // Initial wakeup to process any bootstrap Ready.
        mailbox.wakeup();
    }

    public RegionMailbox mailbox() { return mailbox; }

    public void setChangePeerObserver(RegionPeerImpl.ChangePeerObserver obs) {
        mailbox.changePeerObserver = obs;
    }

    @Override public long regionId() { return mailbox.regionId; }
    @Override public Metapb.Region region() { return mailbox.region; }
    @Override public Metapb.Peer self() { return mailbox.self; }

    @Override
    public boolean isLeader() {
        return mailbox.isLeader();
    }

    @Override public boolean isDestroyed() { return mailbox.destroyed; }
    @Override public long firstIndex() { return raftEngine.firstIndex(); }
    @Override public long appliedIndex() { return raftEngine.appliedIndex(); }

    @Override public long maxTs() {
        if (mailbox.cm == null) return 0;
        return Math.max(mailbox.cm.maxTs().current(), mailbox.cm.safeTs());
    }

    @Override public long currentTerm() { return raftEngine.currentTerm(); }
    @Override public long votedFor() { return raftEngine.votedFor(); }
    @Override public long commitIndex() { return raftEngine.commitIndex(); }
    @Override public long lastIndex() { return raftEngine.lastIndex(); }

    @Override
    public void maybeGenerateSnapshot() {
        mailbox.maybeGenerateSnapshot();
    }

    @Override
    public void updateRegion(Metapb.Region newRegion) {
        mailbox.region = newRegion;
    }

    @Override
    public void transferLeader(long targetPeerId) {
        mailbox.events.offer(new RegionMailbox.TransferLeaderEvent(targetPeerId));
        mailbox.wakeup();
    }

    @Override
    public CompletableFuture<ApplyResult> propose(Proposal p) {
        if (mailbox.destroyed) {
            return CompletableFuture.completedFuture(ApplyResult.err("region destroyed"));
        }
        if (!isLeader()) {
            return CompletableFuture.completedFuture(ApplyResult.err("not leader"));
        }
        long seq = mailbox.proposeSeq.getAndIncrement();
        byte[] envelope = withSeq(p.payload(), seq);
        var fut = new CompletableFuture<ApplyResult>();
        mailbox.pendingProposals.put(seq, fut);
        mailbox.events.offer(new RegionMailbox.ProposeEvent(envelope, seq));
        mailbox.wakeup();
        return fut;
    }

    @Override
    public CompletableFuture<ApplyResult> proposeAdmin(AdminProposal p) {
        return propose(new Proposal(p.payload(), 0, 0));
    }

    @Override
    public CompletableFuture<ApplyResult> proposeConfChange(Eraftpb.ConfChangeV2 cc) {
        if (mailbox.destroyed) {
            return CompletableFuture.completedFuture(ApplyResult.err("region destroyed"));
        }
        var fut = new CompletableFuture<ApplyResult>();
        mailbox.pendingConfChanges.add(fut);
        mailbox.events.offer(new RegionMailbox.ConfChangeEvent(cc));
        mailbox.wakeup();
        return fut;
    }

    @Override
    public CompletableFuture<Void> readIndex() {
        var fut = new CompletableFuture<Void>();
        byte[] ctx = java.util.UUID.randomUUID().toString()
                .getBytes(java.nio.charset.StandardCharsets.UTF_8);
        var key = ByteBuffer.wrap(ctx);
        mailbox.pendingReadIndices.put(key, fut);
        mailbox.events.offer(new RegionMailbox.ReadIndexEvent(ctx));
        mailbox.wakeup();
        fut.whenComplete((v, ex) -> mailbox.pendingReadIndices.remove(key));
        return fut;
    }

    @Override
    public void shutdown() {
        mailbox.shutdown();
    }

    public RegionRaftStorage raftStorage() { return mailbox.raftStorage; }

    // Envelope helper — same as RegionPeerImpl.withSeq
    private static byte[] withSeq(byte[] inner, long seq) {
        if (inner == null || inner.length < 6) {
            throw new IllegalArgumentException("malformed proposal payload");
        }
        var copy = inner.clone();
        copy[2] = (byte) (seq >>> 56);
        copy[3] = (byte) (seq >>> 48);
        copy[4] = (byte) (seq >>> 40);
        copy[5] = (byte) (seq >>> 32);
        copy[6] = (byte) (seq >>> 24);
        copy[7] = (byte) (seq >>> 16);
        copy[8] = (byte) (seq >>> 8);
        copy[9] = (byte) seq;
        return copy;
    }
}
