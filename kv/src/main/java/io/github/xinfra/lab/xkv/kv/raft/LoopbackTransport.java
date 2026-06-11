package io.github.xinfra.lab.xkv.kv.raft;

import io.github.xinfra.lab.raft.Transport;
import io.github.xinfra.lab.raft.proto.Eraftpb;

/**
 * No-op {@link Transport} for single-peer Raft groups (Phase 1 single-region
 * tests). All sent messages are dropped. Receiver is not notified — there
 * is no remote peer to receive from.
 *
 * <p>Phase 4 will replace this with a gRPC-backed transport that routes
 * via the {@code KvRaft.Raft} stream.
 */
public final class LoopbackTransport implements Transport {

    private MessageReceiver receiver;

    @Override public void setReceiver(MessageReceiver r) { this.receiver = r; }
    @Override public void addPeer(long id, String addr) { /* no-op */ }
    @Override public void removePeer(long id) { /* no-op */ }
    @Override public void send(long to, Eraftpb.Message msg) { /* no-op */ }
    @Override public void start() { /* no-op */ }
    @Override public void close() { /* no-op */ }

    public MessageReceiver receiver() { return receiver; }
}
