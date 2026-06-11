package io.github.xinfra.lab.xkv.pd.transport;

import io.github.xinfra.lab.raft.Transport;
import io.github.xinfra.lab.raft.proto.Eraftpb;
import io.github.xinfra.lab.xkv.proto.PdInternalpb;
import io.github.xinfra.lab.xkv.proto.PDRaftGrpc;
import io.github.xinfra.lab.xkv.common.tls.GrpcChannelFactory;
import io.github.xinfra.lab.xkv.common.tls.TlsConfig;
import io.grpc.ManagedChannel;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

/**
 * gRPC-based {@link Transport} for PD-to-PD raft messages.
 *
 * <p>One instance per PD node. Each peer gets a lazily-created gRPC channel
 * and outbound stream. Messages are fire-and-forget; raft retransmission
 * handles losses.
 */
public final class PdRaftTransport implements Transport {
    private static final Logger log = LoggerFactory.getLogger(PdRaftTransport.class);

    private final long selfId;
    private final TlsConfig tls;
    private final ConcurrentHashMap<Long, String> peerAddresses = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<Long, ManagedChannel> channels = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<Long, StreamObserver<PdInternalpb.PdRaftMessage>> outbounds = new ConcurrentHashMap<>();
    private volatile MessageReceiver receiver;

    public PdRaftTransport(long selfId) {
        this(selfId, null);
    }

    public PdRaftTransport(long selfId, TlsConfig tls) {
        this.selfId = selfId;
        this.tls = tls;
    }

    @Override
    public void addPeer(long peerId, String raftAddress) {
        peerAddresses.put(peerId, raftAddress);
    }

    @Override
    public void removePeer(long peerId) {
        peerAddresses.remove(peerId);
        var stream = outbounds.remove(peerId);
        if (stream != null) {
            try { stream.onCompleted(); } catch (Throwable ignored) {}
        }
        var ch = channels.remove(peerId);
        if (ch != null) {
            try { ch.shutdownNow().awaitTermination(2, TimeUnit.SECONDS); }
            catch (InterruptedException e) { Thread.currentThread().interrupt(); }
        }
    }

    @Override
    public void setReceiver(MessageReceiver r) { this.receiver = r; }

    @Override
    public void start() {}

    /** Deliver an inbound raft message from a remote PD node. */
    public void deliver(Eraftpb.Message msg) {
        var r = receiver;
        if (r != null) r.receive(msg);
    }

    @Override
    public void send(long to, Eraftpb.Message msg) {
        if (to == selfId) return;
        try {
            var stream = ensureStream(to);
            if (stream == null) return;
            stream.onNext(PdInternalpb.PdRaftMessage.newBuilder()
                    .setMessage(msg.toByteString())
                    .build());
        } catch (Throwable t) {
            log.debug("pd-raft send to {} failed: {}", to, t.getMessage());
            outbounds.remove(to);
        }
    }

    private StreamObserver<PdInternalpb.PdRaftMessage> ensureStream(long peerId) {
        var existing = outbounds.get(peerId);
        if (existing != null) return existing;

        var addr = peerAddresses.get(peerId);
        if (addr == null) return null;

        var channel = channels.computeIfAbsent(peerId, k ->
                GrpcChannelFactory.build(addr, tls));

        var asyncStub = PDRaftGrpc.newStub(channel);
        var stream = asyncStub.raft(new StreamObserver<>() {
            @Override public void onNext(PdInternalpb.PdRaftDone v) {}
            @Override public void onError(Throwable t) {
                log.debug("pd-raft outbound stream to {} error: {}", peerId, t.getMessage());
                outbounds.remove(peerId);
            }
            @Override public void onCompleted() {
                outbounds.remove(peerId);
            }
        });

        outbounds.put(peerId, stream);
        return stream;
    }

    @Override
    public void close() {
        for (var stream : outbounds.values()) {
            try { stream.onCompleted(); } catch (Throwable ignored) {}
        }
        outbounds.clear();
        for (var ch : channels.values()) {
            try { ch.shutdownNow().awaitTermination(2, TimeUnit.SECONDS); }
            catch (InterruptedException e) { Thread.currentThread().interrupt(); }
        }
        channels.clear();
    }
}
