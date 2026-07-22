package io.github.xinfra.lab.xkv.kv.transport;

import com.google.protobuf.ByteString;
import io.github.xinfra.lab.raft.Transport;
import io.github.xinfra.lab.raft.proto.Eraftpb;
import io.github.xinfra.lab.xkv.common.metrics.XKvMetrics;
import io.github.xinfra.lab.xkv.proto.KvRaftGrpc;
import io.github.xinfra.lab.xkv.proto.KvServerpb;
import io.github.xinfra.lab.xkv.common.tls.GrpcChannelFactory;
import io.github.xinfra.lab.xkv.common.tls.TlsConfig;
import io.grpc.ManagedChannel;
import io.grpc.stub.StreamObserver;
import io.micrometer.core.instrument.Counter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

/**
 * gRPC-backed {@link Transport} for inter-store Raft messages.
 *
 * <p>Each KV store hosts ONE {@code GrpcRaftTransport} per local region peer
 * (the x-raft-lib {@code Node} owns one transport per peer; messages go
 * out via {@link #send} and inbound deliveries arrive via the
 * {@link Transport.MessageReceiver} the {@link io.github.xinfra.lab.xkv.kv.raft.RegionPeer}
 * registers).
 *
 * <p>Outbound: a per-target-peer-id channel + outbound {@link StreamObserver}
 * is created on demand. Channels are reused across regions.
 *
 * <p>Inbound: handled in {@link io.github.xinfra.lab.xkv.kv.server.KvRaftServiceImpl}
 * which routes by {@code RaftMessage.region_id} to the local region's
 * {@link io.github.xinfra.lab.xkv.kv.raft.RegionPeer} and calls
 * {@code Node.step} via the receiver registered here.
 */
public final class GrpcRaftTransport implements Transport {
    private static final Logger log = LoggerFactory.getLogger(GrpcRaftTransport.class);

    private final long regionId;
    /** id of THIS peer; outbound messages where to == self are dropped. */
    private final long selfPeerId;
    private final long selfStoreId;
    private final TlsConfig tls;

    /** Per-target-peer outbound state. Keyed by peer id; address is required to open. */
    private final ConcurrentHashMap<Long, OutboundLink> outbound = new ConcurrentHashMap<>();

    /** peerId → "host:port". Refreshed via {@link #addPeer}. */
    private final ConcurrentHashMap<Long, String> addresses = new ConcurrentHashMap<>();

    /** peerId → storeId. Populated via {@link #addPeer(long, String, long)}. */
    private final ConcurrentHashMap<Long, Long> peerStoreIds = new ConcurrentHashMap<>();

    private volatile MessageReceiver receiver;
    private volatile boolean closed = false;
    private final Counter sendErrorCounter = XKvMetrics.errorCounter("raft_transport", "send");

    public GrpcRaftTransport(long regionId, long selfPeerId, long selfStoreId) {
        this(regionId, selfPeerId, selfStoreId, null);
    }

    public GrpcRaftTransport(long regionId, long selfPeerId, long selfStoreId, TlsConfig tls) {
        this.regionId = regionId;
        this.selfPeerId = selfPeerId;
        this.selfStoreId = selfStoreId;
        this.tls = tls;
    }

    public long regionId() { return regionId; }

    @Override public void setReceiver(MessageReceiver r) { this.receiver = r; }

    /** Inbound dispatch: the gRPC server calls this when a RaftMessage for our region arrives. */
    public void deliver(Eraftpb.Message msg) {
        var r = receiver;
        if (r == null) return;
        try {
            r.receive(msg);
        } catch (Throwable t) {
            log.warn("region={} deliver failed", regionId, t);
        }
    }

    @Override
    public void addPeer(long id, String address) {
        addPeer(id, address, 0);
    }

    public void addPeer(long id, String address, long storeId) {
        if (address == null || address.isEmpty()) return;
        if (storeId != 0) peerStoreIds.put(id, storeId);
        var prev = addresses.put(id, address);
        if (prev != null && !prev.equals(address)) {
            var link = outbound.remove(id);
            if (link != null) link.close();
        }
    }

    @Override
    public void removePeer(long id) {
        addresses.remove(id);
        peerStoreIds.remove(id);
        var link = outbound.remove(id);
        if (link != null) link.close();
    }

    @Override
    public void send(long to, Eraftpb.Message msg) {
        if (closed) return;
        if (to == selfPeerId) return;     // shouldn't normally happen; safety
        var addr = addresses.get(to);
        if (addr == null) {
            log.debug("region={} no address for peer={}; dropping {}", regionId, to, msg.getMsgType());
            return;
        }
        var link = outbound.computeIfAbsent(to, peerId -> new OutboundLink(peerId, addr));
        link.send(msg);
    }

    @Override public void start() { /* lazy: links open on first send */ }

    @Override
    public void close() {
        closed = true;
        for (var link : outbound.values()) link.close();
        outbound.clear();
        addresses.clear();
        peerStoreIds.clear();
    }

    // =====================================================================
    // Outbound link: one ManagedChannel + one server-streaming StreamObserver
    // per target peer. Reuses across reconnect.
    // =====================================================================

    private final class OutboundLink {
        private final long targetPeerId;
        private final String address;

        private volatile ManagedChannel channel;
        private volatile StreamObserver<KvServerpb.RaftMessage> outStream;
        private final java.util.concurrent.locks.ReentrantLock lock = new java.util.concurrent.locks.ReentrantLock();
        private volatile boolean closing = false;

        OutboundLink(long targetPeerId, String address) {
            this.targetPeerId = targetPeerId;
            this.address = address;
        }

        void send(Eraftpb.Message msg) {
            if (closing) return;
            // Snapshots travel inline over the normal raft stream (the message
            // carries the chunk-envelope in msg.snapshot.data). This keeps the
            // follower's RawNode in the loop: it steps MsgSnapshot, surfaces
            // ready.snapshot(), and installs it through the apply path — the
            // side-channel that bypassed raft could never advance the follower.
            ensureStream();
            var s = outStream;
            if (s == null) return;
            try {
                var wire = KvServerpb.RaftMessage.newBuilder()
                        .setRegionId(regionId)
                        .setFromPeer(io.github.xinfra.lab.xkv.proto.Metapb.Peer.newBuilder()
                                .setId(selfPeerId).setStoreId(selfStoreId))
                        .setToPeer(io.github.xinfra.lab.xkv.proto.Metapb.Peer.newBuilder()
                                .setId(targetPeerId)
                                .setStoreId(peerStoreIds.getOrDefault(targetPeerId, 0L)))
                        .setMessage(ByteString.copyFrom(msg.toByteArray()))
                        .build();
                s.onNext(wire);
            } catch (Throwable t) {
                log.warn("region={} send to peer={} failed: {}", regionId, targetPeerId, t.getMessage());
                resetStream();
            }
        }

        private void ensureChannel() {
            if (channel != null) return;
            lock.lock();
            try {
                if (channel != null || closing) return;
                channel = GrpcChannelFactory.build(address, tls);
            } finally {
                lock.unlock();
            }
        }

        private void ensureStream() {
            if (outStream != null) return;
            lock.lock();
            try {
                if (outStream != null || closing) return;
                ensureChannel();
                if (channel == null) return;
                var stub = KvRaftGrpc.newStub(channel);
                // The server-streaming response is just a Done ack; we drop it.
                outStream = stub.raft(new StreamObserver<>() {
                    @Override public void onNext(KvServerpb.Done v) {}
                    @Override public void onError(Throwable t) {
                        sendErrorCounter.increment();
                        log.debug("region={} outbound stream to peer={} error: {}",
                                regionId, targetPeerId, t.getMessage());
                        resetStream();
                    }
                    @Override public void onCompleted() { resetStream(); }
                });
            } finally {
                lock.unlock();
            }
        }

        private void resetStream() {
            lock.lock();
            try {
                outStream = null;
            } finally {
                lock.unlock();
            }
        }

        void close() {
            closing = true;
            lock.lock();
            try {
                if (outStream != null) {
                    try { outStream.onCompleted(); } catch (Throwable e) {
                        log.warn("stream onCompleted failed: {}", e.getMessage());
                    }
                    outStream = null;
                }
                if (channel != null) {
                    try {
                        channel.shutdown().awaitTermination(2, TimeUnit.SECONDS);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                    channel = null;
                }
            } finally {
                lock.unlock();
            }
        }
    }

}
