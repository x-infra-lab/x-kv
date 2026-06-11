package io.github.xinfra.lab.xkv.kv.server;

import io.github.xinfra.lab.xkv.common.logging.MdcContextUtil;
import io.github.xinfra.lab.xkv.kv.engine.StorageEngine;
import io.github.xinfra.lab.xkv.kv.raft.ProposalCodec;
import io.github.xinfra.lab.xkv.kv.raft.RawKvCodec;
import io.github.xinfra.lab.xkv.kv.raft.RegionPeer;
import io.github.xinfra.lab.xkv.proto.Kvrpcpb;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

/**
 * Raw KV business logic — exposed via {@link TikvServiceImpl}.
 *
 * <p>Reads go straight to {@link StorageEngine} (linearizable read is
 * achieved via the leader's read-index; Phase 1 single-peer always serves).
 * Writes go through Raft via {@link RegionPeer#propose}; the future
 * resolves only after the entry has been applied with one fsync (Inv-1).
 */
public final class RawKvService {
    private static final Logger log = LoggerFactory.getLogger(RawKvService.class);

    /** Locator: choose the right region peer for a given key. */
    public interface PeerLocator {
        /** Phase 1 single-region setup returns the same peer for all keys. */
        RegionPeer peerForKey(byte[] key);
    }

    private final StorageEngine engine;
    private final PeerLocator locator;
    private final long proposeTimeoutMs;

    public RawKvService(StorageEngine engine, PeerLocator locator, long proposeTimeoutMs) {
        this.engine = engine;
        this.locator = locator;
        this.proposeTimeoutMs = proposeTimeoutMs;
    }

    // ---- Reads (linearizable via raft readIndex) ----

    public Kvrpcpb.RawGetResponse rawGet(Kvrpcpb.RawGetRequest req) {
        var key = req.getKey().toByteArray();
        var peer = locator.peerForKey(key);
        if (peer == null) {
            return Kvrpcpb.RawGetResponse.newBuilder().setError("region not found").build();
        }
        var readErr = ensureReadable(peer, req.getContext());
        if (readErr != null) {
            return Kvrpcpb.RawGetResponse.newBuilder().setRegionError(readErr).build();
        }
        byte[] v = engine.get(StorageEngine.Cf.DEFAULT, key);
        var b = Kvrpcpb.RawGetResponse.newBuilder();
        if (v == null) {
            b.setNotFound(true);
        } else {
            b.setValue(com.google.protobuf.ByteString.copyFrom(v));
        }
        return b.build();
    }

    public Kvrpcpb.RawBatchGetResponse rawBatchGet(Kvrpcpb.RawBatchGetRequest req) {
        var keys = new ArrayList<byte[]>(req.getKeysCount());
        for (var k : req.getKeysList()) keys.add(k.toByteArray());
        if (keys.isEmpty()) return Kvrpcpb.RawBatchGetResponse.newBuilder().build();
        var peer = locator.peerForKey(keys.get(0));
        if (peer == null) return Kvrpcpb.RawBatchGetResponse.newBuilder().build();
        var readErr = ensureReadable(peer, req.getContext());
        if (readErr != null) {
            return Kvrpcpb.RawBatchGetResponse.newBuilder().setRegionError(readErr).build();
        }
        var values = engine.multiGet(StorageEngine.Cf.DEFAULT, keys);
        var resp = Kvrpcpb.RawBatchGetResponse.newBuilder();
        for (int i = 0; i < keys.size(); i++) {
            if (values.get(i) == null) continue;
            resp.addPairs(Kvrpcpb.KvPair.newBuilder()
                    .setKey(com.google.protobuf.ByteString.copyFrom(keys.get(i)))
                    .setValue(com.google.protobuf.ByteString.copyFrom(values.get(i)))
                    .build());
        }
        return resp.build();
    }

    public Kvrpcpb.RawScanResponse rawScan(Kvrpcpb.RawScanRequest req) {
        byte[] start = req.getStartKey().toByteArray();
        byte[] end   = req.getEndKey().toByteArray();
        var peer = locator.peerForKey(start == null ? new byte[0] : start);
        if (peer == null) return Kvrpcpb.RawScanResponse.newBuilder().build();
        var readErr = ensureReadable(peer, req.getContext());
        if (readErr != null) {
            return Kvrpcpb.RawScanResponse.newBuilder().setRegionError(readErr).build();
        }
        var b = Kvrpcpb.RawScanResponse.newBuilder();
        boolean keyOnly = req.getKeyOnly();
        int limit = req.getLimit() <= 0 ? Integer.MAX_VALUE : req.getLimit();

        var ro = engine.newReadOptions();
        try (var it = engine.newIterator(StorageEngine.Cf.DEFAULT, ro)) {
            if (req.getReverse()) {
                // Reverse scan: start is exclusive upper bound, end is inclusive lower bound.
                it.seekForPrev(start == null ? new byte[0] : start);
                int n = 0;
                while (it.isValid() && n < limit) {
                    var k = it.key();
                    if (start != null && start.length > 0
                            && java.util.Arrays.compareUnsigned(k, start) >= 0) {
                        it.prev();
                        continue;
                    }
                    if (end != null && end.length > 0
                            && java.util.Arrays.compareUnsigned(k, end) < 0) break;
                    var pair = Kvrpcpb.KvPair.newBuilder()
                            .setKey(com.google.protobuf.ByteString.copyFrom(k));
                    if (!keyOnly) {
                        pair.setValue(com.google.protobuf.ByteString.copyFrom(it.value()));
                    }
                    b.addKvs(pair.build());
                    it.prev();
                    n++;
                }
            } else {
                it.seek(start == null ? new byte[0] : start);
                int n = 0;
                while (it.isValid() && n < limit) {
                    var k = it.key();
                    if (end != null && end.length > 0
                            && java.util.Arrays.compareUnsigned(k, end) >= 0) break;
                    var pair = Kvrpcpb.KvPair.newBuilder()
                            .setKey(com.google.protobuf.ByteString.copyFrom(k));
                    if (!keyOnly) {
                        pair.setValue(com.google.protobuf.ByteString.copyFrom(it.value()));
                    }
                    b.addKvs(pair.build());
                    it.next();
                    n++;
                }
            }
        }
        return b.build();
    }

    // ---- Writes (go through Raft) ----

    public Kvrpcpb.RawPutResponse rawPut(Kvrpcpb.RawPutRequest req) {
        var key = req.getKey().toByteArray();
        var val = req.getValue().toByteArray();
        var peer = locator.peerForKey(key);
        if (peer == null) {
            return Kvrpcpb.RawPutResponse.newBuilder().setError("region not found").build();
        }
        if (!peer.isLeader()) {
            return Kvrpcpb.RawPutResponse.newBuilder()
                    .setRegionError(notLeaderError(peer.regionId())).build();
        }
        return propose(
                ProposalCodec.Kind.RAW_PUT,
                RawKvCodec.encodePut(key, val),
                peer,
                err -> Kvrpcpb.RawPutResponse.newBuilder().setError(err).build(),
                () -> Kvrpcpb.RawPutResponse.newBuilder().build());
    }

    public Kvrpcpb.RawDeleteResponse rawDelete(Kvrpcpb.RawDeleteRequest req) {
        var key = req.getKey().toByteArray();
        var peer = locator.peerForKey(key);
        if (peer == null) {
            return Kvrpcpb.RawDeleteResponse.newBuilder().setError("region not found").build();
        }
        if (!peer.isLeader()) {
            return Kvrpcpb.RawDeleteResponse.newBuilder()
                    .setRegionError(notLeaderError(peer.regionId())).build();
        }
        return propose(
                ProposalCodec.Kind.RAW_DELETE,
                RawKvCodec.encodeDelete(key),
                peer,
                err -> Kvrpcpb.RawDeleteResponse.newBuilder().setError(err).build(),
                () -> Kvrpcpb.RawDeleteResponse.newBuilder().build());
    }

    public Kvrpcpb.RawDeleteRangeResponse rawDeleteRange(Kvrpcpb.RawDeleteRangeRequest req) {
        var start = req.getStartKey().toByteArray();
        var end   = req.getEndKey().toByteArray();
        var peer = locator.peerForKey(start);
        if (peer == null) {
            return Kvrpcpb.RawDeleteRangeResponse.newBuilder().setError("region not found").build();
        }
        if (!peer.isLeader()) {
            return Kvrpcpb.RawDeleteRangeResponse.newBuilder()
                    .setRegionError(notLeaderError(peer.regionId())).build();
        }
        return propose(
                ProposalCodec.Kind.RAW_DELETE_RANGE,
                RawKvCodec.encodeDeleteRange(start, end),
                peer,
                err -> Kvrpcpb.RawDeleteRangeResponse.newBuilder().setError(err).build(),
                () -> Kvrpcpb.RawDeleteRangeResponse.newBuilder().build());
    }

    public Kvrpcpb.RawBatchPutResponse rawBatchPut(Kvrpcpb.RawBatchPutRequest req) {
        // Phase 1: serial; cross-region grouping lands in the client.
        var pairs = req.getPairsList();
        for (var p : pairs) {
            var key = p.getKey().toByteArray();
            var val = p.getValue().toByteArray();
            var peer = locator.peerForKey(key);
            if (peer == null) {
                return Kvrpcpb.RawBatchPutResponse.newBuilder().setError("region not found").build();
            }
            if (!peer.isLeader()) {
                return Kvrpcpb.RawBatchPutResponse.newBuilder()
                        .setRegionError(notLeaderError(peer.regionId())).build();
            }
            var resp = propose(ProposalCodec.Kind.RAW_PUT,
                    RawKvCodec.encodePut(key, val),
                    peer,
                    err -> Kvrpcpb.RawBatchPutResponse.newBuilder().setError(err).build(),
                    () -> Kvrpcpb.RawBatchPutResponse.newBuilder().build());
            if (!resp.getError().isEmpty()) return resp;
        }
        return Kvrpcpb.RawBatchPutResponse.newBuilder().build();
    }

    public Kvrpcpb.RawBatchDeleteResponse rawBatchDelete(Kvrpcpb.RawBatchDeleteRequest req) {
        for (var k : req.getKeysList()) {
            var key = k.toByteArray();
            var peer = locator.peerForKey(key);
            if (peer == null) {
                return Kvrpcpb.RawBatchDeleteResponse.newBuilder().setError("region not found").build();
            }
            if (!peer.isLeader()) {
                return Kvrpcpb.RawBatchDeleteResponse.newBuilder()
                        .setRegionError(notLeaderError(peer.regionId())).build();
            }
            var resp = propose(ProposalCodec.Kind.RAW_DELETE,
                    RawKvCodec.encodeDelete(key),
                    peer,
                    err -> Kvrpcpb.RawBatchDeleteResponse.newBuilder().setError(err).build(),
                    () -> Kvrpcpb.RawBatchDeleteResponse.newBuilder().build());
            if (!resp.getError().isEmpty()) return resp;
        }
        return Kvrpcpb.RawBatchDeleteResponse.newBuilder().build();
    }

    public Kvrpcpb.RawCASResponse rawCAS(Kvrpcpb.RawCASRequest req) {
        var key = req.getKey().toByteArray();
        var peer = locator.peerForKey(key);
        if (peer == null) {
            return Kvrpcpb.RawCASResponse.newBuilder().setError("region not found").build();
        }
        if (!peer.isLeader()) {
            return Kvrpcpb.RawCASResponse.newBuilder()
                    .setRegionError(notLeaderError(peer.regionId())).build();
        }
        var envelope = ProposalCodec.encode(ProposalCodec.Kind.RAW_CAS, /* seq= */ 0, req.toByteArray());
        try {
            var future = peer.propose(new RegionPeer.Proposal(envelope, 0, 0));
            var result = future.get(proposeTimeoutMs, TimeUnit.MILLISECONDS);
            if (!result.success()) {
                return Kvrpcpb.RawCASResponse.newBuilder().setError(result.errorMessage()).build();
            }
            try { return Kvrpcpb.RawCASResponse.parseFrom(result.response()); }
            catch (Exception e) {
                return Kvrpcpb.RawCASResponse.newBuilder().setError("decode: " + e.getMessage()).build();
            }
        } catch (Exception e) {
            log.warn("propose RAW_CAS failed: {}", e.getMessage());
            return Kvrpcpb.RawCASResponse.newBuilder().setError(e.getMessage()).build();
        }
    }

    private io.github.xinfra.lab.xkv.proto.Errorpb.Error ensureReadable(
            RegionPeer peer, Kvrpcpb.Context ctx) {
        MdcContextUtil.setRegion(peer.regionId());
        boolean followerRead = ctx != null && (
                ctx.getReplicaRead() == Kvrpcpb.ReplicaReadType.FOLLOWER
                        || ctx.getReplicaRead() == Kvrpcpb.ReplicaReadType.MIXED);
        if (!followerRead && !peer.isLeader()) return notLeaderError(peer.regionId());
        try {
            long waitMs = effectiveTimeoutMs();
            peer.readIndex().get(waitMs, TimeUnit.MILLISECONDS);
        } catch (Exception e) {
            log.warn("readIndex failed for region={}: {}", peer.regionId(), e.getMessage());
            return notLeaderError(peer.regionId());
        }
        return null;
    }

    private long effectiveTimeoutMs() {
        var ctxDeadline = io.grpc.Context.current().getDeadline();
        if (ctxDeadline == null) return proposeTimeoutMs;
        long remaining = ctxDeadline.timeRemaining(TimeUnit.MILLISECONDS);
        if (remaining <= 0) return 1;
        return Math.max(1, Math.min(proposeTimeoutMs, remaining));
    }

    // ---- Helpers ----

    @FunctionalInterface
    private interface ErrorWrapper<T> { T wrap(String error); }

    private <T> T propose(ProposalCodec.Kind kind,
                          byte[] payload,
                          RegionPeer peer,
                          ErrorWrapper<T> errorBuilder,
                          Supplier<T> okBuilder) {
        if (peer == null) return errorBuilder.wrap("region not found");
        if (!peer.isLeader()) return errorBuilder.wrap("not leader");

        var envelope = ProposalCodec.encode(kind, /* seq= */ 0, payload);
        try {
            var future = peer.propose(new RegionPeer.Proposal(envelope, 0, 0));
            var result = future.get(proposeTimeoutMs, TimeUnit.MILLISECONDS);
            if (!result.success()) return errorBuilder.wrap(result.errorMessage());
            return okBuilder.get();
        } catch (Exception e) {
            log.warn("propose {} failed: {}", kind, e.getMessage());
            return errorBuilder.wrap(e.getMessage());
        }
    }

    /** Build a NotLeader region_error for inclusion in a response. */
    public static io.github.xinfra.lab.xkv.proto.Errorpb.Error notLeaderError(long regionId) {
        return io.github.xinfra.lab.xkv.proto.Errorpb.Error.newBuilder()
                .setMessage("not leader")
                .setNotLeader(io.github.xinfra.lab.xkv.proto.Errorpb.NotLeader.newBuilder()
                        .setRegionId(regionId)
                        .build())
                .build();
    }

    /** Tail of a list with non-null elements, used by some helpers. */
    @SuppressWarnings("unused")
    private static <T> List<T> nonNullTail(List<T> in) {
        var out = new ArrayList<T>(in.size());
        for (T x : in) if (x != null) out.add(x);
        return out;
    }
}
