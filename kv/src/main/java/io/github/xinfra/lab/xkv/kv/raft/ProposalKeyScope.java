package io.github.xinfra.lab.xkv.kv.raft;

import com.google.protobuf.InvalidProtocolBufferException;
import io.github.xinfra.lab.xkv.proto.Kvrpcpb;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * Peek at the set of user-keys an incoming proposal will mutate, BEFORE
 * the apply handler runs. The apply loop uses this to acquire ONLY the
 * relevant per-key stripe write-locks instead of the coarse "all stripes"
 * lock — reads on disjoint keys then run concurrently with apply.
 *
 * <p>Returns {@link Optional#empty()} when the proposal's footprint is
 * unknown / region-wide (full-CF scans, range deletes, raft-only admin
 * entries). The caller falls back to the coarse writer lock in those
 * cases.
 *
 * <p>This is a pure decode — no business state is mutated. Failures to
 * decode return empty (caller will then take the coarse lock and the
 * handler itself will produce the proper error response).
 */
public final class ProposalKeyScope {

    private ProposalKeyScope() {}

    public static Optional<List<byte[]>> peekKeys(ProposalCodec.Decoded decoded) {
        try {
            return switch (decoded.kind()) {
                case MVCC_PREWRITE -> {
                    var req = Kvrpcpb.PrewriteRequest.parseFrom(decoded.payload());
                    var ks = new ArrayList<byte[]>(req.getMutationsCount());
                    for (var m : req.getMutationsList()) ks.add(m.getKey().toByteArray());
                    yield Optional.of(ks);
                }
                case MVCC_COMMIT -> {
                    var req = Kvrpcpb.CommitRequest.parseFrom(decoded.payload());
                    var ks = new ArrayList<byte[]>(req.getKeysCount());
                    for (var k : req.getKeysList()) ks.add(k.toByteArray());
                    yield Optional.of(ks);
                }
                case MVCC_ROLLBACK -> {
                    var req = Kvrpcpb.BatchRollbackRequest.parseFrom(decoded.payload());
                    var ks = new ArrayList<byte[]>(req.getKeysCount());
                    for (var k : req.getKeysList()) ks.add(k.toByteArray());
                    yield Optional.of(ks);
                }
                case MVCC_PESSIMISTIC_LOCK -> {
                    var req = Kvrpcpb.PessimisticLockRequest.parseFrom(decoded.payload());
                    var ks = new ArrayList<byte[]>(req.getMutationsCount());
                    for (var m : req.getMutationsList()) ks.add(m.getKey().toByteArray());
                    yield Optional.of(ks);
                }
                case MVCC_PESSIMISTIC_ROLLBACK -> {
                    var req = Kvrpcpb.PessimisticRollbackRequest.parseFrom(decoded.payload());
                    var ks = new ArrayList<byte[]>(req.getKeysCount());
                    for (var k : req.getKeysList()) ks.add(k.toByteArray());
                    yield Optional.of(ks);
                }
                case MVCC_CHECK_TXN_STATUS -> {
                    var req = Kvrpcpb.CheckTxnStatusRequest.parseFrom(decoded.payload());
                    yield Optional.of(List.of(req.getPrimaryKey().toByteArray()));
                }
                case MVCC_TXN_HEARTBEAT -> {
                    var req = Kvrpcpb.TxnHeartBeatRequest.parseFrom(decoded.payload());
                    yield Optional.of(List.of(req.getPrimaryLock().toByteArray()));
                }
                case MVCC_CHECK_SECONDARY_LOCKS -> {
                    var req = Kvrpcpb.CheckSecondaryLocksRequest.parseFrom(decoded.payload());
                    var ks = new ArrayList<byte[]>(req.getKeysCount());
                    for (var k : req.getKeysList()) ks.add(k.toByteArray());
                    yield Optional.of(ks);
                }
                case MVCC_RESOLVE -> {
                    var req = Kvrpcpb.ResolveLockRequest.parseFrom(decoded.payload());
                    if (req.getKeysCount() == 0) yield Optional.empty();  // full scan
                    var ks = new ArrayList<byte[]>(req.getKeysCount());
                    for (var k : req.getKeysList()) ks.add(k.toByteArray());
                    yield Optional.of(ks);
                }
                case RAW_PUT -> {
                    var op = RawKvCodec.decodePut(decoded.payload());
                    yield Optional.of(List.of(op.key()));
                }
                case RAW_DELETE -> {
                    var op = RawKvCodec.decodeDelete(decoded.payload());
                    yield Optional.of(List.of(op.key()));
                }
                case RAW_CAS -> {
                    var req = Kvrpcpb.RawCASRequest.parseFrom(decoded.payload());
                    yield Optional.of(List.of(req.getKey().toByteArray()));
                }
                // Region-wide footprints — fall back to coarse lock.
                case MVCC_GC, TXN_DELETE_RANGE, RAW_DELETE_RANGE -> Optional.empty();
                // Admin: raft-state only, no business CF writes.
                case ADMIN_COMPACT_LOG, ADMIN_SPLIT, ADMIN_PREPARE_MERGE,
                        ADMIN_COMMIT_MERGE, ADMIN_ROLLBACK_MERGE -> Optional.of(List.of());
            };
        } catch (InvalidProtocolBufferException e) {
            // Decode will fail again inside the apply handler with the proper
            // error response — just take the coarse lock here defensively.
            return Optional.empty();
        }
    }

    /** Used by tests / diagnostics — convert a list of keys to a printable form. */
    @SuppressWarnings("unused")
    private static String hex(byte[] k) {
        var sb = new StringBuilder(k.length * 2);
        for (byte b : k) sb.append(String.format("%02x", b));
        return sb.toString();
    }

    /** Used to bound stripe selection where ByteBuffer is preferable to byte[]. */
    @SuppressWarnings("unused")
    private static ByteBuffer wrap(byte[] k) { return ByteBuffer.wrap(k); }
}
