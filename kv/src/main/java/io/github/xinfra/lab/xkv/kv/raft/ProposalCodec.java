package io.github.xinfra.lab.xkv.kv.raft;

import java.nio.ByteBuffer;

/**
 * Wire format for raft entry payloads.
 *
 * <pre>
 *   [version=1B=0x01][kind=1B][proposeSeq=8B][len=4B][payload...]
 * </pre>
 *
 * <p>{@code proposeSeq} is the per-RegionPeer monotonically-increasing
 * counter the proposer assigns when calling {@code propose}. The apply
 * loop uses it to match the applied entry back to the waiting future
 * (since x-raft-lib does not give us a callback identity in {@code Ready}).
 *
 * <p>{@code kind} is the operation tag — RAW_PUT, RAW_DELETE, MVCC_OP, ...
 * The apply handler dispatches by kind.
 *
 * <p>v1 used a giant {@code NormalRequest} protobuf for every payload;
 * v2 keeps the raft-level envelope tight (14 bytes overhead) and lets
 * each kind define its own format.
 */
public final class ProposalCodec {

    public static final byte VERSION = 0x01;

    public enum Kind {
        RAW_PUT((byte) 0x10),
        RAW_DELETE((byte) 0x11),
        RAW_DELETE_RANGE((byte) 0x12),
        RAW_CAS((byte) 0x13),
        // Phase 2:
        MVCC_PREWRITE((byte) 0x20),
        MVCC_COMMIT((byte) 0x21),
        MVCC_ROLLBACK((byte) 0x22),
        MVCC_PESSIMISTIC_LOCK((byte) 0x23),
        MVCC_PESSIMISTIC_ROLLBACK((byte) 0x24),
        MVCC_RESOLVE((byte) 0x25),
        MVCC_GC((byte) 0x26),
        MVCC_CHECK_TXN_STATUS((byte) 0x27),
        MVCC_TXN_HEARTBEAT((byte) 0x28),
        MVCC_CHECK_SECONDARY_LOCKS((byte) 0x29),
        // Admin:
        ADMIN_SPLIT((byte) 0x30),
        ADMIN_PREPARE_MERGE((byte) 0x31),
        ADMIN_COMMIT_MERGE((byte) 0x32),
        ADMIN_ROLLBACK_MERGE((byte) 0x33),
        ADMIN_COMPACT_LOG((byte) 0x34),
        // DDL-style bulk delete (bypasses MVCC).
        TXN_DELETE_RANGE((byte) 0x35),
        ;
        final byte tag;
        Kind(byte tag) { this.tag = tag; }
        public byte tag() { return tag; }

        public static Kind fromTag(byte b) {
            for (var k : values()) if (k.tag == b) return k;
            throw new IllegalArgumentException("unknown proposal kind " + b);
        }
    }

    public record Decoded(Kind kind, long proposeSeq, byte[] payload) {}

    private ProposalCodec() {}

    public static byte[] encode(Kind kind, long proposeSeq, byte[] payload) {
        if (payload == null) payload = new byte[0];
        var bb = ByteBuffer.allocate(1 + 1 + 8 + 4 + payload.length);
        bb.put(VERSION);
        bb.put(kind.tag);
        bb.putLong(proposeSeq);
        bb.putInt(payload.length);
        bb.put(payload);
        return bb.array();
    }

    public static Decoded decode(byte[] entryData) {
        if (entryData == null || entryData.length < 14) {
            throw new IllegalArgumentException("payload too short: " +
                    (entryData == null ? -1 : entryData.length));
        }
        var bb = ByteBuffer.wrap(entryData);
        byte version = bb.get();
        if (version != VERSION) {
            throw new IllegalArgumentException("unknown payload version " + version);
        }
        var kind = Kind.fromTag(bb.get());
        long seq = bb.getLong();
        int len = bb.getInt();
        if (len < 0 || len > entryData.length - 14) {
            throw new IllegalArgumentException("bad payload length " + len);
        }
        var inner = new byte[len];
        bb.get(inner);
        return new Decoded(kind, seq, inner);
    }
}
