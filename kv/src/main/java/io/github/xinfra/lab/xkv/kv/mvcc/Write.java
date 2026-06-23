package io.github.xinfra.lab.xkv.kv.mvcc;

import java.nio.ByteBuffer;

/**
 * MVCC commit / rollback record. Stored in the {@code write} CF, keyed by
 * {@code MvccKey.encode(userKey, commitTs)} (or {@code startTs} for
 * ROLLBACK records).
 *
 * <p>v3 wire format:
 *
 * <pre>
 *   [1B  version=0x33]
 *   [1B  type: PUT|DELETE|ROLLBACK|LOCK]
 *   [8B  startTs]
 *   [1B  flags: bit0=hasOverlappedRollback]
 *   [4B  shortValueLen][shortValueLen B  shortValue]   // 0-len ⇒ no inline value
 * </pre>
 *
 * <h3>Short-value inlining</h3>
 *
 * <p>Values up to {@link #SHORT_VALUE_MAX_LEN} bytes are inlined inside
 * the Write record itself. The MVCC reader can answer {@code Get} from
 * the write CF alone — no second read against {@code default} CF. Cuts
 * snapshot-read latency roughly in half for small-value workloads.
 *
 * <h3>{@code hasOverlappedRollback}</h3>
 *
 * <p>For Async Commit / 1PC: when an in-flight transaction is aborted by
 * a concurrent reader's CheckTxnStatus, but a (delayed) prewrite of the
 * same {@code start_ts} later races in, the rollback record may be
 * written with this flag to indicate it overlaps a concurrent write.
 * Lock resolver respects the flag to avoid double-aborting.
 */
public final class Write {

    public static final byte VERSION = 0x33;
    public static final int SHORT_VALUE_MAX_LEN = 64;

    public enum Type {
        PUT(0),
        DELETE(1),
        ROLLBACK(2),
        LOCK(3);
        final byte tag;
        Type(int tag) { this.tag = (byte) tag; }
        public byte tag() { return tag; }
        public static Type fromTag(byte b) {
            for (var t : values()) if (t.tag == b) return t;
            throw new IllegalArgumentException("unknown write type " + b);
        }
    }

    private final Type type;
    private final long startTs;
    private final byte[] shortValue;          // null if not inlined
    private final boolean hasOverlappedRollback;

    private Write(Type type, long startTs, byte[] shortValue, boolean hasOverlappedRollback) {
        this.type = type;
        this.startTs = startTs;
        this.shortValue = shortValue;
        this.hasOverlappedRollback = hasOverlappedRollback;
    }

    public Type type() { return type; }
    public long startTs() { return startTs; }
    public byte[] shortValue() { return shortValue; }
    public boolean hasShortValue() { return shortValue != null; }
    public boolean hasOverlappedRollback() { return hasOverlappedRollback; }

    public byte[] encode() {
        int len = (shortValue == null) ? 0 : shortValue.length;
        var bb = ByteBuffer.allocate(1 + 1 + 8 + 1 + 4 + len);
        bb.put(VERSION);
        bb.put(type.tag);
        bb.putLong(startTs);
        bb.put((byte) (hasOverlappedRollback ? 0x01 : 0x00));
        bb.putInt(len);
        if (shortValue != null) bb.put(shortValue);
        return bb.array();
    }

    public static Write decode(byte[] bytes) {
        var bb = ByteBuffer.wrap(bytes);
        byte version = bb.get();
        if (version != VERSION) {
            throw new IllegalArgumentException("unknown write version 0x" + Integer.toHexString(version & 0xFF));
        }
        var type = Type.fromTag(bb.get());
        long startTs = bb.getLong();
        byte flags = bb.get();
        int len = bb.getInt();
        byte[] sv = null;
        if (len > 0) {
            sv = new byte[len];
            bb.get(sv);
        }
        return new Write(type, startTs, sv, (flags & 0x01) != 0);
    }

    public static Write put(long startTs)            { return new Write(Type.PUT, startTs, null, false); }
    public static Write put(long startTs, byte[] sv) { return new Write(Type.PUT, startTs, sv, false); }
    public static Write delete(long startTs)         { return new Write(Type.DELETE, startTs, null, false); }
    public static Write rollback(long startTs)       { return new Write(Type.ROLLBACK, startTs, null, false); }
    public static Write rollbackOverlapping(long startTs) { return new Write(Type.ROLLBACK, startTs, null, true); }
    public static Write lock(long startTs)           { return new Write(Type.LOCK, startTs, null, false); }

    public Write withOverlappedRollback() {
        return new Write(type, startTs, shortValue, true);
    }
}
