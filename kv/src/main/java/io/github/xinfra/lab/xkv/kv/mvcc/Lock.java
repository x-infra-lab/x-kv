package io.github.xinfra.lab.xkv.kv.mvcc;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

/**
 * Percolator lock record. Stored in the {@code lock} CF; one entry per
 * locked userKey at most.
 *
 * <p>v3 wire format (binary, big-endian where multi-byte):
 *
 * <pre>
 *   [1B  version=0x33]
 *   [1B  type: PUT|DELETE|LOCK|PESSIMISTIC]
 *   [8B  startTs]
 *   [8B  ttlMs]
 *   [8B  txnSize]
 *   [8B  minCommitTs]
 *   [8B  forUpdateTs (0 if optimistic)]
 *   [1B  flags: bit0=useAsyncCommit]
 *   [4B  primaryLen][primaryLen B  primary]
 *   [4B  secondariesCount]
 *     for each secondary:
 *       [4B  keyLen][keyLen B  key]
 * </pre>
 *
 * <p>Difference from v1's v2 format:
 * <ul>
 *   <li><b>{@code forUpdateTs}</b> — needed for pessimistic locks. v1 had
 *       this only for the lock-CF runtime data, not in the persisted
 *       binary; conflict detection on restart was lossy.</li>
 *   <li><b>flags / async-commit secondaries</b> — required to make the
 *       async-commit path actually work. v1's proto declared the option
 *       but the on-disk lock did not carry the secondaries list, so
 *       lock resolver had no way to verify all secondaries on
 *       primary-commit recovery.</li>
 * </ul>
 */
public final class Lock {

    public static final byte VERSION = 0x33;

    public enum Type {
        PUT(0),
        DELETE(1),
        LOCK(2),
        PESSIMISTIC(3);
        final byte tag;
        Type(int tag) { this.tag = (byte) tag; }
        public byte tag() { return tag; }
        public static Type fromTag(byte b) {
            for (var t : values()) if (t.tag == b) return t;
            throw new IllegalArgumentException("unknown lock type " + b);
        }
    }

    private final Type type;
    private final byte[] primary;
    private final long startTs;
    private final long ttlMs;
    private final long txnSize;
    private final long minCommitTs;
    private final long forUpdateTs;
    private final boolean useAsyncCommit;
    private final List<byte[]> secondaries;

    private Lock(Builder b) {
        this.type = b.type;
        this.primary = b.primary;
        this.startTs = b.startTs;
        this.ttlMs = b.ttlMs;
        this.txnSize = b.txnSize;
        this.minCommitTs = b.minCommitTs;
        this.forUpdateTs = b.forUpdateTs;
        this.useAsyncCommit = b.useAsyncCommit;
        this.secondaries = b.secondaries == null ? List.of() : List.copyOf(b.secondaries);
    }

    public Type type() { return type; }
    public byte[] primary() { return primary; }
    public long startTs() { return startTs; }
    public long ttlMs() { return ttlMs; }
    public long txnSize() { return txnSize; }
    public long minCommitTs() { return minCommitTs; }
    public long forUpdateTs() { return forUpdateTs; }
    public boolean useAsyncCommit() { return useAsyncCommit; }
    public List<byte[]> secondaries() { return secondaries; }

    public boolean isPessimistic() { return type == Type.PESSIMISTIC; }

    public byte[] encode() {
        int total = 1 + 1 + 8 + 8 + 8 + 8 + 8 + 1 + 4 + primary.length + 4;
        for (var s : secondaries) total += 4 + s.length;
        var bb = ByteBuffer.allocate(total);
        bb.put(VERSION);
        bb.put(type.tag);
        bb.putLong(startTs);
        bb.putLong(ttlMs);
        bb.putLong(txnSize);
        bb.putLong(minCommitTs);
        bb.putLong(forUpdateTs);
        bb.put((byte) (useAsyncCommit ? 0x01 : 0x00));
        bb.putInt(primary.length);
        bb.put(primary);
        bb.putInt(secondaries.size());
        for (var s : secondaries) {
            bb.putInt(s.length);
            bb.put(s);
        }
        return bb.array();
    }

    public static Lock decode(byte[] bytes) {
        var bb = ByteBuffer.wrap(bytes);
        byte version = bb.get();
        if (version != VERSION) {
            throw new IllegalArgumentException("unknown lock version 0x" + Integer.toHexString(version & 0xFF));
        }
        var type = Type.fromTag(bb.get());
        long startTs = bb.getLong();
        long ttlMs = bb.getLong();
        long txnSize = bb.getLong();
        long minCommitTs = bb.getLong();
        long forUpdateTs = bb.getLong();
        byte flags = bb.get();
        var primary = new byte[bb.getInt()]; bb.get(primary);
        int sCount = bb.getInt();
        var secondaries = new ArrayList<byte[]>(sCount);
        for (int i = 0; i < sCount; i++) {
            var k = new byte[bb.getInt()];
            bb.get(k);
            secondaries.add(k);
        }
        return Lock.builder()
                .type(type)
                .primary(primary)
                .startTs(startTs)
                .ttlMs(ttlMs)
                .txnSize(txnSize)
                .minCommitTs(minCommitTs)
                .forUpdateTs(forUpdateTs)
                .useAsyncCommit((flags & 0x01) != 0)
                .secondaries(secondaries)
                .build();
    }

    public static Builder builder() { return new Builder(); }

    public static final class Builder {
        private Type type = Type.PUT;
        private byte[] primary = new byte[0];
        private long startTs;
        private long ttlMs;
        private long txnSize;
        private long minCommitTs;
        private long forUpdateTs;
        private boolean useAsyncCommit;
        private List<byte[]> secondaries;

        public Builder type(Type t) { this.type = t; return this; }
        public Builder primary(byte[] p) { this.primary = p; return this; }
        public Builder startTs(long v) { this.startTs = v; return this; }
        public Builder ttlMs(long v) { this.ttlMs = v; return this; }
        public Builder txnSize(long v) { this.txnSize = v; return this; }
        public Builder minCommitTs(long v) { this.minCommitTs = v; return this; }
        public Builder forUpdateTs(long v) { this.forUpdateTs = v; return this; }
        public Builder useAsyncCommit(boolean v) { this.useAsyncCommit = v; return this; }
        public Builder secondaries(List<byte[]> v) { this.secondaries = v; return this; }

        public Lock build() { return new Lock(this); }
    }
}
