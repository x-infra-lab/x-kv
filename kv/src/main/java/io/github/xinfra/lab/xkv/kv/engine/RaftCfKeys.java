package io.github.xinfra.lab.xkv.kv.engine;

import java.nio.ByteBuffer;

/**
 * Key layout inside the {@code raft} column family.
 *
 * <p>One CF holds Raft state for all regions, distinguished by a 9-byte
 * prefix: {@code [type=1B][regionId=8B big-endian]}. Big-endian so the
 * keys for one region are physically contiguous (better iteration locality
 * for log scans).
 *
 * <p>Within a region's prefix, the {@code type} byte further partitions the
 * data:
 * <ul>
 *   <li>{@code 0x01 LOG}      — Raft log entries; key suffix = bigEndian(index)</li>
 *   <li>{@code 0x02 META}     — fixed singleton: {@code HardState}</li>
 *   <li>{@code 0x03 APPLIED}  — fixed singleton: applied index</li>
 *   <li>{@code 0x04 DEDUP}    — per-clientId entries; suffix = bigEndian(clientId)</li>
 *   <li>{@code 0x05 SNAPMETA} — fixed singleton: snapshot meta</li>
 *   <li>{@code 0x06 CONFSTATE}— fixed singleton: ConfState</li>
 *   <li>{@code 0x07 REGION}   — fixed singleton: serialized {@code metapb.Region}</li>
 * </ul>
 *
 * <p>The encoding is deliberately compact and keys are fixed-length where
 * possible — index iteration in the log type uses a tight range scan with
 * upper-bound = {@code logKey(regionId, lastIndex+1)}.
 */
public final class RaftCfKeys {

    public static final byte TYPE_LOG = 0x01;
    public static final byte TYPE_META = 0x02;
    public static final byte TYPE_APPLIED = 0x03;
    public static final byte TYPE_DEDUP = 0x04;
    public static final byte TYPE_SNAPMETA = 0x05;
    public static final byte TYPE_CONFSTATE = 0x06;
    public static final byte TYPE_REGION = 0x07;
    public static final byte TYPE_MAX_TS = 0x08;
    public static final byte TYPE_MERGE_STATE = 0x09;

    private RaftCfKeys() {}

    public static byte[] regionPrefix(long regionId) {
        return ByteBuffer.allocate(9).put((byte) 0x00).putLong(regionId).array();
    }

    public static byte[] regionTypePrefix(long regionId, byte type) {
        var b = ByteBuffer.allocate(9);
        b.put(type).putLong(regionId);
        return b.array();
    }

    public static byte[] logKey(long regionId, long index) {
        var b = ByteBuffer.allocate(17);
        b.put(TYPE_LOG).putLong(regionId).putLong(index);
        return b.array();
    }

    public static long logIndexFromKey(byte[] key) {
        if (key.length != 17 || key[0] != TYPE_LOG) {
            throw new IllegalArgumentException("not a log key");
        }
        return ByteBuffer.wrap(key, 9, 8).getLong();
    }

    public static long logRegionIdFromKey(byte[] key) {
        if (key.length < 17 || key[0] != TYPE_LOG) {
            throw new IllegalArgumentException("not a log key");
        }
        return ByteBuffer.wrap(key, 1, 8).getLong();
    }

    public static byte[] metaKey(long regionId) {
        return regionTypePrefix(regionId, TYPE_META);
    }

    public static byte[] appliedKey(long regionId) {
        return regionTypePrefix(regionId, TYPE_APPLIED);
    }

    public static byte[] dedupKey(long regionId, long clientId) {
        var b = ByteBuffer.allocate(17);
        b.put(TYPE_DEDUP).putLong(regionId).putLong(clientId);
        return b.array();
    }

    public static long dedupClientIdFromKey(byte[] key) {
        if (key.length != 17 || key[0] != TYPE_DEDUP) {
            throw new IllegalArgumentException("not a dedup key");
        }
        return ByteBuffer.wrap(key, 9, 8).getLong();
    }

    public static byte[] snapshotMetaKey(long regionId) {
        return regionTypePrefix(regionId, TYPE_SNAPMETA);
    }

    public static byte[] maxTsKey(long regionId) {
        return regionTypePrefix(regionId, TYPE_MAX_TS);
    }

    public static byte[] mergeStateKey(long regionId) {
        return regionTypePrefix(regionId, TYPE_MERGE_STATE);
    }

    public static byte[] confStateKey(long regionId) {
        return regionTypePrefix(regionId, TYPE_CONFSTATE);
    }

    public static byte[] regionKey(long regionId) {
        return regionTypePrefix(regionId, TYPE_REGION);
    }

    public static byte[] allRegionKeysPrefix() {
        return new byte[] { TYPE_REGION };
    }

    public static byte[] allRegionKeysEnd() {
        return new byte[] { (byte) (TYPE_REGION + 1) };
    }

    public static long regionIdFromKey(byte[] key) {
        if (key.length != 9 || key[0] != TYPE_REGION) {
            throw new IllegalArgumentException("not a region key");
        }
        return ByteBuffer.wrap(key, 1, 8).getLong();
    }

    /** Encode a long as 8-byte little-endian for value-side serialization. */
    public static byte[] longToBytes(long v) {
        return ByteBuffer.allocate(8).putLong(v).array();
    }

    public static long bytesToLong(byte[] b) {
        if (b == null || b.length != 8) throw new IllegalArgumentException("expected 8 bytes");
        return ByteBuffer.wrap(b).getLong();
    }
}
