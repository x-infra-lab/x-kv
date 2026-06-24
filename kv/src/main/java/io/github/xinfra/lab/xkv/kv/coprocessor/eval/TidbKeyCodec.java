package io.github.xinfra.lab.xkv.kv.coprocessor.eval;

import io.github.xinfra.lab.xkv.proto.Tipb;

import java.nio.charset.StandardCharsets;
import java.util.List;

/**
 * TiDB-compatible key codec for table record and index entries.
 *
 * <p>Record key layout (19 bytes):
 * {@code 0x74 || bigEndian(tableId) || 0x5F 0x72 || bigEndian(handle ^ MIN_VALUE)}
 *
 * <p>Index key layout:
 * {@code 0x74 || bigEndian(tableId) || 0x5F 0x69 || bigEndian(indexId) || encodedColumns...}
 *
 * <p>For non-unique index: value = bigEndian(handle ^ MIN_VALUE)
 * <p>For unique index: value = empty; handle is the last 8 bytes of the key
 */
public final class TidbKeyCodec {

    private static final byte TABLE_PREFIX = 0x74;
    private static final byte RECORD_MARK_1 = 0x5F;
    private static final byte RECORD_MARK_2 = 0x72;
    private static final byte INDEX_MARK_1 = 0x5F;
    private static final byte INDEX_MARK_2 = 0x69;

    public static final int TABLE_PREFIX_LEN = 9;
    public static final int RECORD_KEY_LEN = 19;
    public static final int INDEX_PREFIX_LEN = 19;

    private TidbKeyCodec() {}

    public static byte[] encodeRecordKey(long tableId, long handle) {
        byte[] key = new byte[RECORD_KEY_LEN];
        key[0] = TABLE_PREFIX;
        encodeInt64(key, 1, tableId);
        key[9] = RECORD_MARK_1;
        key[10] = RECORD_MARK_2;
        encodeInt64(key, 11, handle);
        return key;
    }

    public static byte[] encodeIndexKeyPrefix(long tableId, long indexId) {
        byte[] key = new byte[INDEX_PREFIX_LEN];
        key[0] = TABLE_PREFIX;
        encodeInt64(key, 1, tableId);
        key[9] = INDEX_MARK_1;
        key[10] = INDEX_MARK_2;
        encodeInt64(key, 11, indexId);
        return key;
    }

    public static byte[] encodeIndexKey(long tableId, long indexId, CopDatum[] columns) {
        byte[] prefix = encodeIndexKeyPrefix(tableId, indexId);
        byte[] colBytes = encodeIndexColumns(columns);
        byte[] key = new byte[prefix.length + colBytes.length];
        System.arraycopy(prefix, 0, key, 0, prefix.length);
        System.arraycopy(colBytes, 0, key, prefix.length, colBytes.length);
        return key;
    }

    public static byte[] encodeIndexKeyWithHandle(long tableId, long indexId,
                                                    CopDatum[] columns, long handle) {
        byte[] prefix = encodeIndexKeyPrefix(tableId, indexId);
        byte[] colBytes = encodeIndexColumns(columns);
        byte[] key = new byte[prefix.length + colBytes.length + 8];
        System.arraycopy(prefix, 0, key, 0, prefix.length);
        System.arraycopy(colBytes, 0, key, prefix.length, colBytes.length);
        encodeInt64(key, prefix.length + colBytes.length, handle);
        return key;
    }

    public static long decodeTableId(byte[] key) {
        return decodeInt64(key, 1);
    }

    public static boolean isRecordKey(byte[] key) {
        return key.length >= RECORD_KEY_LEN
                && key[0] == TABLE_PREFIX
                && key[9] == RECORD_MARK_1
                && key[10] == RECORD_MARK_2;
    }

    public static boolean isIndexKey(byte[] key) {
        return key.length >= INDEX_PREFIX_LEN
                && key[0] == TABLE_PREFIX
                && key[9] == INDEX_MARK_1
                && key[10] == INDEX_MARK_2;
    }

    public static long decodeRecordHandle(byte[] key) {
        return decodeInt64(key, 11);
    }

    public static long decodeIndexId(byte[] key) {
        return decodeInt64(key, 11);
    }

    public static long decodeHandleFromValue(byte[] value) {
        return decodeInt64(value, 0);
    }

    /**
     * Decode index column values from an index key, starting after the
     * index prefix (offset 19).
     */
    public static CopDatum[] decodeIndexColumns(byte[] key, List<Tipb.ColumnInfo> indexColumns) {
        int offset = INDEX_PREFIX_LEN;
        CopDatum[] result = new CopDatum[indexColumns.size()];
        int[] bytesRead = new int[1];
        for (int i = 0; i < indexColumns.size(); i++) {
            if (offset >= key.length) {
                result[i] = CopDatum.nil();
                continue;
            }
            result[i] = decodeOneColumn(key, offset, indexColumns.get(i).getDataType(), bytesRead);
            offset += bytesRead[0];
        }
        return result;
    }

    // --- Encoding helpers ---

    private static byte[] encodeIndexColumns(CopDatum[] columns) {
        int totalLen = 0;
        byte[][] parts = new byte[columns.length][];
        for (int i = 0; i < columns.length; i++) {
            parts[i] = encodeOneDatum(columns[i]);
            totalLen += parts[i].length;
        }
        byte[] result = new byte[totalLen];
        int pos = 0;
        for (byte[] part : parts) {
            System.arraycopy(part, 0, result, pos, part.length);
            pos += part.length;
        }
        return result;
    }

    private static byte[] encodeOneDatum(CopDatum datum) {
        if (datum.isNull()) {
            return new byte[]{CopCodecUtil.NULL_FLAG};
        }
        if (datum instanceof CopDatum.IntVal iv) {
            byte[] buf = new byte[9];
            buf[0] = CopCodecUtil.INT_FLAG;
            encodeInt64(buf, 1, iv.value());
            return buf;
        }
        if (datum instanceof CopDatum.StringVal sv) {
            byte[] raw = sv.value().getBytes(StandardCharsets.UTF_8);
            byte[] lenBuf = encodeVarint(raw.length);
            byte[] buf = new byte[1 + lenBuf.length + raw.length];
            buf[0] = CopCodecUtil.COMPACT_BYTES_FLAG;
            System.arraycopy(lenBuf, 0, buf, 1, lenBuf.length);
            System.arraycopy(raw, 0, buf, 1 + lenBuf.length, raw.length);
            return buf;
        }
        if (datum instanceof CopDatum.BytesVal bv) {
            byte[] raw = bv.value();
            byte[] lenBuf = encodeVarint(raw.length);
            byte[] buf = new byte[1 + lenBuf.length + raw.length];
            buf[0] = CopCodecUtil.BYTES_DATUM_FLAG;
            System.arraycopy(lenBuf, 0, buf, 1, lenBuf.length);
            System.arraycopy(raw, 0, buf, 1 + lenBuf.length, raw.length);
            return buf;
        }
        if (datum instanceof CopDatum.DoubleVal dv) {
            byte[] buf = new byte[9];
            buf[0] = CopCodecUtil.FLOAT_FLAG;
            long bits = Double.doubleToLongBits(dv.value());
            for (int i = 0; i < 8; i++) {
                buf[1 + i] = (byte) (bits >>> (56 - 8 * i));
            }
            return buf;
        }
        return new byte[]{CopCodecUtil.NULL_FLAG};
    }

    private static CopDatum decodeOneColumn(byte[] data, int offset, int dataType,
                                             int[] bytesRead) {
        byte flag = data[offset];
        int pos = offset + 1;
        return switch (flag) {
            case CopCodecUtil.NULL_FLAG -> { bytesRead[0] = 1; yield CopDatum.nil(); }
            case CopCodecUtil.INT_FLAG -> {
                long v = decodeInt64(data, pos);
                bytesRead[0] = 9;
                yield CopDatum.of(v);
            }
            case CopCodecUtil.FLOAT_FLAG -> {
                long bits = 0;
                for (int i = 0; i < 8; i++) {
                    bits = (bits << 8) | (data[pos + i] & 0xFFL);
                }
                bytesRead[0] = 9;
                yield CopDatum.of(Double.longBitsToDouble(bits));
            }
            case CopCodecUtil.COMPACT_BYTES_FLAG -> {
                int[] innerRead = new int[1];
                long len = CopCodecUtil.decodeVarint(data, pos, innerRead);
                pos += innerRead[0];
                byte[] raw = new byte[(int) len];
                System.arraycopy(data, pos, raw, 0, (int) len);
                bytesRead[0] = 1 + innerRead[0] + (int) len;
                yield CopDatum.of(new String(raw, StandardCharsets.UTF_8));
            }
            case CopCodecUtil.BYTES_DATUM_FLAG -> {
                int[] innerRead = new int[1];
                long len = CopCodecUtil.decodeVarint(data, pos, innerRead);
                pos += innerRead[0];
                byte[] raw = new byte[(int) len];
                System.arraycopy(data, pos, raw, 0, (int) len);
                bytesRead[0] = 1 + innerRead[0] + (int) len;
                yield CopDatum.of(raw);
            }
            default -> { bytesRead[0] = 1; yield CopDatum.nil(); }
        };
    }

    public static void encodeInt64(byte[] buf, int offset, long v) {
        long u = v ^ Long.MIN_VALUE;
        for (int i = 0; i < 8; i++) {
            buf[offset + i] = (byte) (u >>> (56 - 8 * i));
        }
    }

    public static long decodeInt64(byte[] buf, int offset) {
        long u = 0;
        for (int i = 0; i < 8; i++) {
            u = (u << 8) | (buf[offset + i] & 0xFFL);
        }
        return u ^ Long.MIN_VALUE;
    }

    private static byte[] encodeVarint(long v) {
        long uv = (v << 1) ^ (v >> 63);
        if (uv < 128) return new byte[]{(byte) uv};
        byte[] buf = new byte[10];
        int i = 0;
        while (uv >= 0x80) {
            buf[i++] = (byte) (uv | 0x80);
            uv >>>= 7;
        }
        buf[i++] = (byte) uv;
        byte[] result = new byte[i];
        System.arraycopy(buf, 0, result, 0, i);
        return result;
    }
}
