package io.github.xinfra.lab.xkv.kv.coprocessor.eval;

import java.time.LocalDateTime;

public final class CopCodecUtil {
    public static final byte NULL_FLAG = 0x00;
    public static final byte COMPACT_BYTES_FLAG = 0x02;
    public static final byte INT_FLAG = 0x03;
    public static final byte FLOAT_FLAG = 0x05;
    public static final byte DECIMAL_FLAG = 0x06;
    public static final byte DURATION_FLAG = 0x07;
    public static final byte BYTES_DATUM_FLAG = 0x0A;

    private CopCodecUtil() {}

    public static long decodeInt64(byte[] b, int offset) {
        long u = 0;
        for (int i = 0; i < 8; i++) {
            u = (u << 8) | (b[offset + i] & 0xFF);
        }
        return u ^ Long.MIN_VALUE;
    }

    public static long decodeUint64(byte[] b, int offset) {
        long v = 0;
        for (int i = 0; i < 8; i++) {
            v = (v << 8) | (b[offset + i] & 0xFF);
        }
        return v;
    }

    public static long decodeVarint(byte[] data, int offset, int[] bytesRead) {
        long uv = 0;
        int shift = 0;
        int idx = offset;
        while (idx < data.length) {
            byte b = data[idx++];
            uv |= (long) (b & 0x7F) << shift;
            if ((b & 0x80) == 0) break;
            shift += 7;
        }
        bytesRead[0] = idx - offset;
        return (uv >>> 1) ^ -(uv & 1);
    }

    public static long decodeUvarint(byte[] data, int offset, int[] bytesRead) {
        long v = 0;
        int shift = 0;
        int idx = offset;
        while (idx < data.length) {
            byte b = data[idx++];
            v |= (long) (b & 0x7F) << shift;
            if ((b & 0x80) == 0) break;
            shift += 7;
        }
        bytesRead[0] = idx - offset;
        return v;
    }

    public static LocalDateTime decodeDatetime(byte[] data, int offset) {
        long packed = decodeUint64(data, offset);
        int second = (int) (packed % 100); packed /= 100;
        int minute = (int) (packed % 100); packed /= 100;
        int hour   = (int) (packed % 100); packed /= 100;
        int day    = (int) (packed % 100); packed /= 100;
        int month  = (int) (packed % 100); packed /= 100;
        int year   = (int) packed;
        return LocalDateTime.of(year, month, day, hour, minute, second);
    }
}
