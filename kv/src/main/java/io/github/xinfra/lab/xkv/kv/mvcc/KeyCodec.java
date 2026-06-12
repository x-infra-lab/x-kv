package io.github.xinfra.lab.xkv.kv.mvcc;

/**
 * TiKV-compatible memcomparable key encoding.
 *
 * <p>Encodes arbitrary byte arrays into a form that preserves lexicographic
 * ordering and guarantees that no encoded key is a byte-prefix of another.
 * This makes RocksDB {@code deleteRange(encode(start), encode(end))} correct
 * even when the original keys have prefix relationships.
 *
 * <p>Algorithm (matches TiKV {@code codec::bytes::encode_bytes}):
 * <ol>
 *   <li>Split input into 8-byte groups.</li>
 *   <li>For each group: write 8 data bytes (zero-padded if partial),
 *       then a marker byte = {@code 0xFF - padding_count}.</li>
 *   <li>Always emit a final terminator group when {@code index == len}
 *       (all-zero data, marker {@code 0xF7}).</li>
 * </ol>
 *
 * <p>Output length: {@code (floor(len/8) + 1) * 9} bytes.
 */
public final class KeyCodec {

    static final int ENC_GROUP_SIZE = 8;
    static final byte ENC_MARKER = (byte) 0xFF;
    static final byte ENC_PAD = 0x00;

    private KeyCodec() {}

    public static byte[] encodeBytes(byte[] data) {
        int len = data.length;
        int groups = len / ENC_GROUP_SIZE + 1;
        byte[] dst = new byte[groups * (ENC_GROUP_SIZE + 1)];
        int index = 0;
        int dstPos = 0;

        while (index <= len) {
            int remain = len - index;
            int padCount;
            if (remain >= ENC_GROUP_SIZE) {
                System.arraycopy(data, index, dst, dstPos, ENC_GROUP_SIZE);
                padCount = 0;
            } else {
                if (remain > 0) {
                    System.arraycopy(data, index, dst, dstPos, remain);
                }
                for (int i = remain; i < ENC_GROUP_SIZE; i++) {
                    dst[dstPos + i] = ENC_PAD;
                }
                padCount = ENC_GROUP_SIZE - remain;
            }
            dst[dstPos + ENC_GROUP_SIZE] = (byte) (ENC_MARKER - padCount);
            dstPos += ENC_GROUP_SIZE + 1;
            index += ENC_GROUP_SIZE;
        }

        return dst;
    }

    public static byte[] decodeBytes(byte[] encoded) {
        if (encoded.length == 0) {
            throw new IllegalArgumentException("encoded key is empty");
        }
        if (encoded.length % (ENC_GROUP_SIZE + 1) != 0) {
            throw new IllegalArgumentException(
                    "encoded key length " + encoded.length + " not a multiple of " + (ENC_GROUP_SIZE + 1));
        }

        int groups = encoded.length / (ENC_GROUP_SIZE + 1);
        byte[] buf = new byte[groups * ENC_GROUP_SIZE];
        int dataLen = 0;

        for (int g = 0; g < groups; g++) {
            int base = g * (ENC_GROUP_SIZE + 1);
            int marker = encoded[base + ENC_GROUP_SIZE] & 0xFF;
            int padCount = (ENC_MARKER & 0xFF) - marker;
            if (padCount < 0 || padCount > ENC_GROUP_SIZE) {
                throw new IllegalArgumentException(
                        "invalid marker 0x" + Integer.toHexString(marker) + " at group " + g);
            }
            int realBytes = ENC_GROUP_SIZE - padCount;
            System.arraycopy(encoded, base, buf, dataLen, realBytes);
            dataLen += realBytes;

            if (padCount > 0) {
                break;
            }
        }

        byte[] result = new byte[dataLen];
        System.arraycopy(buf, 0, result, 0, dataLen);
        return result;
    }

    public static int encodedLength(int rawLength) {
        return (rawLength / ENC_GROUP_SIZE + 1) * (ENC_GROUP_SIZE + 1);
    }
}
