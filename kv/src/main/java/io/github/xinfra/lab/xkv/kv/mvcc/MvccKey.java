package io.github.xinfra.lab.xkv.kv.mvcc;

import java.nio.ByteBuffer;
import java.util.Arrays;

/**
 * MVCC key encoding with TiKV-compatible length-prefixed user keys.
 *
 * <pre>
 *   encode(userKey, ts) = KeyCodec.encodeBytes(userKey) ‖ bigEndian(~ts)
 * </pre>
 *
 * <p>The userKey portion uses {@link KeyCodec#encodeBytes} so that no
 * encoded userKey is a byte-prefix of another. This makes
 * {@code deleteRange(encode(start), encode(end))} correct for all CFs,
 * solving the prefix-ambiguity problem that bare concatenation had.
 *
 * <p>Bit-inverting the timestamp before big-endian encoding makes the
 * physical key order put the <em>newest</em> version of a userKey first.
 *
 * <h3>LOCK CF keys</h3>
 *
 * <p>The LOCK CF stores keys as {@code KeyCodec.encodeBytes(userKey)}
 * (no ts suffix). Use {@link #lockKey} and {@link #userKeyFromLockKey}.
 */
public final class MvccKey {

    public static final int TS_SUFFIX_LEN = 8;

    private MvccKey() {}

    public static byte[] encode(byte[] userKey, long ts) {
        byte[] encodedUser = KeyCodec.encodeBytes(userKey);
        var dst = new byte[encodedUser.length + TS_SUFFIX_LEN];
        System.arraycopy(encodedUser, 0, dst, 0, encodedUser.length);
        long inverted = ~ts;
        for (int i = 0; i < 8; i++) {
            dst[encodedUser.length + i] = (byte) (inverted >>> (56 - i * 8));
        }
        return dst;
    }

    /** Extract the userKey portion (decoded from the length-prefixed encoding). */
    public static byte[] userKey(byte[] mvccKey) {
        if (mvccKey.length < TS_SUFFIX_LEN) {
            throw new IllegalArgumentException("mvcc key too short: " + mvccKey.length);
        }
        var encoded = new byte[mvccKey.length - TS_SUFFIX_LEN];
        System.arraycopy(mvccKey, 0, encoded, 0, encoded.length);
        return KeyCodec.decodeBytes(encoded);
    }

    /** Extract the original ts (uninverts the suffix). */
    public static long ts(byte[] mvccKey) {
        if (mvccKey.length < TS_SUFFIX_LEN) {
            throw new IllegalArgumentException("mvcc key too short: " + mvccKey.length);
        }
        long inverted = 0;
        for (int i = 0; i < 8; i++) {
            inverted = (inverted << 8) | (mvccKey[mvccKey.length - 8 + i] & 0xFFL);
        }
        return ~inverted;
    }

    /** A seek key whose userKey is {@code u} and ts = {@code Long.MAX_VALUE}. */
    public static byte[] firstVersionFor(byte[] userKey) {
        return encode(userKey, Long.MAX_VALUE);
    }

    /**
     * A seek key strictly greater than every version of {@code userKey} but
     * less than every version of any other userKey that sorts after.
     *
     * <p>With length-prefixed encoding, the encoded userKey portion is
     * prefix-free. Appending 9 bytes of {@code 0xFF} after the encoded
     * userKey is strictly greater than any 8-byte ts suffix.
     */
    public static byte[] afterAllVersionsOf(byte[] userKey) {
        byte[] encodedUser = KeyCodec.encodeBytes(userKey);
        var dst = new byte[encodedUser.length + TS_SUFFIX_LEN + 1];
        System.arraycopy(encodedUser, 0, dst, 0, encodedUser.length);
        Arrays.fill(dst, encodedUser.length, dst.length, (byte) 0xFF);
        return dst;
    }

    /**
     * True if {@code mvccKey}'s userKey portion equals {@code userKey}.
     * Encodes the userKey and compares the prefix bytes.
     */
    public static boolean userKeyEquals(byte[] mvccKey, byte[] userKey) {
        byte[] encodedUser = KeyCodec.encodeBytes(userKey);
        if (mvccKey.length != encodedUser.length + TS_SUFFIX_LEN) return false;
        for (int i = 0; i < encodedUser.length; i++) {
            if (mvccKey[i] != encodedUser[i]) return false;
        }
        return true;
    }

    /** Compose userKey from a {@link ByteBuffer} view of an mvcc key. */
    public static byte[] userKeyOf(ByteBuffer mvccKey) {
        var bytes = new byte[mvccKey.remaining()];
        mvccKey.get(bytes);
        return userKey(bytes);
    }

    /** Encode a bare userKey for LOCK CF storage. */
    public static byte[] lockKey(byte[] userKey) {
        return KeyCodec.encodeBytes(userKey);
    }

    /** Decode a LOCK CF key back to the bare userKey. */
    public static byte[] userKeyFromLockKey(byte[] lockKey) {
        return KeyCodec.decodeBytes(lockKey);
    }
}
