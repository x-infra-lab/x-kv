package io.github.xinfra.lab.xkv.kv.mvcc;

import java.nio.ByteBuffer;

/**
 * MVCC key encoding. Same as TiKV:
 *
 * <pre>
 *   encode(userKey, ts) = userKey ‖ bigEndian(~ts)
 * </pre>
 *
 * <p>Bit-inverting the timestamp before big-endian encoding makes the
 * physical key order put the <em>newest</em> version of a userKey first.
 * A forward seek on {@code encode(userKey, MAX_LONG)} lands on the latest
 * version. For a snapshot read at {@code readTs}, seek to
 * {@code encode(userKey, readTs)} and the iterator visits versions newer
 * than {@code readTs} first (whose committed-ts is &gt; readTs and so
 * invisible — skip them) and then the first visible version.
 *
 * <h3>Suffix length</h3>
 *
 * <p>Always 8 bytes. Used by the write CF's prefix bloom filter
 * configuration in {@code RocksStorageEngine}: capping the prefix
 * extractor at userKey length lets seek-by-userKey skip CF reads when no
 * version exists for that userKey.
 */
public final class MvccKey {

    public static final int TS_SUFFIX_LEN = 8;

    private MvccKey() {}

    public static byte[] encode(byte[] userKey, long ts) {
        var dst = new byte[userKey.length + TS_SUFFIX_LEN];
        System.arraycopy(userKey, 0, dst, 0, userKey.length);
        long inverted = ~ts;
        for (int i = 0; i < 8; i++) {
            dst[userKey.length + i] = (byte) (inverted >>> (56 - i * 8));
        }
        return dst;
    }

    /** Extract the userKey portion. */
    public static byte[] userKey(byte[] mvccKey) {
        if (mvccKey.length < TS_SUFFIX_LEN) {
            throw new IllegalArgumentException("mvcc key too short: " + mvccKey.length);
        }
        var u = new byte[mvccKey.length - TS_SUFFIX_LEN];
        System.arraycopy(mvccKey, 0, u, 0, u.length);
        return u;
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
     * <p>Implementation: append 9 bytes of {@code 0xFF}. RocksDB uses byte-
     * wise lexicographic compare; a 9-byte all-FF suffix is strictly greater
     * than any 8-byte suffix, so {@code encode(userKey, anyTs)} sorts before
     * the result. (Appending a single byte would not work: the lowest ts
     * value 0 produces an 8-byte all-FF suffix, which is greater than any
     * single-byte suffix.)
     */
    public static byte[] afterAllVersionsOf(byte[] userKey) {
        var dst = new byte[userKey.length + 9];
        System.arraycopy(userKey, 0, dst, 0, userKey.length);
        java.util.Arrays.fill(dst, userKey.length, dst.length, (byte) 0xFF);
        return dst;
    }

    /** True if {@code mvccKey} starts with {@code userKey}. */
    public static boolean userKeyEquals(byte[] mvccKey, byte[] userKey) {
        if (mvccKey.length != userKey.length + TS_SUFFIX_LEN) return false;
        for (int i = 0; i < userKey.length; i++) {
            if (mvccKey[i] != userKey[i]) return false;
        }
        return true;
    }

    /** Compose userKey from a {@link ByteBuffer} view of an mvcc key. */
    public static byte[] userKeyOf(ByteBuffer mvccKey) {
        var bytes = new byte[mvccKey.remaining()];
        mvccKey.get(bytes);
        return userKey(bytes);
    }
}
