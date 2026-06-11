package io.github.xinfra.lab.xkv.kv.mvcc;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Comparator;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Phase 2 verification: MVCC key encoding round-trips and orders newest-first
 * under the byte-wise unsigned comparison RocksDB uses.
 */
final class MvccKeyTest {

    /** RocksDB compares keys as unsigned bytes — match it. */
    private static final Comparator<byte[]> UNSIGNED = Arrays::compareUnsigned;

    @Test
    void encodeRoundTrip() {
        var u = "alice".getBytes();
        long ts = 12_345_678L;
        var enc = MvccKey.encode(u, ts);
        assertThat(MvccKey.userKey(enc)).isEqualTo(u);
        assertThat(MvccKey.ts(enc)).isEqualTo(ts);
    }

    @Test
    void encodingPlacesNewerVersionFirst() {
        var u = "k".getBytes();
        var k1 = MvccKey.encode(u, 100);
        var k2 = MvccKey.encode(u, 200);
        var k3 = MvccKey.encode(u, 300);

        var sorted = Arrays.asList(k2, k3, k1);
        sorted.sort(UNSIGNED);
        assertThat(MvccKey.ts(sorted.get(0))).as("newest first").isEqualTo(300);
        assertThat(MvccKey.ts(sorted.get(1))).isEqualTo(200);
        assertThat(MvccKey.ts(sorted.get(2))).isEqualTo(100);
    }

    @Test
    void differentUserKeysOrderedByUserKey() {
        var k1 = MvccKey.encode("a".getBytes(), 999);
        var k2 = MvccKey.encode("b".getBytes(), 1);
        assertThat(UNSIGNED.compare(k1, k2)).isNegative();
    }

    @Test
    void firstVersionAndAfterAllVersionsBracketEachUserKey() {
        var u = "user:1".getBytes();
        var first = MvccKey.firstVersionFor(u);
        var after = MvccKey.afterAllVersionsOf(u);
        for (long ts : List.of(1L, 100L, 1_000_000L)) {
            var v = MvccKey.encode(u, ts);
            assertThat(UNSIGNED.compare(first, v))
                    .as("firstVersionFor sorts <= every version of the same userKey")
                    .isNotPositive();
            assertThat(UNSIGNED.compare(v, after)).isNegative();
        }
    }

    @Test
    void userKeyEqualsHelper() {
        var u = "k".getBytes();
        var enc = MvccKey.encode(u, 42);
        assertThat(MvccKey.userKeyEquals(enc, u)).isTrue();
        assertThat(MvccKey.userKeyEquals(enc, "x".getBytes())).isFalse();
        assertThat(MvccKey.userKeyEquals(enc, "kk".getBytes())).isFalse();
    }
}
