package io.github.xinfra.lab.xkv.kv.mvcc;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.Arrays;
import java.util.Comparator;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

final class KeyCodecTest {

    private static final Comparator<byte[]> UNSIGNED = Arrays::compareUnsigned;

    static Stream<byte[]> roundTripInputs() {
        return Stream.of(
                new byte[0],
                new byte[]{0x01},
                new byte[]{0x00},
                new byte[]{(byte) 0xFF},
                "hello".getBytes(),
                new byte[8],
                new byte[]{1, 2, 3, 4, 5, 6, 7, 8},
                new byte[]{1, 2, 3, 4, 5, 6, 7, 8, 9},
                new byte[16],
                "a]longer*key!with.special/chars".getBytes()
        );
    }

    @ParameterizedTest
    @MethodSource("roundTripInputs")
    void encodeDecodeRoundTrip(byte[] input) {
        byte[] encoded = KeyCodec.encodeBytes(input);
        byte[] decoded = KeyCodec.decodeBytes(encoded);
        assertThat(decoded).isEqualTo(input);
    }

    @Test
    void encodedLengthMatchesActual() {
        for (int len = 0; len <= 24; len++) {
            byte[] data = new byte[len];
            int expected = KeyCodec.encodedLength(len);
            assertThat(KeyCodec.encodeBytes(data)).hasSize(expected);
        }
    }

    @Test
    void encodedLengthIsMultipleOfGroupPlusOne() {
        for (int len = 0; len <= 24; len++) {
            int encoded = KeyCodec.encodedLength(len);
            assertThat(encoded % (KeyCodec.ENC_GROUP_SIZE + 1)).isZero();
        }
    }

    @Test
    void lexicographicOrderPreserved() {
        byte[][] inputs = {
                new byte[0],
                new byte[]{0x00},
                new byte[]{0x00, 0x01},
                new byte[]{0x01},
                new byte[]{0x01, 0x00},
                new byte[]{0x01, 0x01},
                new byte[]{0x02},
                "abc".getBytes(),
                "abd".getBytes(),
                "b".getBytes(),
                new byte[]{(byte) 0xFE},
                new byte[]{(byte) 0xFF},
                new byte[]{(byte) 0xFF, (byte) 0xFF},
        };

        for (int i = 0; i < inputs.length - 1; i++) {
            byte[] encA = KeyCodec.encodeBytes(inputs[i]);
            byte[] encB = KeyCodec.encodeBytes(inputs[i + 1]);
            assertThat(UNSIGNED.compare(encA, encB))
                    .as("encode(%s) < encode(%s)",
                            Arrays.toString(inputs[i]),
                            Arrays.toString(inputs[i + 1]))
                    .isNegative();
        }
    }

    @Test
    void prefixFreeProperty() {
        byte[][] pairs = {
                "a".getBytes(), "ab".getBytes(),
                new byte[]{0x01}, new byte[]{0x01, 0x00},
                new byte[0], new byte[]{0x00},
                "key".getBytes(), "key\0".getBytes(),
                new byte[8], new byte[]{0, 0, 0, 0, 0, 0, 0, 0, 0},
        };

        for (int i = 0; i < pairs.length; i += 2) {
            byte[] encA = KeyCodec.encodeBytes(pairs[i]);
            byte[] encB = KeyCodec.encodeBytes(pairs[i + 1]);
            boolean aIsPrefixOfB = encB.length > encA.length
                    && Arrays.equals(encB, 0, encA.length, encA, 0, encA.length);
            assertThat(aIsPrefixOfB)
                    .as("encode(%s) must NOT be a byte-prefix of encode(%s)",
                            Arrays.toString(pairs[i]),
                            Arrays.toString(pairs[i + 1]))
                    .isFalse();
        }
    }

    @Test
    void decodeRejectsEmptyInput() {
        assertThatThrownBy(() -> KeyCodec.decodeBytes(new byte[0]))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void decodeRejectsBadLength() {
        assertThatThrownBy(() -> KeyCodec.decodeBytes(new byte[5]))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void eightByteInputProducesTwoGroups() {
        byte[] data = {1, 2, 3, 4, 5, 6, 7, 8};
        byte[] enc = KeyCodec.encodeBytes(data);
        assertThat(enc).hasSize(18);
        assertThat(enc[8]).isEqualTo((byte) 0xFF);
        assertThat(enc[17]).isEqualTo((byte) 0xF7);
    }
}
