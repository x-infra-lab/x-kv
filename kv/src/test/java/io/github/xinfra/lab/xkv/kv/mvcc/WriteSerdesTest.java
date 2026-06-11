package io.github.xinfra.lab.xkv.kv.mvcc;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

final class WriteSerdesTest {

    @Test
    void putWithoutShortValue() {
        var w = Write.put(100);
        var dec = Write.decode(w.encode());
        assertThat(dec.type()).isEqualTo(Write.Type.PUT);
        assertThat(dec.startTs()).isEqualTo(100);
        assertThat(dec.hasShortValue()).isFalse();
        assertThat(dec.hasOverlappedRollback()).isFalse();
    }

    @Test
    void putWithShortValue() {
        var sv = "small".getBytes();
        var w = Write.put(123, sv);
        var dec = Write.decode(w.encode());
        assertThat(dec.hasShortValue()).isTrue();
        assertThat(dec.shortValue()).isEqualTo(sv);
        assertThat(dec.startTs()).isEqualTo(123);
    }

    @Test
    void deleteAndRollback() {
        var d = Write.decode(Write.delete(7).encode());
        assertThat(d.type()).isEqualTo(Write.Type.DELETE);
        var rb = Write.decode(Write.rollback(7).encode());
        assertThat(rb.type()).isEqualTo(Write.Type.ROLLBACK);
    }

    @Test
    void overlappedRollbackFlag() {
        var w = Write.rollbackOverlapping(99);
        var dec = Write.decode(w.encode());
        assertThat(dec.hasOverlappedRollback()).isTrue();
    }
}
