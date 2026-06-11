package io.github.xinfra.lab.xkv.kv.mvcc;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

final class LockSerdesTest {

    @Test
    void roundTripFullLock() {
        var primary = "primary".getBytes();
        var lock = Lock.builder()
                .type(Lock.Type.PUT)
                .primary(primary)
                .startTs(100)
                .ttlMs(3_000)
                .txnSize(5)
                .minCommitTs(101)
                .forUpdateTs(0)
                .useAsyncCommit(true)
                .secondaries(List.of("k1".getBytes(), "k2".getBytes(), "k3".getBytes()))
                .build();

        var enc = lock.encode();
        var dec = Lock.decode(enc);

        assertThat(dec.type()).isEqualTo(Lock.Type.PUT);
        assertThat(dec.primary()).isEqualTo(primary);
        assertThat(dec.startTs()).isEqualTo(100);
        assertThat(dec.ttlMs()).isEqualTo(3_000);
        assertThat(dec.txnSize()).isEqualTo(5);
        assertThat(dec.minCommitTs()).isEqualTo(101);
        assertThat(dec.forUpdateTs()).isEqualTo(0);
        assertThat(dec.useAsyncCommit()).isTrue();
        assertThat(dec.secondaries()).hasSize(3);
        assertThat(dec.secondaries().get(1)).isEqualTo("k2".getBytes());
    }

    @Test
    void roundTripPessimistic() {
        var lock = Lock.builder()
                .type(Lock.Type.PESSIMISTIC)
                .primary("p".getBytes())
                .startTs(50)
                .forUpdateTs(75)
                .ttlMs(2_000)
                .build();
        var dec = Lock.decode(lock.encode());
        assertThat(dec.isPessimistic()).isTrue();
        assertThat(dec.forUpdateTs()).isEqualTo(75);
        assertThat(dec.useAsyncCommit()).isFalse();
        assertThat(dec.secondaries()).isEmpty();
    }
}
