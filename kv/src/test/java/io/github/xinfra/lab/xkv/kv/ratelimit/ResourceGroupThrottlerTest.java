package io.github.xinfra.lab.xkv.kv.ratelimit;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

final class ResourceGroupThrottlerTest {

    @Test
    void unknownGroupAllowsByDefault() {
        var throttler = new ResourceGroupThrottler();
        assertThat(throttler.tryConsume("nonexistent", 1)).isTrue();
        assertThat(throttler.tryConsume(null, 1)).isTrue();
        assertThat(throttler.tryConsume("", 1)).isTrue();
    }

    @Test
    void configuredGroupEnforcesLimit() {
        var throttler = new ResourceGroupThrottler();
        throttler.updateSettings("batch", 10, 5);
        // Burst capacity is 5 — first 5 should succeed.
        for (int i = 0; i < 5; i++) {
            assertThat(throttler.tryConsume("batch", 1)).isTrue();
        }
        // 6th should fail (no refill time).
        assertThat(throttler.tryConsume("batch", 1)).isFalse();
    }

    @Test
    void removeGroupAllowsTraffic() {
        var throttler = new ResourceGroupThrottler();
        throttler.updateSettings("temp", 10, 0);
        assertThat(throttler.tryConsume("temp", 1)).isFalse();
        throttler.removeGroup("temp");
        assertThat(throttler.tryConsume("temp", 1)).isTrue();
    }

    @Test
    void updateSettingsHotReload() {
        var throttler = new ResourceGroupThrottler();
        throttler.updateSettings("rg", 10, 2);
        assertThat(throttler.tryConsume("rg", 2)).isTrue();
        assertThat(throttler.tryConsume("rg", 1)).isFalse();

        throttler.updateSettings("rg", 10, 100);
        // After update, bucket should still have old tokens (near 0), but
        // the burst limit is raised so refill will eventually work.
        // At minimum, the settings were accepted.
        assertThat(throttler.groupCount()).isEqualTo(1);
    }

    @Test
    void defaultGroupZeroRateRemovesEntry() {
        var throttler = new ResourceGroupThrottler();
        throttler.updateSettings("default", 0, 0);
        assertThat(throttler.groupCount()).isZero();
        assertThat(throttler.tryConsume("default", 1)).isTrue();
    }

    @Test
    void groupCountTracksActiveGroups() {
        var throttler = new ResourceGroupThrottler();
        assertThat(throttler.groupCount()).isZero();
        throttler.updateSettings("a", 10, 10);
        throttler.updateSettings("b", 20, 20);
        assertThat(throttler.groupCount()).isEqualTo(2);
        throttler.removeGroup("a");
        assertThat(throttler.groupCount()).isEqualTo(1);
    }
}
