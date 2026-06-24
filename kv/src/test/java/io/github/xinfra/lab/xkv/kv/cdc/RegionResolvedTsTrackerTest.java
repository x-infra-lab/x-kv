package io.github.xinfra.lab.xkv.kv.cdc;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

final class RegionResolvedTsTrackerTest {

    @Test
    void noLocksReturnsFallback() {
        var tracker = new RegionResolvedTsTracker();
        assertThat(tracker.resolvedTs(1, 500)).isEqualTo(500);
    }

    @Test
    void singleLockReturnsStartTsMinusOne() {
        var tracker = new RegionResolvedTsTracker();
        tracker.trackLock(1, 100);
        assertThat(tracker.resolvedTs(1, 500)).isEqualTo(99);
    }

    @Test
    void multipleLocksReturnsMinStartTsMinusOne() {
        var tracker = new RegionResolvedTsTracker();
        tracker.trackLock(1, 200);
        tracker.trackLock(1, 100);
        tracker.trackLock(1, 300);
        assertThat(tracker.resolvedTs(1, 500)).isEqualTo(99);
    }

    @Test
    void untrackAdvancesResolvedTs() {
        var tracker = new RegionResolvedTsTracker();
        tracker.trackLock(1, 100);
        tracker.trackLock(1, 200);

        tracker.untrackLock(1, 100);
        assertThat(tracker.resolvedTs(1, 500)).isEqualTo(199);

        tracker.untrackLock(1, 200);
        assertThat(tracker.resolvedTs(1, 500)).isEqualTo(500);
    }

    @Test
    void regionsAreIndependent() {
        var tracker = new RegionResolvedTsTracker();
        tracker.trackLock(1, 100);
        tracker.trackLock(2, 200);

        assertThat(tracker.resolvedTs(1, 500)).isEqualTo(99);
        assertThat(tracker.resolvedTs(2, 500)).isEqualTo(199);
        assertThat(tracker.resolvedTs(3, 500)).isEqualTo(500);
    }

    @Test
    void untrackNonexistentIsNoOp() {
        var tracker = new RegionResolvedTsTracker();
        tracker.untrackLock(1, 100);
        assertThat(tracker.resolvedTs(1, 500)).isEqualTo(500);
    }

    @Test
    void duplicateTrackIsIdempotent() {
        var tracker = new RegionResolvedTsTracker();
        tracker.trackLock(1, 100);
        tracker.trackLock(1, 100);
        assertThat(tracker.resolvedTs(1, 500)).isEqualTo(99);

        tracker.untrackLock(1, 100);
        assertThat(tracker.resolvedTs(1, 500)).isEqualTo(500);
    }
}
