package io.github.xinfra.lab.xkv.pd.state;

import java.util.Collection;

/**
 * Manages global GC safe-point and per-service safe-points.
 *
 * <p><strong>Global safe-point</strong>: the lower bound below which MVCC
 * versions may be GC'd. Computed every {@code advanceIntervalMs} as
 * {@code min(now - gcLifetime, min over active services)}.
 *
 * <p><strong>Service safe-points</strong>: BR / CDC / long-running SQL
 * register a safe-point lower bound here. While the registration is alive
 * the global safe-point cannot advance past it. Each registration carries
 * a TTL and auto-expires if not refreshed (so a crashed BR job does not
 * pin the safe-point forever).
 *
 * <p>The v1 design completely lacked this, with the result that running BR /
 * CDC / SQL would race against the GC worker and lose data.
 */
public interface SafePointService {

    /** Currently effective global safe-point. */
    long currentSafePoint();

    /**
     * Register / refresh a service safe-point.
     *
     * @param serviceId opaque per-service identifier
     * @param ttlSeconds how long the registration stays alive without refresh
     * @param safePoint the lower bound this service requires
     * @return the global effective min safe-point AFTER applying this update.
     *     Caller compares this against its requested {@code safePoint} —
     *     if min < requested, caller's GC has been throttled.
     */
    long updateServiceSafePoint(String serviceId, long ttlSeconds, long safePoint);

    /** Remove a service safe-point (BR job finished, CDC unsubscribe). */
    void deleteServiceSafePoint(String serviceId);

    /** All currently active service registrations. */
    Collection<ServiceEntry> listServiceSafePoints();

    record ServiceEntry(String serviceId, long expiresAtMs, long safePoint) {}

    /**
     * Advance the global safe-point. Called periodically by a background
     * worker; respects active service registrations as the lower-bound
     * floor. Returns the new global value (== old value if no advance).
     */
    long advance();

    /**
     * Operator-driven advance: ratchet the GC safe-point floor up to
     * {@code target}. Subsequent {@link #currentSafePoint()} = min(this
     * floor, min over active services, time-based floor). Monotonic — a
     * lower target is ignored. Returns the new effective global safe-point.
     */
    long updateGcSafePoint(long target);
}
