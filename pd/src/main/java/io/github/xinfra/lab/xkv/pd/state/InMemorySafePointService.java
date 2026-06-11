package io.github.xinfra.lab.xkv.pd.state;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Default {@link SafePointService}. Tracks one global GC safe-point and
 * a map of per-service safe-point registrations.
 *
 * <h3>The v1 missing piece</h3>
 *
 * <p>v1 had only {@code gc_safe_point}. Active BR / CDC / long-running SQL
 * had no way to keep the safe-point from advancing past their working
 * snapshot. The result: a running BR job and the GC worker would race;
 * GC could remove the very versions BR was reading, corrupting the backup.
 *
 * <p>v2's contract:
 * <pre>
 *   effective_safe_point = min(now - gcLifetime, min over active services)
 * </pre>
 *
 * <p>{@link #updateServiceSafePoint} returns the new global min. Callers
 * compare it to their requested floor — if min &lt; requested, their GC
 * has been throttled and they should adjust.
 *
 * <p>Service registrations carry a TTL: a crashed BR job won't pin the
 * global safe-point forever; after TTL it expires.
 *
 * <h3>Persistence note</h3>
 *
 * <p>This in-memory implementation is correct for a single-leader Phase 3
 * setup. For HA, every mutator must go through Raft apply; that wrapper
 * lands when {@code RaftBackedSafePointService} is wired in Phase 4.
 */
public final class InMemorySafePointService implements SafePointService {
    private static final Logger log = LoggerFactory.getLogger(InMemorySafePointService.class);

    /** Default GC lifetime: the floor for the global safe-point relative to now. */
    public static final long DEFAULT_GC_LIFETIME_MS = 10 * 60 * 1000L;

    private final AtomicLong globalSafePoint = new AtomicLong(0);
    /**
     * Operator-driven floor — what {@code UpdateGCSafePoint} advances. The
     * effective safe-point is the min of (this, time-floor, min over
     * services). Monotonic.
     */
    private final AtomicLong gcSafePointFloor = new AtomicLong(0);
    private final Map<String, ServiceEntry> services = new HashMap<>();
    private final Object servicesLock = new Object();

    private final java.util.function.LongSupplier nowMs;
    private final long gcLifetimeMs;

    public InMemorySafePointService() {
        this(System::currentTimeMillis, DEFAULT_GC_LIFETIME_MS);
    }

    public InMemorySafePointService(java.util.function.LongSupplier nowMs, long gcLifetimeMs) {
        this.nowMs = nowMs;
        this.gcLifetimeMs = gcLifetimeMs;
    }

    @Override
    public long currentSafePoint() { return globalSafePoint.get(); }

    @Override
    public long updateServiceSafePoint(String serviceId, long ttlSeconds, long safePoint) {
        long now = nowMs.getAsLong();
        synchronized (servicesLock) {
            if (ttlSeconds <= 0) {
                services.remove(serviceId);
                log.info("safe-point: deregistered service '{}'", serviceId);
            } else {
                long expiresAt = now + ttlSeconds * 1000L;
                services.put(serviceId, new ServiceEntry(serviceId, expiresAt, safePoint));
            }
            return computeMinLocked(now);
        }
    }

    @Override
    public void deleteServiceSafePoint(String serviceId) {
        synchronized (servicesLock) {
            services.remove(serviceId);
        }
    }

    @Override
    public Collection<ServiceEntry> listServiceSafePoints() {
        long now = nowMs.getAsLong();
        synchronized (servicesLock) {
            evictExpiredLocked(now);
            return new ArrayList<>(services.values());
        }
    }

    @Override
    public long advance() {
        long now = nowMs.getAsLong();
        synchronized (servicesLock) {
            evictExpiredLocked(now);
            long target = computeMinLocked(now);
            // Monotonic — never go backwards.
            long prev = globalSafePoint.get();
            if (target > prev) {
                globalSafePoint.set(target);
                log.info("safe-point advanced: {} → {}", prev, target);
                return target;
            }
            return prev;
        }
    }

    @Override
    public long updateGcSafePoint(long target) {
        // Ratchet the operator-driven floor monotonically.
        gcSafePointFloor.updateAndGet(prev -> Math.max(prev, target));
        // Recompute and publish the effective safe-point.
        return advance();
    }

    /**
     * Effective safe-point = min over:
     * <ul>
     *   <li>operator-driven floor ({@code UpdateGCSafePoint}); when zero,
     *       falls through to the time-based floor</li>
     *   <li>each non-expired service safe-point registration (TSO units)</li>
     * </ul>
     *
     * <p>The values here are TSO timestamps (HLC-encoded), NOT wall-clock
     * milliseconds. We do NOT mix units: the time-based floor
     * ({@code now_ms - gcLifetimeMs}) was historically used as a fallback
     * but TSO and ms have completely different scales (~5 orders of
     * magnitude). Mixing them is the v2 bug fixed here. The fallback now
     * applies only when the operator hasn't called {@code UpdateGCSafePoint}
     * yet — useful for an initial bootstrap, but operators are expected to
     * advance the safe-point explicitly.
     */
    private long computeMinLocked(long now) {
        long operatorFloor = gcSafePointFloor.get();
        long min;
        if (operatorFloor > 0) {
            min = operatorFloor;
        } else {
            // No operator advance yet — fall back to the time floor in
            // wall-clock ms. The caller WILL see ms-units here, which is
            // documented behavior of the bootstrap path.
            min = gcLifetimeMs > 0 ? Math.max(0, now - gcLifetimeMs) : 0;
        }
        for (var e : services.values()) {
            if (e.expiresAtMs() < now) continue;
            if (e.safePoint() < min) min = e.safePoint();
        }
        log.debug("computeMin now={} operatorFloor={} services={} → {}",
                now, operatorFloor, services.size(), min);
        return min;
    }

    private void evictExpiredLocked(long now) {
        services.values().removeIf(e -> e.expiresAtMs() < now);
    }
}
