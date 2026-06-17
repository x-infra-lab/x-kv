package io.github.xinfra.lab.xkv.pd.state;

import io.github.xinfra.lab.xkv.proto.Pdpb;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Concrete {@link OperatorController} with per-store concurrency limits
 * (token-bucket style) and operator timeout.
 *
 * <p>Sits between schedulers and {@link OperatorQueue}. When a scheduler
 * submits an {@link Operator} via {@link #addOperator}, the controller:
 * <ol>
 *   <li>Checks per-store in-flight limits for every target store.</li>
 *   <li>Rejects if the region already has an in-flight operator.</li>
 *   <li>Records the operator and forwards its materialized response to
 *       the {@link OperatorQueue} for heartbeat delivery.</li>
 * </ol>
 *
 * <p>On {@link #dispatch}, expired operators are evicted so they don't
 * block the store limit indefinitely.
 */
public final class OperatorControllerImpl implements OperatorController {
    private static final Logger log = LoggerFactory.getLogger(OperatorControllerImpl.class);

    private final OperatorQueue queue;
    private final int maxPerStore;
    private final long timeoutMs;

    private final ConcurrentHashMap<Long, OperatorRecord> inFlight = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<Long, AtomicInteger> storeInFlight = new ConcurrentHashMap<>();

    record OperatorRecord(Operator operator, Set<Long> targetStoreIds, long createdAtMs) {}

    public OperatorControllerImpl(OperatorQueue queue, int maxPerStore, long timeoutMs) {
        this.queue = queue;
        this.maxPerStore = maxPerStore;
        this.timeoutMs = timeoutMs;
    }

    @Override
    public synchronized boolean addOperator(Operator op) {
        long regionId = op.regionId();

        if (inFlight.containsKey(regionId)) {
            log.debug("operator rejected: region {} already has in-flight operator", regionId);
            return false;
        }

        Set<Long> targets = (op instanceof SimpleOperator so) ? so.targetStoreIds() : Set.of();

        for (long storeId : targets) {
            int current = storeInFlight
                    .computeIfAbsent(storeId, k -> new AtomicInteger(0))
                    .get();
            if (current >= maxPerStore) {
                log.debug("operator rejected: store {} at limit ({}/{})", storeId, current, maxPerStore);
                return false;
            }
        }

        var record = new OperatorRecord(op, targets, System.currentTimeMillis());
        if (inFlight.putIfAbsent(regionId, record) != null) {
            return false;
        }

        for (long storeId : targets) {
            storeInFlight.computeIfAbsent(storeId, k -> new AtomicInteger(0)).incrementAndGet();
        }

        if (op instanceof SimpleOperator so) {
            queue.offer(regionId, so.response());
        }

        log.info("operator added: region={} kind={} desc={} targets={}",
                regionId, op.kind(), op.desc(), targets);
        return true;
    }

    @Override
    public synchronized boolean removeOperator(long regionId) {
        var record = inFlight.remove(regionId);
        if (record == null) return false;
        decrementStoreCounters(record.targetStoreIds());
        log.debug("operator removed: region={}", regionId);
        return true;
    }

    @Override
    public Collection<Operator> getOperators() {
        var result = new ArrayList<Operator>(inFlight.size());
        for (var rec : inFlight.values()) {
            result.add(rec.operator());
        }
        return result;
    }

    @Override
    public Optional<Operator> getOperator(long regionId) {
        var rec = inFlight.get(regionId);
        return rec == null ? Optional.empty() : Optional.of(rec.operator());
    }

    @Override
    public Optional<Pdpb.RegionHeartbeatResponse> dispatch(Pdpb.RegionHeartbeatRequest hb) {
        long regionId = hb.hasRegion() ? hb.getRegion().getId() : 0L;
        if (regionId == 0) return Optional.empty();

        evictExpired();

        var record = inFlight.get(regionId);
        if (record == null) return queue.poll(regionId);

        var outcome = record.operator().observe(hb);
        if (outcome == Operator.Outcome.FINISHED || outcome == Operator.Outcome.FAILED) {
            removeOperator(regionId);
            queue.poll(regionId);
        }

        return Optional.of(record.operator().next(hb));
    }

    @Override
    public void shutdown() {
        inFlight.clear();
        storeInFlight.clear();
    }

    /** Visible for tests. */
    public int storeInFlightCount(long storeId) {
        var counter = storeInFlight.get(storeId);
        return counter == null ? 0 : counter.get();
    }

    /** Visible for tests. */
    public int totalInFlight() {
        return inFlight.size();
    }

    private void evictExpired() {
        long now = System.currentTimeMillis();
        var it = inFlight.entrySet().iterator();
        while (it.hasNext()) {
            var entry = it.next();
            if (now - entry.getValue().createdAtMs() > timeoutMs) {
                it.remove();
                decrementStoreCounters(entry.getValue().targetStoreIds());
                log.info("operator timed out: region={}", entry.getKey());
            }
        }
    }

    private void decrementStoreCounters(Set<Long> storeIds) {
        for (long storeId : storeIds) {
            var counter = storeInFlight.get(storeId);
            if (counter != null) {
                counter.updateAndGet(v -> Math.max(0, v - 1));
            }
        }
    }
}
