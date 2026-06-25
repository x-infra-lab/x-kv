package io.github.xinfra.lab.xkv.pd.state;

import io.github.xinfra.lab.xkv.proto.Pdpb;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public final class OperatorControllerImpl implements OperatorController {
    private static final Logger log = LoggerFactory.getLogger(OperatorControllerImpl.class);

    private static final int MAX_HISTORY = 100;

    private final int maxPerStore;
    private final long timeoutMs;

    private final ConcurrentHashMap<Long, OperatorRecord> inFlight = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<Long, AtomicInteger> storeInFlight = new ConcurrentHashMap<>();
    private final ConcurrentLinkedDeque<Operator> historyDeque = new ConcurrentLinkedDeque<>();

    private final AtomicLong opsCreated = new AtomicLong();
    private final AtomicLong opsSuccess = new AtomicLong();
    private final AtomicLong opsFailed = new AtomicLong();
    private final AtomicLong opsTimeout = new AtomicLong();
    private final AtomicLong opsReplaced = new AtomicLong();

    record OperatorRecord(Operator operator, Set<Long> targetStoreIds, long createdAtMs) {}

    public OperatorControllerImpl(int maxPerStore, long timeoutMs) {
        this.maxPerStore = maxPerStore;
        this.timeoutMs = timeoutMs;
    }

    @Override
    public synchronized boolean addOperator(Operator op) {
        long regionId = op.regionId();

        var existing = inFlight.get(regionId);
        if (existing != null) {
            if (op.priority() > existing.operator().priority()) {
                doRemoveOperator(regionId, existing);
                opsReplaced.incrementAndGet();
                log.info("operator replaced: region={} old={} new={} (priority {} > {})",
                        regionId, existing.operator().desc(), op.desc(),
                        op.priority(), existing.operator().priority());
            } else {
                log.debug("operator rejected: region {} already has in-flight operator (priority {} <= {})",
                        regionId, op.priority(), existing.operator().priority());
                return false;
            }
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

        opsCreated.incrementAndGet();
        log.info("operator added: region={} kind={} desc={} priority={} targets={}",
                regionId, op.kind(), op.desc(), op.priority(), targets);
        return true;
    }

    @Override
    public synchronized boolean removeOperator(long regionId) {
        var record = inFlight.get(regionId);
        if (record == null) return false;
        doRemoveOperator(regionId, record);
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
    public synchronized Optional<Pdpb.RegionHeartbeatResponse> dispatch(Pdpb.RegionHeartbeatRequest hb) {
        long regionId = hb.hasRegion() ? hb.getRegion().getId() : 0L;
        if (regionId == 0) return Optional.empty();

        evictExpired();

        var record = inFlight.get(regionId);
        if (record == null) return Optional.empty();

        var outcome = record.operator().observe(hb);
        switch (outcome) {
            case FINISHED -> {
                doRemoveOperator(regionId, record);
                addHistory(record.operator());
                opsSuccess.incrementAndGet();
                log.info("operator finished: region={} desc={}", regionId, record.operator().desc());
                return Optional.empty();
            }
            case FAILED -> {
                doRemoveOperator(regionId, record);
                addHistory(record.operator());
                opsFailed.incrementAndGet();
                log.warn("operator failed: region={} desc={}", regionId, record.operator().desc());
                return Optional.empty();
            }
            case PENDING -> {
                return Optional.of(record.operator().next(hb));
            }
        }
        return Optional.empty();
    }

    @Override
    public List<Operator> history() {
        return List.copyOf(historyDeque);
    }

    @Override
    public void shutdown() {
        inFlight.clear();
        storeInFlight.clear();
    }

    public int storeInFlightCount(long storeId) {
        var counter = storeInFlight.get(storeId);
        return counter == null ? 0 : counter.get();
    }

    public int totalInFlight() {
        return inFlight.size();
    }

    public long opsCreated()  { return opsCreated.get(); }
    public long opsSuccess()  { return opsSuccess.get(); }
    public long opsFailed()   { return opsFailed.get(); }
    public long opsTimeout()  { return opsTimeout.get(); }
    public long opsReplaced() { return opsReplaced.get(); }

    private void doRemoveOperator(long regionId, OperatorRecord record) {
        inFlight.remove(regionId);
        decrementStoreCounters(record.targetStoreIds());
    }

    private void addHistory(Operator op) {
        historyDeque.addFirst(op);
        while (historyDeque.size() > MAX_HISTORY) historyDeque.removeLast();
    }

    private void evictExpired() {
        long now = System.currentTimeMillis();
        var it = inFlight.entrySet().iterator();
        while (it.hasNext()) {
            var entry = it.next();
            if (now - entry.getValue().createdAtMs() > timeoutMs) {
                it.remove();
                decrementStoreCounters(entry.getValue().targetStoreIds());
                addHistory(entry.getValue().operator());
                opsTimeout.incrementAndGet();
                log.info("operator timed out: region={} desc={}",
                        entry.getKey(), entry.getValue().operator().desc());
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
