package io.github.xinfra.lab.xkv.pd.state;

import io.github.xinfra.lab.xkv.proto.Pdpb;

import java.util.List;
import java.util.Set;

public final class SimpleOperator implements Operator {

    private final long id;
    private final long regionId;
    private final Kind kind;
    private final String desc;
    private final Pdpb.RegionHeartbeatResponse response;
    private final Set<Long> targetStoreIds;
    private final long createdAtMs;
    private final List<Step> steps;
    private final int priority;
    private int currentStep = 0;

    public SimpleOperator(long id, long regionId, Kind kind, String desc,
                          Pdpb.RegionHeartbeatResponse response,
                          Set<Long> targetStoreIds,
                          List<Step> steps,
                          int priority) {
        this.id = id;
        this.regionId = regionId;
        this.kind = kind;
        this.desc = desc;
        this.response = response;
        this.targetStoreIds = Set.copyOf(targetStoreIds);
        this.createdAtMs = System.currentTimeMillis();
        this.steps = List.copyOf(steps);
        this.priority = priority;
    }

    @Override public long id() { return id; }
    @Override public long regionId() { return regionId; }
    @Override public Kind kind() { return kind; }
    @Override public String desc() { return desc; }
    @Override public long createdAtMs() { return createdAtMs; }
    @Override public List<Step> steps() { return steps; }
    @Override public int priority() { return priority; }

    @Override
    public Pdpb.RegionHeartbeatResponse next(Pdpb.RegionHeartbeatRequest hb) {
        return response;
    }

    @Override
    public Outcome observe(Pdpb.RegionHeartbeatRequest hb) {
        if (steps.isEmpty()) return Outcome.FINISHED;
        while (currentStep < steps.size()) {
            if (steps.get(currentStep).satisfied(hb)) {
                currentStep++;
            } else {
                return Outcome.PENDING;
            }
        }
        return Outcome.FINISHED;
    }

    public Pdpb.RegionHeartbeatResponse response() { return response; }

    public Set<Long> targetStoreIds() { return targetStoreIds; }
}
