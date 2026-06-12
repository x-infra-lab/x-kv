package io.github.xinfra.lab.xkv.pd.state;

import io.github.xinfra.lab.xkv.proto.Pdpb;

import java.util.List;
import java.util.Set;

/**
 * Minimal {@link Operator} implementation used by schedulers to produce
 * operators for {@link OperatorControllerImpl}.
 *
 * <p>Each SimpleOperator wraps a single pre-built
 * {@link Pdpb.RegionHeartbeatResponse}. The controller materializes it
 * into the {@link OperatorQueue} and tracks it for store-limit enforcement.
 */
public final class SimpleOperator implements Operator {

    private final long id;
    private final long regionId;
    private final Kind kind;
    private final String desc;
    private final Pdpb.RegionHeartbeatResponse response;
    private final Set<Long> targetStoreIds;
    private final long createdAtMs;

    public SimpleOperator(long id, long regionId, Kind kind, String desc,
                          Pdpb.RegionHeartbeatResponse response,
                          Set<Long> targetStoreIds) {
        this.id = id;
        this.regionId = regionId;
        this.kind = kind;
        this.desc = desc;
        this.response = response;
        this.targetStoreIds = Set.copyOf(targetStoreIds);
        this.createdAtMs = System.currentTimeMillis();
    }

    @Override public long id() { return id; }
    @Override public long regionId() { return regionId; }
    @Override public Kind kind() { return kind; }
    @Override public String desc() { return desc; }
    @Override public long createdAtMs() { return createdAtMs; }
    @Override public List<Step> steps() { return List.of(); }

    @Override
    public Pdpb.RegionHeartbeatResponse next(Pdpb.RegionHeartbeatRequest hb) {
        return response;
    }

    @Override
    public Outcome observe(Pdpb.RegionHeartbeatRequest hb) {
        return Outcome.FINISHED;
    }

    public Pdpb.RegionHeartbeatResponse response() { return response; }

    public Set<Long> targetStoreIds() { return targetStoreIds; }
}
