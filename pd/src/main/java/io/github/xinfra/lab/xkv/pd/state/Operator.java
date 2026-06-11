package io.github.xinfra.lab.xkv.pd.state;

import io.github.xinfra.lab.xkv.proto.Metapb;
import io.github.xinfra.lab.xkv.proto.Pdpb;

import java.util.List;

/**
 * One scheduling action against one region.
 *
 * <p>Operators are atomic from the scheduler's POV but may span multiple
 * Raft conf-changes (split-merge, joint-consensus). They are pushed down
 * through {@link Pdpb.RegionHeartbeatResponse} on the heartbeat stream.
 *
 * <p>Lifecycle: {@code RUNNING -> SUCCESS|TIMEOUT|CANCEL|REPLACE}. Once
 * non-RUNNING the operator is removed from the controller's index.
 */
public interface Operator {

    long id();

    long regionId();

    Kind kind();

    /** Coarse classification used by the scheduler for limit/quota accounting. */
    enum Kind {
        ADD_PEER,
        REMOVE_PEER,
        TRANSFER_LEADER,
        SPLIT,
        MERGE,
        SCATTER,
        BALANCE_REGION,
        BALANCE_LEADER,
        HOT_REGION
    }

    /** Steps that compose the operator. */
    List<Step> steps();

    /** Materialize the operator's next step into a heartbeat response. */
    Pdpb.RegionHeartbeatResponse next(Pdpb.RegionHeartbeatRequest hb);

    /**
     * Inspect a heartbeat report and decide whether the in-flight step has
     * completed, failed, or still pending.
     */
    Outcome observe(Pdpb.RegionHeartbeatRequest hb);

    enum Outcome { PENDING, FINISHED, FAILED }

    /** Why the controller created this operator — useful for ops & metrics. */
    String desc();

    long createdAtMs();

    /** Per-step abstraction so the scheduler can inspect / throttle. */
    interface Step {
        StepType type();

        Metapb.Peer peer();

        boolean satisfied(Pdpb.RegionHeartbeatRequest hb);
    }

    enum StepType {
        ADD_LEARNER,
        ADD_VOTER,
        REMOVE_PEER,
        TRANSFER_LEADER,
        PROMOTE_LEARNER,
        DEMOTE_VOTER,
        SPLIT_REGION,
        MERGE_REGION
    }
}
