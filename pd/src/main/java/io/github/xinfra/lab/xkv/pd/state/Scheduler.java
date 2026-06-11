package io.github.xinfra.lab.xkv.pd.state;

import io.github.xinfra.lab.xkv.proto.Pdpb;

import java.util.List;

/**
 * One scheduling policy. Plug-in style: each strategy lives behind this
 * interface and is registered with {@link OperatorController}.
 *
 * <p>Built-in schedulers (Phase 4):
 * <ul>
 *   <li>{@code BalanceRegionScheduler} — moves regions off hot/full stores.</li>
 *   <li>{@code BalanceLeaderScheduler} — even leader distribution.</li>
 *   <li>{@code HotRegionScheduler} — moves QPS-hot regions.</li>
 *   <li>{@code EvictLeaderScheduler} — drains leaders from a store (decom).</li>
 *   <li>{@code ReplicaCheckerScheduler} — refills offline / down peers.</li>
 *   <li>{@code MergeChecker} — merges adjacent small regions.</li>
 *   <li>{@code RuleChecker} — enforces placement rules / labels.</li>
 * </ul>
 *
 * <p>Each scheduler is invoked at most once per heartbeat tick. It MUST be
 * stateless w.r.t. cluster state — it reads from {@link ClusterView} and
 * emits operators; the controller serializes execution.
 */
public interface Scheduler {

    String name();

    /** Per-tick: produce zero or more operators. */
    List<Operator> schedule(ClusterView view);

    /** True if the scheduler should be invoked for this heartbeat. */
    default boolean isScheduleAllowed(Pdpb.RegionHeartbeatRequest hb) { return true; }

    default boolean isPaused() { return false; }
}
