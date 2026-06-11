package io.github.xinfra.lab.xkv.pd.state;

import io.github.xinfra.lab.xkv.proto.Pdpb;

import java.util.Collection;
import java.util.Optional;

/**
 * Owner of all in-flight {@link Operator}s.
 *
 * <p>Schedulers add operators by calling {@link #addOperator}; the controller
 * is responsible for:
 * <ul>
 *   <li>Per-store / per-kind limit enforcement (store-limit token bucket).</li>
 *   <li>Operator timeout and replacement.</li>
 *   <li>Materializing the next step into a heartbeat response.</li>
 *   <li>Updating operator state in response to heartbeat observations.</li>
 * </ul>
 *
 * <p>Schedulers must NOT push raw {@link Pdpb.RegionHeartbeatResponse} into
 * the heartbeat stream directly — that bypasses limits and tracking and was
 * one of the v1 design rot points.
 */
public interface OperatorController {

    /**
     * Submit a new operator. Returns false if a higher-priority operator is
     * already in flight for the same region, if the destination store has
     * exhausted its limit, or if the operator's preconditions no longer hold.
     */
    boolean addOperator(Operator op);

    /** Cancel any operator on the region; returns true if one was cancelled. */
    boolean removeOperator(long regionId);

    /** Snapshot the current in-flight operator set (for /metrics, debug RPC). */
    Collection<Operator> getOperators();

    Optional<Operator> getOperator(long regionId);

    /**
     * Drive operator state forward against an inbound region heartbeat.
     * Called by the heartbeat handler before any scheduler dispatches new
     * operators on the same heartbeat — order matters: stale operators must
     * be drained first.
     *
     * @return the response to push back on the heartbeat stream, or empty
     *         if no operator is in flight for this region.
     */
    Optional<Pdpb.RegionHeartbeatResponse> dispatch(Pdpb.RegionHeartbeatRequest hb);

    void shutdown();
}
