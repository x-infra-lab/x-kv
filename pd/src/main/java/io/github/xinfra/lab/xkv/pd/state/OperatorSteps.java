package io.github.xinfra.lab.xkv.pd.state;

import io.github.xinfra.lab.xkv.proto.Metapb;
import io.github.xinfra.lab.xkv.proto.Pdpb;

/**
 * Concrete {@link Operator.Step} implementations for each step type.
 *
 * <p>Each step checks the inbound {@link Pdpb.RegionHeartbeatRequest} to
 * determine whether the corresponding scheduling action has been applied
 * by the KV store.
 */
public final class OperatorSteps {

    private OperatorSteps() {}

    public record AddPeerStep(Metapb.Peer peer) implements Operator.Step {
        @Override public Operator.StepType type() { return Operator.StepType.ADD_VOTER; }
        @Override public boolean satisfied(Pdpb.RegionHeartbeatRequest hb) {
            for (var p : hb.getRegion().getPeersList()) {
                if (p.getId() == peer.getId()) return true;
            }
            return false;
        }
    }

    public record AddLearnerStep(Metapb.Peer peer) implements Operator.Step {
        @Override public Operator.StepType type() { return Operator.StepType.ADD_LEARNER; }
        @Override public boolean satisfied(Pdpb.RegionHeartbeatRequest hb) {
            for (var p : hb.getRegion().getPeersList()) {
                if (p.getId() == peer.getId()) return true;
            }
            return false;
        }
    }

    public record RemovePeerStep(Metapb.Peer peer) implements Operator.Step {
        @Override public Operator.StepType type() { return Operator.StepType.REMOVE_PEER; }
        @Override public boolean satisfied(Pdpb.RegionHeartbeatRequest hb) {
            for (var p : hb.getRegion().getPeersList()) {
                if (p.getId() == peer.getId()) return false;
            }
            return true;
        }
    }

    public record TransferLeaderStep(Metapb.Peer peer) implements Operator.Step {
        @Override public Operator.StepType type() { return Operator.StepType.TRANSFER_LEADER; }
        @Override public boolean satisfied(Pdpb.RegionHeartbeatRequest hb) {
            return hb.hasLeader() && hb.getLeader().getId() == peer.getId();
        }
    }

    public record PromoteLearnerStep(Metapb.Peer peer) implements Operator.Step {
        @Override public Operator.StepType type() { return Operator.StepType.PROMOTE_LEARNER; }
        @Override public boolean satisfied(Pdpb.RegionHeartbeatRequest hb) {
            for (var p : hb.getRegion().getPeersList()) {
                if (p.getId() == peer.getId() && p.getRole() == Metapb.PeerRole.Voter) {
                    return true;
                }
            }
            return false;
        }
    }

    public record SplitRegionStep(Metapb.Peer peer, long expectedMinVersion)
            implements Operator.Step {
        public SplitRegionStep(long expectedMinVersion) {
            this(Metapb.Peer.getDefaultInstance(), expectedMinVersion);
        }
        @Override public Operator.StepType type() { return Operator.StepType.SPLIT_REGION; }
        @Override public boolean satisfied(Pdpb.RegionHeartbeatRequest hb) {
            return hb.getRegion().getRegionEpoch().getVersion() >= expectedMinVersion;
        }
    }

    public record MergeRegionStep(Metapb.Peer peer, long sourceRegionId)
            implements Operator.Step {
        public MergeRegionStep(long sourceRegionId) {
            this(Metapb.Peer.getDefaultInstance(), sourceRegionId);
        }
        @Override public Operator.StepType type() { return Operator.StepType.MERGE_REGION; }
        @Override public boolean satisfied(Pdpb.RegionHeartbeatRequest hb) {
            return false;
        }
    }
}
