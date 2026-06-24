package io.github.xinfra.lab.xkv.pd.state.placement;

import io.github.xinfra.lab.xkv.proto.Metapb;
import io.github.xinfra.lab.xkv.proto.Pdpb;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public final class PlacementRule implements Comparable<PlacementRule> {

    private final String groupId;
    private final String id;
    private final int index;
    private final boolean override;
    private final byte[] startKey;
    private final byte[] endKey;
    private final String role;
    private final int count;
    private final List<LabelConstraint> labelConstraints;
    private final List<String> locationLabels;

    public PlacementRule(String groupId, String id, int index, boolean override,
                         byte[] startKey, byte[] endKey, String role, int count,
                         List<LabelConstraint> labelConstraints,
                         List<String> locationLabels) {
        this.groupId = groupId;
        this.id = id;
        this.index = index;
        this.override = override;
        this.startKey = startKey != null ? startKey : new byte[0];
        this.endKey = endKey != null ? endKey : new byte[0];
        this.role = role != null ? role.toLowerCase() : "voter";
        this.count = count;
        this.labelConstraints = labelConstraints != null ? List.copyOf(labelConstraints) : List.of();
        this.locationLabels = locationLabels != null ? List.copyOf(locationLabels) : List.of();
    }

    public static PlacementRule fromProto(Pdpb.PlacementRule proto) {
        List<LabelConstraint> constraints = new ArrayList<>(proto.getLabelConstraintsCount());
        for (var c : proto.getLabelConstraintsList()) {
            constraints.add(LabelConstraint.fromProto(c));
        }
        return new PlacementRule(
                proto.getGroupId(),
                proto.getId(),
                proto.getIndex(),
                proto.getOverride(),
                proto.getStartKey().toByteArray(),
                proto.getEndKey().toByteArray(),
                proto.getRole(),
                proto.getCount(),
                constraints,
                proto.getLocationLabelsList()
        );
    }

    public Pdpb.PlacementRule toProto() {
        var builder = Pdpb.PlacementRule.newBuilder()
                .setGroupId(groupId)
                .setId(id)
                .setIndex(index)
                .setOverride(override)
                .setRole(role)
                .setCount(count);
        if (startKey.length > 0) builder.setStartKey(com.google.protobuf.ByteString.copyFrom(startKey));
        if (endKey.length > 0) builder.setEndKey(com.google.protobuf.ByteString.copyFrom(endKey));
        for (var c : labelConstraints) builder.addLabelConstraints(c.toProto());
        builder.addAllLocationLabels(locationLabels);
        return builder.build();
    }

    public boolean matchesRegion(Metapb.Region region) {
        byte[] regionStart = region.getStartKey().toByteArray();
        byte[] regionEnd = region.getEndKey().toByteArray();

        if (startKey.length > 0 && regionEnd.length > 0
                && Arrays.compareUnsigned(regionEnd, startKey) <= 0) {
            return false;
        }
        if (endKey.length > 0 && regionStart.length > 0
                && Arrays.compareUnsigned(regionStart, endKey) >= 0) {
            return false;
        }
        if (startKey.length == 0 && endKey.length == 0) return true;

        return true;
    }

    public boolean storeMatchesConstraints(Metapb.Store store) {
        for (var c : labelConstraints) {
            if (!c.matches(store)) return false;
        }
        return true;
    }

    public boolean isVoter() {
        return "voter".equals(role);
    }

    public boolean isLearner() {
        return "learner".equals(role);
    }

    public boolean matchesPeerRole(Metapb.Peer peer) {
        if (isVoter()) {
            return peer.getRole() == Metapb.PeerRole.Voter
                    || peer.getRole() == Metapb.PeerRole.IncomingVoter;
        }
        if (isLearner()) {
            return peer.getRole() == Metapb.PeerRole.Learner;
        }
        return true;
    }

    public String key() {
        return groupId + "/" + id;
    }

    @Override
    public int compareTo(PlacementRule other) {
        int cmp = this.groupId.compareTo(other.groupId);
        if (cmp != 0) return cmp;
        cmp = Integer.compare(this.index, other.index);
        if (cmp != 0) return cmp;
        return this.id.compareTo(other.id);
    }

    public String groupId() { return groupId; }
    public String id() { return id; }
    public int index() { return index; }
    public boolean override() { return override; }
    public byte[] startKey() { return startKey; }
    public byte[] endKey() { return endKey; }
    public String role() { return role; }
    public int count() { return count; }
    public List<LabelConstraint> labelConstraints() { return labelConstraints; }
    public List<String> locationLabels() { return locationLabels; }
}
