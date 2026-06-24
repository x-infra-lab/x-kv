package io.github.xinfra.lab.xkv.pd.state.placement;

import io.github.xinfra.lab.xkv.proto.Metapb;
import io.github.xinfra.lab.xkv.proto.Pdpb;

import java.util.HashSet;
import java.util.Set;

public final class LabelConstraint {

    private final String key;
    private final Pdpb.LabelConstraintOp op;
    private final Set<String> values;

    public LabelConstraint(String key, Pdpb.LabelConstraintOp op, Iterable<String> values) {
        this.key = key;
        this.op = op;
        this.values = new HashSet<>();
        for (String v : values) this.values.add(v);
    }

    public static LabelConstraint fromProto(Pdpb.LabelConstraint proto) {
        return new LabelConstraint(proto.getKey(), proto.getOp(), proto.getValuesList());
    }

    public Pdpb.LabelConstraint toProto() {
        return Pdpb.LabelConstraint.newBuilder()
                .setKey(key)
                .setOp(op)
                .addAllValues(values)
                .build();
    }

    public boolean matches(Metapb.Store store) {
        String storeValue = null;
        for (var label : store.getLabelsList()) {
            if (label.getKey().equals(key)) {
                storeValue = label.getValue();
                break;
            }
        }

        switch (op) {
            case IN:
                return storeValue != null && values.contains(storeValue);
            case NOT_IN:
                return storeValue == null || !values.contains(storeValue);
            case EXISTS:
                return storeValue != null;
            case NOT_EXISTS:
                return storeValue == null;
            default:
                return false;
        }
    }

    public String key() { return key; }
    public Pdpb.LabelConstraintOp op() { return op; }
    public Set<String> values() { return values; }
}
