package io.github.xinfra.lab.xkv.pd.state.placement;

import io.github.xinfra.lab.xkv.proto.Metapb;
import io.github.xinfra.lab.xkv.proto.Pdpb;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class LabelConstraintTest {

    private static Metapb.Store storeWithLabels(long id, String... kvPairs) {
        var b = Metapb.Store.newBuilder().setId(id).setState(Metapb.StoreState.Up);
        for (int i = 0; i < kvPairs.length; i += 2) {
            b.addLabels(Metapb.StoreLabel.newBuilder()
                    .setKey(kvPairs[i]).setValue(kvPairs[i + 1]));
        }
        return b.build();
    }

    @Test
    void inMatchesWhenValuePresent() {
        var c = new LabelConstraint("zone", Pdpb.LabelConstraintOp.IN, List.of("us-east", "us-west"));
        assertThat(c.matches(storeWithLabels(1, "zone", "us-east"))).isTrue();
        assertThat(c.matches(storeWithLabels(2, "zone", "us-west"))).isTrue();
        assertThat(c.matches(storeWithLabels(3, "zone", "eu-central"))).isFalse();
        assertThat(c.matches(storeWithLabels(4))).isFalse();
    }

    @Test
    void notInRejectsMatchingValue() {
        var c = new LabelConstraint("zone", Pdpb.LabelConstraintOp.NOT_IN, List.of("eu-central"));
        assertThat(c.matches(storeWithLabels(1, "zone", "us-east"))).isTrue();
        assertThat(c.matches(storeWithLabels(2, "zone", "eu-central"))).isFalse();
        assertThat(c.matches(storeWithLabels(3))).isTrue();
    }

    @Test
    void existsChecksKeyPresence() {
        var c = new LabelConstraint("rack", Pdpb.LabelConstraintOp.EXISTS, List.of());
        assertThat(c.matches(storeWithLabels(1, "rack", "r1"))).isTrue();
        assertThat(c.matches(storeWithLabels(2, "zone", "us-east"))).isFalse();
    }

    @Test
    void notExistsChecksKeyAbsence() {
        var c = new LabelConstraint("ssd", Pdpb.LabelConstraintOp.NOT_EXISTS, List.of());
        assertThat(c.matches(storeWithLabels(1, "zone", "us-east"))).isTrue();
        assertThat(c.matches(storeWithLabels(2, "ssd", "true"))).isFalse();
    }

    @Test
    void protoRoundTrip() {
        var original = new LabelConstraint("zone", Pdpb.LabelConstraintOp.IN,
                List.of("a", "b"));
        var proto = original.toProto();
        var restored = LabelConstraint.fromProto(proto);
        assertThat(restored.key()).isEqualTo("zone");
        assertThat(restored.op()).isEqualTo(Pdpb.LabelConstraintOp.IN);
        assertThat(restored.values()).containsExactlyInAnyOrder("a", "b");
    }
}
