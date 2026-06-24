package io.github.xinfra.lab.xkv.pd.state.placement;

import com.google.protobuf.ByteString;
import io.github.xinfra.lab.xkv.proto.Metapb;
import io.github.xinfra.lab.xkv.proto.Pdpb;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class PlacementRuleManagerTest {

    private PlacementRuleManager mgr;

    @BeforeEach
    void setUp() {
        mgr = new PlacementRuleManager();
    }

    @Test
    void seedDefaultCreatesVoterRule() {
        mgr.seedDefault(3);
        var rules = mgr.getRules();
        assertThat(rules).hasSize(1);
        assertThat(rules.get(0).groupId()).isEqualTo("pd");
        assertThat(rules.get(0).id()).isEqualTo("default");
        assertThat(rules.get(0).count()).isEqualTo(3);
        assertThat(rules.get(0).isVoter()).isTrue();
    }

    @Test
    void seedDefaultIdempotent() {
        mgr.seedDefault(3);
        mgr.seedDefault(5);
        assertThat(mgr.getRules()).hasSize(1);
        assertThat(mgr.getRules().get(0).count()).isEqualTo(3);
    }

    @Test
    void setAndDeleteRule() {
        var rule = new PlacementRule("custom", "r1", 0, false,
                null, null, "voter", 2, List.of(), List.of());
        mgr.setRule(rule);
        assertThat(mgr.getRule("custom", "r1")).isNotNull();
        assertThat(mgr.ruleCount()).isEqualTo(1);

        mgr.deleteRule("custom", "r1");
        assertThat(mgr.getRule("custom", "r1")).isNull();
        assertThat(mgr.ruleCount()).isEqualTo(0);
    }

    @Test
    void rulesForRegionMatchesByKeyRange() {
        mgr.setRule(new PlacementRule("pd", "all", 0, false,
                null, null, "voter", 3, List.of(), List.of()));
        mgr.setRule(new PlacementRule("pd", "partial", 1, false,
                "a".getBytes(), "m".getBytes(), "learner", 1, List.of(), List.of()));

        var regionInRange = Metapb.Region.newBuilder().setId(1)
                .setStartKey(ByteString.copyFromUtf8("b"))
                .setEndKey(ByteString.copyFromUtf8("f")).build();
        var matching = mgr.rulesForRegion(regionInRange);
        assertThat(matching).hasSize(2);

        var regionOutOfRange = Metapb.Region.newBuilder().setId(2)
                .setStartKey(ByteString.copyFromUtf8("n"))
                .setEndKey(ByteString.copyFromUtf8("z")).build();
        var matching2 = mgr.rulesForRegion(regionOutOfRange);
        assertThat(matching2).hasSize(1);
        assertThat(matching2.get(0).id()).isEqualTo("all");
    }

    @Test
    void encodeDecodeRoundTrip() {
        mgr.seedDefault(3);
        mgr.setRule(new PlacementRule("custom", "r1", 5, false,
                "x".getBytes(), "z".getBytes(), "learner", 1,
                List.of(new LabelConstraint("zone", Pdpb.LabelConstraintOp.IN,
                        List.of("us-west"))),
                List.of("zone", "rack")));

        byte[] data = mgr.encode();
        var restored = new PlacementRuleManager();
        restored.decode(data);

        assertThat(restored.ruleCount()).isEqualTo(2);
        var r1 = restored.getRule("custom", "r1");
        assertThat(r1).isNotNull();
        assertThat(r1.count()).isEqualTo(1);
        assertThat(r1.isLearner()).isTrue();
        assertThat(r1.locationLabels()).containsExactly("zone", "rack");
        assertThat(r1.labelConstraints()).hasSize(1);
    }

    @Test
    void isolationScoreMaxWhenAllLabelsDiffer() {
        var candidate = storeWithLabels(1, "zone", "us-west", "rack", "r1", "host", "h1");
        var existing = List.of(
                storeWithLabels(2, "zone", "us-east", "rack", "r2", "host", "h2")
        );
        int score = mgr.isolationScore(candidate, existing, List.of("zone", "rack", "host"));
        assertThat(score).isEqualTo(3);
    }

    @Test
    void isolationScoreDecreasesWithSharedLabels() {
        var candidate = storeWithLabels(1, "zone", "us-east", "rack", "r1", "host", "h1");
        var existing = List.of(
                storeWithLabels(2, "zone", "us-east", "rack", "r2", "host", "h2")
        );
        int score = mgr.isolationScore(candidate, existing, List.of("zone", "rack", "host"));
        assertThat(score).isEqualTo(2);
    }

    @Test
    void isolationScoreZeroWhenAllShared() {
        var candidate = storeWithLabels(1, "zone", "us-east", "rack", "r1", "host", "h1");
        var existing = List.of(
                storeWithLabels(2, "zone", "us-east", "rack", "r1", "host", "h1")
        );
        int score = mgr.isolationScore(candidate, existing, List.of("zone", "rack", "host"));
        assertThat(score).isEqualTo(0);
    }

    @Test
    void isolationScoreZeroWhenNoLocationLabels() {
        var candidate = storeWithLabels(1, "zone", "us-west");
        var existing = List.of(storeWithLabels(2, "zone", "us-east"));
        int score = mgr.isolationScore(candidate, existing, List.of());
        assertThat(score).isEqualTo(0);
    }

    private static Metapb.Store storeWithLabels(long id, String... kvPairs) {
        var b = Metapb.Store.newBuilder().setId(id).setState(Metapb.StoreState.Up);
        for (int i = 0; i < kvPairs.length; i += 2) {
            b.addLabels(Metapb.StoreLabel.newBuilder()
                    .setKey(kvPairs[i]).setValue(kvPairs[i + 1]));
        }
        return b.build();
    }
}
