package io.github.xinfra.lab.xkv.pd.state.placement;

import io.github.xinfra.lab.xkv.proto.Metapb;
import io.github.xinfra.lab.xkv.proto.Pdpb;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

public final class PlacementRuleManager {

    public static final String DEFAULT_GROUP = "pd";
    public static final String DEFAULT_RULE_ID = "default";

    private final ConcurrentHashMap<String, PlacementRule> rules = new ConcurrentHashMap<>();

    public PlacementRuleManager() {}

    public void seedDefault(int maxPeerCount) {
        if (maxPeerCount <= 0) maxPeerCount = 3;
        String key = DEFAULT_GROUP + "/" + DEFAULT_RULE_ID;
        rules.putIfAbsent(key, new PlacementRule(
                DEFAULT_GROUP, DEFAULT_RULE_ID, 0, false,
                null, null, "voter", maxPeerCount,
                List.of(), List.of()
        ));
    }

    public void setRule(PlacementRule rule) {
        rules.put(rule.key(), rule);
    }

    public boolean deleteRule(String groupId, String id) {
        return rules.remove(groupId + "/" + id) != null;
    }

    public PlacementRule getRule(String groupId, String id) {
        return rules.get(groupId + "/" + id);
    }

    public List<PlacementRule> getRules() {
        List<PlacementRule> result = new ArrayList<>(rules.values());
        Collections.sort(result);
        return result;
    }

    public List<PlacementRule> rulesForRegion(Metapb.Region region) {
        List<PlacementRule> matching = new ArrayList<>();
        for (var rule : rules.values()) {
            if (rule.matchesRegion(region)) {
                matching.add(rule);
            }
        }
        Collections.sort(matching);
        return matching;
    }

    public int isolationScore(Metapb.Store candidate,
                               List<Metapb.Store> existingPeerStores,
                               List<String> locationLabels) {
        if (locationLabels.isEmpty()) return 0;

        int score = 0;
        for (String label : locationLabels) {
            String candidateVal = labelValue(candidate, label);
            boolean allDiffer = true;
            for (var existing : existingPeerStores) {
                String existingVal = labelValue(existing, label);
                if (candidateVal != null && candidateVal.equals(existingVal)) {
                    allDiffer = false;
                    break;
                }
            }
            if (allDiffer) score++;
        }
        return score;
    }

    private static String labelValue(Metapb.Store store, String key) {
        for (var label : store.getLabelsList()) {
            if (label.getKey().equals(key)) return label.getValue();
        }
        return null;
    }

    public byte[] encode() {
        List<PlacementRule> all = getRules();
        List<byte[]> protos = new ArrayList<>(all.size());
        int totalLen = 4;
        for (var rule : all) {
            byte[] data = rule.toProto().toByteArray();
            protos.add(data);
            totalLen += 4 + data.length;
        }
        ByteBuffer buf = ByteBuffer.allocate(totalLen).order(ByteOrder.BIG_ENDIAN);
        buf.putInt(all.size());
        for (var data : protos) {
            buf.putInt(data.length);
            buf.put(data);
        }
        return buf.array();
    }

    public void decode(byte[] snapshot) {
        if (snapshot == null || snapshot.length < 4) return;
        ByteBuffer buf = ByteBuffer.wrap(snapshot).order(ByteOrder.BIG_ENDIAN);
        int count = buf.getInt();
        rules.clear();
        for (int i = 0; i < count; i++) {
            int len = buf.getInt();
            byte[] data = new byte[len];
            buf.get(data);
            try {
                Pdpb.PlacementRule proto = Pdpb.PlacementRule.parseFrom(data);
                PlacementRule rule = PlacementRule.fromProto(proto);
                rules.put(rule.key(), rule);
            } catch (Exception e) {
                // skip corrupt entries
            }
        }
    }

    public int ruleCount() {
        return rules.size();
    }
}
