package io.github.xinfra.lab.xkv.pd.state.keyspace;

import io.github.xinfra.lab.xkv.proto.Pdpb;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

public final class ResourceGroupManager {
    private static final Logger log = LoggerFactory.getLogger(ResourceGroupManager.class);

    public static final String DEFAULT_GROUP_NAME = "default";

    private final ConcurrentHashMap<String, Pdpb.ResourceGroup> groups = new ConcurrentHashMap<>();

    public ResourceGroupManager() {
        seedDefault();
    }

    private void seedDefault() {
        var defaultGroup = Pdpb.ResourceGroup.newBuilder()
                .setName(DEFAULT_GROUP_NAME)
                .setRuSettings(Pdpb.RUSettings.newBuilder()
                        .setFillRate(0)
                        .setBurstLimit(0)
                        .build())
                .setPriority(0)
                .setBackground(false)
                .build();
        groups.putIfAbsent(DEFAULT_GROUP_NAME, defaultGroup);
    }

    public boolean addGroup(Pdpb.ResourceGroup group) {
        if (group.getName().isEmpty()) return false;
        if (groups.containsKey(group.getName())) return false;
        groups.put(group.getName(), group);
        log.info("resource group added: name={} fillRate={} burstLimit={}",
                group.getName(),
                group.getRuSettings().getFillRate(),
                group.getRuSettings().getBurstLimit());
        return true;
    }

    public boolean modifyGroup(Pdpb.ResourceGroup group) {
        if (!groups.containsKey(group.getName())) return false;
        groups.put(group.getName(), group);
        log.info("resource group modified: name={} fillRate={} burstLimit={}",
                group.getName(),
                group.getRuSettings().getFillRate(),
                group.getRuSettings().getBurstLimit());
        return true;
    }

    public void setGroup(Pdpb.ResourceGroup group) {
        groups.put(group.getName(), group);
    }

    public boolean deleteGroup(String name) {
        if (DEFAULT_GROUP_NAME.equals(name)) return false;
        boolean removed = groups.remove(name) != null;
        if (removed) {
            log.info("resource group deleted: name={}", name);
        }
        return removed;
    }

    public Pdpb.ResourceGroup getGroup(String name) {
        return groups.get(name);
    }

    public List<Pdpb.ResourceGroup> listGroups() {
        var result = new ArrayList<>(groups.values());
        result.sort((a, b) -> a.getName().compareTo(b.getName()));
        return Collections.unmodifiableList(result);
    }

    public int size() {
        return groups.size();
    }

    public List<Pdpb.ResourceGroup> encode() {
        return new ArrayList<>(groups.values());
    }

    public void decode(List<Pdpb.ResourceGroup> groupList) {
        groups.clear();
        for (var g : groupList) {
            groups.put(g.getName(), g);
        }
        if (!groups.containsKey(DEFAULT_GROUP_NAME)) {
            seedDefault();
        }
    }
}
