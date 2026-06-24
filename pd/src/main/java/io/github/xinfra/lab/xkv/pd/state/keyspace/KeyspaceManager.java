package io.github.xinfra.lab.xkv.pd.state.keyspace;

import io.github.xinfra.lab.xkv.proto.Pdpb;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

public final class KeyspaceManager {
    private static final Logger log = LoggerFactory.getLogger(KeyspaceManager.class);

    public static final int DEFAULT_KEYSPACE_ID = 0;
    public static final String DEFAULT_KEYSPACE_NAME = "DEFAULT";

    private final ConcurrentHashMap<Integer, Pdpb.KeyspaceMeta> byId = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, Integer> byName = new ConcurrentHashMap<>();
    private final AtomicInteger nextId = new AtomicInteger(1);

    public KeyspaceManager() {
        seedDefault();
    }

    private void seedDefault() {
        var meta = Pdpb.KeyspaceMeta.newBuilder()
                .setId(DEFAULT_KEYSPACE_ID)
                .setName(DEFAULT_KEYSPACE_NAME)
                .setState(Pdpb.KeyspaceState.ENABLED)
                .setCreatedAt(System.currentTimeMillis())
                .build();
        byId.putIfAbsent(DEFAULT_KEYSPACE_ID, meta);
        byName.putIfAbsent(DEFAULT_KEYSPACE_NAME, DEFAULT_KEYSPACE_ID);
    }

    public Pdpb.KeyspaceMeta createKeyspace(String name, java.util.Map<String, String> config) {
        if (byName.containsKey(name)) {
            return null;
        }
        int id = nextId.getAndIncrement();
        var builder = Pdpb.KeyspaceMeta.newBuilder()
                .setId(id)
                .setName(name)
                .setState(Pdpb.KeyspaceState.ENABLED)
                .setCreatedAt(System.currentTimeMillis());
        if (config != null) {
            builder.putAllConfig(config);
        }
        var meta = builder.build();
        byId.put(id, meta);
        byName.put(name, id);
        log.info("keyspace created: id={} name={}", id, name);
        return meta;
    }

    public void setKeyspace(Pdpb.KeyspaceMeta meta) {
        byId.put(meta.getId(), meta);
        byName.put(meta.getName(), meta.getId());
        if (meta.getId() >= nextId.get()) {
            nextId.set(meta.getId() + 1);
        }
    }

    public Pdpb.KeyspaceMeta loadByName(String name) {
        Integer id = byName.get(name);
        if (id == null) return null;
        return byId.get(id);
    }

    public Pdpb.KeyspaceMeta loadById(int id) {
        return byId.get(id);
    }

    public Pdpb.KeyspaceMeta updateState(int id, Pdpb.KeyspaceState newState) {
        var existing = byId.get(id);
        if (existing == null) return null;

        if (!isValidTransition(existing.getState(), newState)) {
            return null;
        }

        var updated = existing.toBuilder().setState(newState).build();
        byId.put(id, updated);
        return updated;
    }

    public List<Pdpb.KeyspaceMeta> listKeyspaces(Pdpb.KeyspaceState stateFilter) {
        var result = new ArrayList<Pdpb.KeyspaceMeta>();
        for (var meta : byId.values()) {
            if (stateFilter == null || meta.getState() == stateFilter) {
                result.add(meta);
            }
        }
        result.sort((a, b) -> Integer.compare(a.getId(), b.getId()));
        return Collections.unmodifiableList(result);
    }

    public List<Pdpb.KeyspaceMeta> listAll() {
        return listKeyspaces(null);
    }

    public int size() {
        return byId.size();
    }

    public List<Pdpb.KeyspaceMeta> encode() {
        return new ArrayList<>(byId.values());
    }

    public void decode(List<Pdpb.KeyspaceMeta> keyspaces) {
        byId.clear();
        byName.clear();
        for (var ks : keyspaces) {
            byId.put(ks.getId(), ks);
            byName.put(ks.getName(), ks.getId());
            if (ks.getId() >= nextId.get()) {
                nextId.set(ks.getId() + 1);
            }
        }
        if (!byId.containsKey(DEFAULT_KEYSPACE_ID)) {
            seedDefault();
        }
    }

    private static boolean isValidTransition(Pdpb.KeyspaceState from, Pdpb.KeyspaceState to) {
        return switch (from) {
            case ENABLED -> to == Pdpb.KeyspaceState.DISABLED;
            case DISABLED -> to == Pdpb.KeyspaceState.ENABLED || to == Pdpb.KeyspaceState.ARCHIVED;
            case ARCHIVED -> to == Pdpb.KeyspaceState.TOMBSTONE;
            default -> false;
        };
    }
}
