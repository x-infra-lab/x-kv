package io.github.xinfra.lab.xkv.kv.cdc;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Consumer;

public final class CdcEventBus {
    private static final Logger log = LoggerFactory.getLogger(CdcEventBus.class);

    private final ConcurrentHashMap<Long, CopyOnWriteArrayList<Consumer<CdcEvent>>> subscribers =
            new ConcurrentHashMap<>();

    public void subscribe(long regionId, Consumer<CdcEvent> subscriber) {
        subscribers.computeIfAbsent(regionId, k -> new CopyOnWriteArrayList<>())
                .add(subscriber);
    }

    public void unsubscribe(long regionId, Consumer<CdcEvent> subscriber) {
        var list = subscribers.get(regionId);
        if (list != null) {
            list.remove(subscriber);
            if (list.isEmpty()) subscribers.remove(regionId);
        }
    }

    public void publish(CdcEvent event) {
        var list = subscribers.get(event.regionId());
        if (list == null) return;
        for (var sub : list) {
            try {
                sub.accept(event);
            } catch (Throwable t) {
                log.warn("CDC subscriber failed region={}: {}", event.regionId(), t.getMessage());
            }
        }
    }

    public boolean hasSubscribers(long regionId) {
        var list = subscribers.get(regionId);
        return list != null && !list.isEmpty();
    }
}
