package io.github.xinfra.lab.xkv.pd.state;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

public final class SchedulerManager {
    private static final Logger log = LoggerFactory.getLogger(SchedulerManager.class);

    public record SchedulerStatus(String name, boolean running, boolean paused) {}

    private interface PausableScheduler {
        void pause();
        void resume();
        boolean isPaused();
    }

    private record Entry(String name, AutoCloseable scheduler, PausableScheduler pausable) {}

    private final ConcurrentHashMap<String, Entry> schedulers = new ConcurrentHashMap<>();

    public void register(String name, LeaderBalanceScheduler s) {
        schedulers.put(name, new Entry(name, s, new PausableScheduler() {
            @Override public void pause()  { s.pause(); }
            @Override public void resume() { s.resume(); }
            @Override public boolean isPaused() { return s.isPaused(); }
        }));
        log.info("SchedulerManager: registered '{}'", name);
    }

    public void register(String name, RegionBalanceScheduler s) {
        schedulers.put(name, new Entry(name, s, new PausableScheduler() {
            @Override public void pause()  { s.pause(); }
            @Override public void resume() { s.resume(); }
            @Override public boolean isPaused() { return s.isPaused(); }
        }));
        log.info("SchedulerManager: registered '{}'", name);
    }

    public void register(String name, HotRegionScheduler s) {
        schedulers.put(name, new Entry(name, s, new PausableScheduler() {
            @Override public void pause()  { s.pause(); }
            @Override public void resume() { s.resume(); }
            @Override public boolean isPaused() { return s.isPaused(); }
        }));
        log.info("SchedulerManager: registered '{}'", name);
    }

    public void register(String name, SplitCheckerScheduler s) {
        schedulers.put(name, new Entry(name, s, new PausableScheduler() {
            @Override public void pause()  { s.pause(); }
            @Override public void resume() { s.resume(); }
            @Override public boolean isPaused() { return s.isPaused(); }
        }));
        log.info("SchedulerManager: registered '{}'", name);
    }

    public void register(String name, MergeCheckerScheduler s) {
        schedulers.put(name, new Entry(name, s, new PausableScheduler() {
            @Override public void pause()  { s.pause(); }
            @Override public void resume() { s.resume(); }
            @Override public boolean isPaused() { return s.isPaused(); }
        }));
        log.info("SchedulerManager: registered '{}'", name);
    }

    public void register(String name, RuleCheckerScheduler s) {
        schedulers.put(name, new Entry(name, s, new PausableScheduler() {
            @Override public void pause()  { s.pause(); }
            @Override public void resume() { s.resume(); }
            @Override public boolean isPaused() { return s.isPaused(); }
        }));
        log.info("SchedulerManager: registered '{}'", name);
    }

    public void unregister(String name) {
        schedulers.remove(name);
    }

    public void unregisterAll() {
        schedulers.clear();
    }

    public boolean pause(String name) {
        var entry = schedulers.get(name);
        if (entry == null) return false;
        entry.pausable().pause();
        log.info("SchedulerManager: paused '{}'", name);
        return true;
    }

    public boolean resume(String name) {
        var entry = schedulers.get(name);
        if (entry == null) return false;
        entry.pausable().resume();
        log.info("SchedulerManager: resumed '{}'", name);
        return true;
    }

    public List<SchedulerStatus> list() {
        var result = new ArrayList<SchedulerStatus>();
        for (var entry : schedulers.values()) {
            result.add(new SchedulerStatus(entry.name(), true, entry.pausable().isPaused()));
        }
        result.sort((a, b) -> a.name().compareTo(b.name()));
        return Collections.unmodifiableList(result);
    }

    public SchedulerStatus getStatus(String name) {
        var entry = schedulers.get(name);
        if (entry == null) return null;
        return new SchedulerStatus(entry.name(), true, entry.pausable().isPaused());
    }

    public int size() {
        return schedulers.size();
    }
}
