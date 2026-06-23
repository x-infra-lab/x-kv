package io.github.xinfra.lab.xkv.kv.raft;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Single shared tick timer for all regions.
 *
 * <p>Replaces the per-region {@code ScheduledExecutorService} in
 * {@link RegionPeerImpl}. One timer thread fires periodically and enqueues
 * a TICK event to each registered {@link RegionMailbox}, then wakes it on
 * the shared {@link RaftPoller}.
 *
 * <p>Thread saving: N per-region timers → 1 shared timer.
 */
public final class TickDriver {
    private static final Logger log = LoggerFactory.getLogger(TickDriver.class);

    private final List<RegionMailbox> mailboxes = new CopyOnWriteArrayList<>();
    private final ScheduledExecutorService timer;

    public TickDriver(long tickIntervalMs) {
        this.timer = Executors.newSingleThreadScheduledExecutor(r -> {
            var t = new Thread(r, "raft-tick-driver");
            t.setDaemon(true);
            return t;
        });
        this.timer.scheduleAtFixedRate(this::tickAll,
                tickIntervalMs, tickIntervalMs, TimeUnit.MILLISECONDS);
    }

    public void register(RegionMailbox mailbox) {
        mailboxes.add(mailbox);
    }

    public void unregister(RegionMailbox mailbox) {
        mailboxes.remove(mailbox);
    }

    private void tickAll() {
        for (var mb : mailboxes) {
            if (!mb.isRunning()) continue;
            try {
                mb.enqueueTick();
            } catch (Throwable t) {
                log.warn("tick failed for region={}: {}", mb.regionId(), t.getMessage());
            }
        }
    }

    public void shutdown() {
        timer.shutdownNow();
    }
}
