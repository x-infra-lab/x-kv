package io.github.xinfra.lab.xkv.kv.raft;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Fixed-size thread pool that processes Raft Ready cycles for all regions.
 *
 * <p>Replaces the per-region dedicated {@code readyThread} in
 * {@link RegionPeerImpl}. Instead of one thread per region blocking on
 * {@code node.ready()}, a small number of poller threads (default:
 * {@code max(4, availableProcessors())}) pick regions from a shared
 * ready-queue and drive their Raft state machines.
 *
 * <p>Each region is wrapped in a {@link RegionMailbox}. When a mailbox is
 * woken up (by tick, propose, step, or readIndex), it is placed on the
 * ready-queue. A poller thread picks it, drains pending events into the
 * underlying {@code RawNode}, then checks {@code hasReady()} and processes
 * any Ready cycle. If more work remains, the mailbox is re-enqueued.
 *
 * <p>Thread count at 200 regions:
 * <ul>
 *   <li>Before (per-region): 200 readyThreads + 200 tickTimers = 400+ threads</li>
 *   <li>After (BatchSystem): N pollerThreads + 1 tickDriver = N+1 threads</li>
 * </ul>
 */
public final class RaftPoller {
    private static final Logger log = LoggerFactory.getLogger(RaftPoller.class);

    private final BlockingQueue<RegionMailbox> readyQueue = new LinkedBlockingQueue<>();
    private final Thread[] workers;
    private final AtomicBoolean running = new AtomicBoolean(true);

    public RaftPoller(int threads) {
        this.workers = new Thread[threads];
        for (int i = 0; i < threads; i++) {
            workers[i] = new Thread(this::pollLoop, "raft-poller-" + i);
            workers[i].setDaemon(true);
            workers[i].start();
        }
    }

    public RaftPoller() {
        this(Math.max(4, Runtime.getRuntime().availableProcessors()));
    }

    void schedule(RegionMailbox mailbox) {
        readyQueue.offer(mailbox);
    }

    private void pollLoop() {
        while (running.get()) {
            try {
                var mailbox = readyQueue.take();
                if (!mailbox.isRunning()) continue;
                mailbox.processOnce(this);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            } catch (Throwable t) {
                log.error("poller loop failure", t);
                try { Thread.sleep(50); } catch (InterruptedException ie) { return; }
            }
        }
    }

    public void shutdown() {
        running.set(false);
        for (var w : workers) {
            w.interrupt();
        }
        for (var w : workers) {
            try { w.join(5_000); } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }
}
