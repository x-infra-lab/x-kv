package io.github.xinfra.lab.xkv.pd.state;

import io.github.xinfra.lab.xkv.proto.Metapb;
import io.github.xinfra.lab.xkv.proto.Pdpb;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Per-store region-count balance.
 *
 * <p>Counts the number of regions whose peer list includes each store (a
 * store "hosts" the region). Identifies the most- and least-loaded
 * stores; when the difference exceeds {@link #MAX_REGIONS_DIFF}, enqueues
 * a {@code ChangePeer(AddNode, target=under-loaded)} operator on a
 * region currently hosted only on over-loaded stores. Removing the peer
 * on the over-loaded store is a follow-up step the actual conf-change
 * apply path will need; for now this scheduler just primes the operator
 * channel and proves the policy logic.
 *
 * <h3>Algorithm trade-offs</h3>
 *
 * <ul>
 *   <li>Region-count is a proxy for store load; production-grade PD adds
 *       store-capacity-in-bytes and hot-region weighting. Tracked in the
 *       deferred list.</li>
 *   <li>Caps {@link #MAX_OPERATORS_PER_TICK} to avoid storming the
 *       cluster with simultaneous AddPeer operators that all race for
 *       quorum on the under-loaded store.</li>
 * </ul>
 */
public final class RegionBalanceScheduler implements AutoCloseable {
    private static final Logger log = LoggerFactory.getLogger(RegionBalanceScheduler.class);

    /** Imbalance threshold — only enqueue when max-min strictly exceeds this. */
    public static final int MAX_REGIONS_DIFF = 1;
    /** Per-tick operator cap. */
    public static final int MAX_OPERATORS_PER_TICK = 4;

    private final PdStateMachine state;
    private final OperatorQueue operators;
    private final StoreStatsCache storeStats;
    private final long intervalMs;
    private final ScheduledExecutorService timer;
    private final AtomicLong ticksTotal = new AtomicLong();
    private final AtomicLong operatorsScheduled = new AtomicLong();
    private volatile boolean closed = false;

    /** Stores with available ratio below this threshold are not AddPeer targets. */
    private static final double LOW_SPACE_RATIO = 0.05;

    public RegionBalanceScheduler(PdStateMachine state, OperatorQueue operators, long intervalMs) {
        this(state, operators, new StoreStatsCache(), intervalMs);
    }

    public RegionBalanceScheduler(PdStateMachine state, OperatorQueue operators,
                                   StoreStatsCache storeStats, long intervalMs) {
        this.state = state;
        this.operators = operators;
        this.storeStats = storeStats;
        this.intervalMs = intervalMs;
        this.timer = Executors.newSingleThreadScheduledExecutor(r -> {
            var t = new Thread(r, "pd-region-balance");
            t.setDaemon(true);
            return t;
        });
    }

    public void start() {
        timer.scheduleAtFixedRate(this::tickSafely, intervalMs, intervalMs, TimeUnit.MILLISECONDS);
        log.info("RegionBalanceScheduler started: interval={}ms diff_threshold={}",
                intervalMs, MAX_REGIONS_DIFF);
    }

    public long ticksTotal() { return ticksTotal.get(); }
    public long operatorsScheduled() { return operatorsScheduled.get(); }

    private void tickSafely() {
        if (closed) return;
        try { runOnce(); }
        catch (Throwable t) { log.warn("region-balance tick failed: {}", t.getMessage()); }
    }

    /** Visible for tests. Returns operators enqueued this round. */
    public int runOnce() {
        ticksTotal.incrementAndGet();

        // Bucket: storeId → list of regions hosted on that store.
        // We ALSO track each region's planned store set in a parallel map
        // so that an AddPeer scheduled THIS round is reflected in subsequent
        // iterations (without it, we'd happily AddPeer the same region to
        // the same store twice in one tick).
        var byStore = new HashMap<Long, java.util.List<Metapb.Region>>();
        var regionStores = new HashMap<Long, java.util.Set<Long>>();
        for (var s : state.allStores()) {
            byStore.put(s.getId(), new ArrayList<>());
        }
        for (var r : state.allRegions()) {
            var stores = new java.util.HashSet<Long>(r.getPeersCount());
            for (var p : r.getPeersList()) {
                byStore.computeIfAbsent(p.getStoreId(), k -> new ArrayList<>()).add(r);
                stores.add(p.getStoreId());
            }
            regionStores.put(r.getId(), stores);
        }
        if (byStore.size() < 2) return 0;

        int scheduled = 0;
        var counts = new HashMap<Long, Integer>();
        for (var e : byStore.entrySet()) counts.put(e.getKey(), e.getValue().size());

        while (scheduled < MAX_OPERATORS_PER_TICK) {
            long maxStore = -1;
            long minStore = -1;
            int max = Integer.MIN_VALUE;
            int min = Integer.MAX_VALUE;
            for (var e : counts.entrySet()) {
                if (e.getValue() > max) { max = e.getValue(); maxStore = e.getKey(); }
                // Skip busy stores and stores with critically low disk space.
                if (e.getValue() < min
                        && !storeStats.isBusy(e.getKey())
                        && storeStats.availableRatio(e.getKey()) >= LOW_SPACE_RATIO) {
                    min = e.getValue(); minStore = e.getKey();
                }
            }
            if (minStore == -1 || max - min <= MAX_REGIONS_DIFF) break;

            // Find a region currently hosted on maxStore but NOT on minStore
            // — accounting for AddPeer operators we've already scheduled this
            // round (those targets are in regionStores).
            Metapb.Region target = null;
            for (var r : byStore.get(maxStore)) {
                if (!regionStores.get(r.getId()).contains(minStore)) {
                    target = r;
                    break;
                }
            }
            if (target == null) {
                // No region on maxStore that's not already (planned to be)
                // on minStore — bump min's count synthetically so the loop
                // moves on instead of spinning.
                counts.put(minStore, max);
                continue;
            }

            long newPeerId = state.allocId(1);
            var newPeer = Metapb.Peer.newBuilder()
                    .setId(newPeerId).setStoreId(minStore).setRole(Metapb.PeerRole.Voter).build();
            operators.scheduleChangePeer(target.getId(), newPeer, Pdpb.ConfChangeType.AddNode);
            operatorsScheduled.incrementAndGet();
            scheduled++;
            byStore.computeIfAbsent(minStore, k -> new ArrayList<>()).add(target);
            regionStores.get(target.getId()).add(minStore);
            counts.merge(minStore, 1, Integer::sum);
            // We DON'T decrement maxStore — region is still on max, the
            // RemovePeer half is left to a separate (future) operator step.
        }
        if (scheduled > 0) {
            log.info("region-balance: scheduled {} AddPeer operators", scheduled);
        }
        return scheduled;
    }

    @Override
    public void close() {
        closed = true;
        timer.shutdownNow();
    }
}
