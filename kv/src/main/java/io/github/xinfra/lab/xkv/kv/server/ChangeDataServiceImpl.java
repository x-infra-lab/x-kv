package io.github.xinfra.lab.xkv.kv.server;

import com.google.protobuf.ByteString;
import io.github.xinfra.lab.xkv.kv.cdc.CdcEvent;
import io.github.xinfra.lab.xkv.kv.cdc.CdcEventBus;
import io.github.xinfra.lab.xkv.kv.cdc.CdcIncrementalScanner;
import io.github.xinfra.lab.xkv.kv.cdc.RegionResolvedTsTracker;
import io.github.xinfra.lab.xkv.kv.engine.StorageEngine;
import io.github.xinfra.lab.xkv.proto.Cdcpb;
import io.github.xinfra.lab.xkv.proto.ChangeDataGrpc;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.LongSupplier;

public final class ChangeDataServiceImpl extends ChangeDataGrpc.ChangeDataImplBase {
    private static final Logger log = LoggerFactory.getLogger(ChangeDataServiceImpl.class);
    private static final long RESOLVED_TS_INTERVAL_MS = 1_000;

    private final CdcEventBus eventBus;
    private final StorageEngine engine;
    private final LongSupplier fallbackTsSupplier;
    private final RegionResolvedTsTracker resolvedTsTracker;
    private final ScheduledExecutorService scheduler;

    public ChangeDataServiceImpl(CdcEventBus eventBus, LongSupplier fallbackTsSupplier) {
        this(eventBus, null, fallbackTsSupplier, null);
    }

    public ChangeDataServiceImpl(CdcEventBus eventBus, LongSupplier fallbackTsSupplier,
                                  RegionResolvedTsTracker resolvedTsTracker) {
        this(eventBus, null, fallbackTsSupplier, resolvedTsTracker);
    }

    public ChangeDataServiceImpl(CdcEventBus eventBus, StorageEngine engine,
                                  LongSupplier fallbackTsSupplier,
                                  RegionResolvedTsTracker resolvedTsTracker) {
        this.eventBus = eventBus;
        this.engine = engine;
        this.fallbackTsSupplier = fallbackTsSupplier;
        this.resolvedTsTracker = resolvedTsTracker;
        this.scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            var t = new Thread(r, "cdc-resolved-ts");
            t.setDaemon(true);
            return t;
        });
    }

    @Override
    public StreamObserver<Cdcpb.ChangeDataRequest> eventFeed(
            StreamObserver<Cdcpb.ChangeDataEvent> responseObserver) {
        return new StreamObserver<>() {
            private final Object sendLock = new Object();
            private final ConcurrentHashMap<Long, Consumer<CdcEvent>> subscriptions =
                    new ConcurrentHashMap<>();
            private volatile boolean closed;

            @Override
            public void onNext(Cdcpb.ChangeDataRequest req) {
                long regionId = req.getRegionId();
                if (req.hasRegister()) {
                    if (subscriptions.containsKey(regionId)) return;
                    byte[] filterStart = req.getStartKey().isEmpty()
                            ? null : req.getStartKey().toByteArray();
                    byte[] filterEnd = req.getEndKey().isEmpty()
                            ? null : req.getEndKey().toByteArray();
                    long checkpointTs = req.getCheckpointTs();

                    // Determine scanTs boundary: take a snapshot of the
                    // current max TS BEFORE subscribing so we can guarantee
                    // gap-free coverage: scan covers (checkpointTs, scanTs],
                    // live events cover (scanTs, ∞).
                    long scanTs = (checkpointTs > 0 && engine != null)
                            ? fallbackTsSupplier.getAsLong() : 0;

                    Consumer<CdcEvent> sub = event -> {
                        if (closed) return;
                        // When incremental scan is active, filter out events
                        // already covered by the scan window.
                        if (scanTs > 0 && event.commitTs() <= scanTs) return;
                        if (filterStart != null
                                && Arrays.compareUnsigned(event.key(), filterStart) < 0) return;
                        if (filterEnd != null
                                && Arrays.compareUnsigned(event.key(), filterEnd) >= 0) return;
                        sendEvent(event, responseObserver, sendLock, closed);
                    };
                    subscriptions.put(regionId, sub);
                    eventBus.subscribe(regionId, sub);

                    // Incremental scan: replay committed writes in (checkpointTs, scanTs].
                    if (scanTs > 0) {
                        try (var snapshot = engine.newSnapshot()) {
                            var events = CdcIncrementalScanner.scan(
                                    engine, snapshot, regionId,
                                    filterStart, filterEnd,
                                    checkpointTs, scanTs);
                            for (var event : events) {
                                if (closed) break;
                                sendEvent(event, responseObserver, sendLock, closed);
                            }
                            // Send resolved_ts = scanTs to mark the boundary.
                            synchronized (sendLock) {
                                if (!closed) {
                                    try {
                                        responseObserver.onNext(Cdcpb.ChangeDataEvent.newBuilder()
                                                .addEvents(Cdcpb.Event.newBuilder()
                                                        .setRegionId(regionId)
                                                        .setResolvedTs(scanTs))
                                                .build());
                                    } catch (Throwable t) {
                                        log.debug("CDC scan resolved_ts send failed: {}",
                                                t.getMessage());
                                    }
                                }
                            }
                        } catch (Throwable t) {
                            log.warn("CDC incremental scan failed region={}: {}",
                                    regionId, t.getMessage());
                        }
                    }

                    log.info("CDC EventFeed: registered region={} checkpointTs={}",
                            regionId, checkpointTs);
                } else if (req.hasDeregister()) {
                    var sub = subscriptions.remove(regionId);
                    if (sub != null) {
                        eventBus.unsubscribe(regionId, sub);
                        log.info("CDC EventFeed: deregistered region={}", regionId);
                    }
                }
            }

            @Override
            public void onError(Throwable t) {
                log.debug("CDC EventFeed stream error: {}", t.getMessage());
                cleanup();
            }

            @Override
            public void onCompleted() {
                cleanup();
                synchronized (sendLock) {
                    responseObserver.onCompleted();
                }
            }

            private void cleanup() {
                closed = true;
                for (var entry : subscriptions.entrySet()) {
                    eventBus.unsubscribe(entry.getKey(), entry.getValue());
                }
                subscriptions.clear();
            }
        };
    }

    @Override
    public StreamObserver<Cdcpb.ResolvedTsRequest> resolvedTs(
            StreamObserver<Cdcpb.ResolvedTsEvent> responseObserver) {
        return new StreamObserver<>() {
            private final Object sendLock = new Object();
            private final Set<Long> regions = ConcurrentHashMap.newKeySet();
            private volatile boolean closed;
            private ScheduledFuture<?> ticker;

            @Override
            public void onNext(Cdcpb.ResolvedTsRequest req) {
                regions.add(req.getRegionId());
                if (ticker == null) {
                    ticker = scheduler.scheduleAtFixedRate(this::pushResolvedTs,
                            RESOLVED_TS_INTERVAL_MS, RESOLVED_TS_INTERVAL_MS,
                            TimeUnit.MILLISECONDS);
                }
            }

            private void pushResolvedTs() {
                if (closed || regions.isEmpty()) return;
                long fallback = fallbackTsSupplier.getAsLong();
                synchronized (sendLock) {
                    if (closed) return;
                    for (long regionId : regions) {
                        long ts = resolvedTsTracker != null
                                ? resolvedTsTracker.resolvedTs(regionId, fallback)
                                : fallback;
                        var event = Cdcpb.ResolvedTsEvent.newBuilder()
                                .addRegions(regionId)
                                .setTs(ts)
                                .build();
                        try {
                            responseObserver.onNext(event);
                        } catch (Throwable t) {
                            log.debug("CDC ResolvedTs send failed region={}: {}",
                                    regionId, t.getMessage());
                            return;
                        }
                    }
                }
            }

            @Override
            public void onError(Throwable t) {
                log.debug("CDC ResolvedTs stream error: {}", t.getMessage());
                cleanup();
            }

            @Override
            public void onCompleted() {
                cleanup();
                synchronized (sendLock) {
                    responseObserver.onCompleted();
                }
            }

            private void cleanup() {
                closed = true;
                if (ticker != null) ticker.cancel(false);
                regions.clear();
            }
        };
    }

    private static void sendEvent(CdcEvent event,
                                    StreamObserver<Cdcpb.ChangeDataEvent> observer,
                                    Object sendLock, boolean closed) {
        var row = Cdcpb.Row.newBuilder()
                .setType(event.type())
                .setKey(ByteString.copyFrom(event.key()))
                .setStartTs(event.startTs())
                .setCommitTs(event.commitTs());
        if (event.value() != null) {
            row.setValue(ByteString.copyFrom(event.value()));
        }
        if (event.oldValue() != null) {
            row.setOldValue(ByteString.copyFrom(event.oldValue()));
        }
        var cdcEvent = Cdcpb.ChangeDataEvent.newBuilder()
                .addEvents(Cdcpb.Event.newBuilder()
                        .setRegionId(event.regionId())
                        .setEntries(Cdcpb.Entries.newBuilder()
                                .addEntries(row)))
                .build();
        synchronized (sendLock) {
            if (!closed) {
                try {
                    observer.onNext(cdcEvent);
                } catch (Throwable t) {
                    log.debug("CDC event send failed region={}: {}",
                            event.regionId(), t.getMessage());
                }
            }
        }
    }

    public void close() {
        scheduler.shutdownNow();
    }
}
