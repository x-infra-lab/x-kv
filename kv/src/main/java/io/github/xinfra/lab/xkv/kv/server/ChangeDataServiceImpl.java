package io.github.xinfra.lab.xkv.kv.server;

import com.google.protobuf.ByteString;
import io.github.xinfra.lab.xkv.kv.cdc.CdcEvent;
import io.github.xinfra.lab.xkv.kv.cdc.CdcEventBus;
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
    private final LongSupplier resolvedTsSupplier;
    private final ScheduledExecutorService scheduler;

    public ChangeDataServiceImpl(CdcEventBus eventBus, LongSupplier resolvedTsSupplier) {
        this.eventBus = eventBus;
        this.resolvedTsSupplier = resolvedTsSupplier;
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
                    Consumer<CdcEvent> sub = event -> {
                        if (closed) return;
                        if (filterStart != null
                                && Arrays.compareUnsigned(event.key(), filterStart) < 0) return;
                        if (filterEnd != null
                                && Arrays.compareUnsigned(event.key(), filterEnd) >= 0) return;
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
                                    responseObserver.onNext(cdcEvent);
                                } catch (Throwable t) {
                                    log.debug("CDC event send failed region={}: {}",
                                            event.regionId(), t.getMessage());
                                }
                            }
                        }
                    };
                    subscriptions.put(regionId, sub);
                    eventBus.subscribe(regionId, sub);
                    log.info("CDC EventFeed: registered region={}", regionId);
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
                long ts = resolvedTsSupplier.getAsLong();
                var event = Cdcpb.ResolvedTsEvent.newBuilder()
                        .addAllRegions(regions)
                        .setTs(ts)
                        .build();
                synchronized (sendLock) {
                    if (!closed) {
                        try {
                            responseObserver.onNext(event);
                        } catch (Throwable t) {
                            log.debug("CDC ResolvedTs send failed: {}", t.getMessage());
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

    public void close() {
        scheduler.shutdownNow();
    }
}
