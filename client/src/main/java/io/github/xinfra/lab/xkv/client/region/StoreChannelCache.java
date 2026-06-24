package io.github.xinfra.lab.xkv.client.region;

import io.github.xinfra.lab.xkv.client.pd.PdClient;
import io.github.xinfra.lab.xkv.common.auth.AuthClientInterceptor;
import io.github.xinfra.lab.xkv.common.metrics.GrpcClientMetricsInterceptor;
import io.github.xinfra.lab.xkv.common.metrics.XKvMetrics;
import io.github.xinfra.lab.xkv.common.tls.GrpcChannelFactory;
import io.github.xinfra.lab.xkv.common.tls.TlsConfig;
import io.github.xinfra.lab.xkv.proto.Pdpb;
import io.github.xinfra.lab.xkv.proto.TikvGrpc;
import io.grpc.ClientInterceptor;
import io.grpc.ManagedChannel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.ArrayList;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

/**
 * Per-client cache of {@code storeId → (ManagedChannel, TikvBlockingStub)}.
 *
 * <p>Channels are constructed lazily on first use and reused for the
 * lifetime of the cache. {@code closeStore} drops a channel on shutdown
 * or when PD reports a store removed (Phase 7 hook).
 *
 * <p>Lookups go through PD ({@code GetStore}) when the cache misses.
 */
public final class StoreChannelCache implements AutoCloseable {
    private static final Logger log = LoggerFactory.getLogger(StoreChannelCache.class);

    private final PdClient pdClient;
    private final TlsConfig tls;
    private final String authToken;
    private final long grpcTimeoutMs;
    private final int channelsPerStore;
    private final ConcurrentHashMap<Long, Entry> byStoreId = new ConcurrentHashMap<>();

    public StoreChannelCache(PdClient pdClient) {
        this(pdClient, null, null, null, 1);
    }

    public StoreChannelCache(PdClient pdClient, TlsConfig tls, String authToken) {
        this(pdClient, tls, authToken, null, 1);
    }

    public StoreChannelCache(PdClient pdClient, TlsConfig tls, String authToken,
                             Duration grpcTimeout) {
        this(pdClient, tls, authToken, grpcTimeout, 1);
    }

    public StoreChannelCache(PdClient pdClient, TlsConfig tls, String authToken,
                             Duration grpcTimeout, int channelsPerStore) {
        this.pdClient = pdClient;
        this.tls = tls;
        this.authToken = authToken;
        this.grpcTimeoutMs = grpcTimeout != null ? grpcTimeout.toMillis() : 10_000L;
        this.channelsPerStore = Math.max(1, channelsPerStore);
    }

    /** Resolve and dial — returns null if PD doesn't know the store. */
    public TikvGrpc.TikvBlockingStub stubFor(long storeId) {
        var e = byStoreId.get(storeId);
        if (e == null) {
            e = dial(storeId);
            if (e == null) return null;
            var prev = byStoreId.putIfAbsent(storeId, e);
            if (prev != null) {
                close(e);
                e = prev;
            }
        }
        return e.nextStub().withDeadlineAfter(grpcTimeoutMs, TimeUnit.MILLISECONDS);
    }

    /** Return an async stub for the given store (used by BatchCommandsClient). */
    public TikvGrpc.TikvStub asyncStubFor(long storeId) {
        var e = byStoreId.get(storeId);
        if (e == null) {
            e = dial(storeId);
            if (e == null) return null;
            var prev = byStoreId.putIfAbsent(storeId, e);
            if (prev != null) {
                close(e);
                e = prev;
            }
        }
        return e.nextAsyncStub();
    }

    private Entry dial(long storeId) {
        try {
            var resp = pdClient.blockingStub().getStore(Pdpb.GetStoreRequest.newBuilder().setStoreId(storeId).build());
            if (!resp.hasStore() || resp.getStore().getAddress().isEmpty()) {
                log.warn("PD has no address for store {}", storeId);
                return null;
            }
            var interceptors = new ArrayList<ClientInterceptor>();
            interceptors.add(new GrpcClientMetricsInterceptor(XKvMetrics.registryOrNoop()));
            if (authToken != null) {
                interceptors.add(new AuthClientInterceptor(authToken));
            }
            var channels = new ManagedChannel[channelsPerStore];
            var blockingStubs = new TikvGrpc.TikvBlockingStub[channelsPerStore];
            var asyncStubs = new TikvGrpc.TikvStub[channelsPerStore];
            for (int i = 0; i < channelsPerStore; i++) {
                channels[i] = GrpcChannelFactory.build(resp.getStore().getAddress(), tls, interceptors);
                blockingStubs[i] = TikvGrpc.newBlockingStub(channels[i]);
                asyncStubs[i] = TikvGrpc.newStub(channels[i]);
            }
            return new Entry(channels, blockingStubs, asyncStubs);
        } catch (Throwable t) {
            log.warn("dial store {} failed: {}", storeId, t.getMessage());
            return null;
        }
    }

    public void closeStore(long storeId) {
        var e = byStoreId.remove(storeId);
        if (e != null) close(e);
    }

    @Override
    public void close() {
        for (var e : byStoreId.values()) close(e);
        byStoreId.clear();
    }

    private static void close(Entry e) {
        for (var ch : e.channels) {
            try { ch.shutdownNow().awaitTermination(2, TimeUnit.SECONDS); }
            catch (InterruptedException ignored) { Thread.currentThread().interrupt(); }
        }
    }

    private static final class Entry {
        private final ManagedChannel[] channels;
        private final TikvGrpc.TikvBlockingStub[] blockingStubs;
        private final TikvGrpc.TikvStub[] asyncStubs;
        private final java.util.concurrent.atomic.AtomicInteger idx =
                new java.util.concurrent.atomic.AtomicInteger();

        Entry(ManagedChannel[] channels, TikvGrpc.TikvBlockingStub[] blockingStubs,
              TikvGrpc.TikvStub[] asyncStubs) {
            this.channels = channels;
            this.blockingStubs = blockingStubs;
            this.asyncStubs = asyncStubs;
        }

        TikvGrpc.TikvBlockingStub nextStub() {
            return blockingStubs[Math.floorMod(idx.getAndIncrement(), blockingStubs.length)];
        }

        TikvGrpc.TikvStub nextAsyncStub() {
            return asyncStubs[Math.floorMod(idx.getAndIncrement(), asyncStubs.length)];
        }
    }
}
