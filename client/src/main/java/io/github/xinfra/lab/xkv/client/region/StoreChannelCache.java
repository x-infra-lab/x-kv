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

import java.util.ArrayList;
import java.util.List;
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
    private final ConcurrentHashMap<Long, Entry> byStoreId = new ConcurrentHashMap<>();

    public StoreChannelCache(PdClient pdClient) {
        this(pdClient, null, null);
    }

    public StoreChannelCache(PdClient pdClient, TlsConfig tls, String authToken) {
        this.pdClient = pdClient;
        this.tls = tls;
        this.authToken = authToken;
    }

    /** Resolve and dial — returns null if PD doesn't know the store. */
    public TikvGrpc.TikvBlockingStub stubFor(long storeId) {
        var e = byStoreId.computeIfAbsent(storeId, this::dial);
        return e == null ? null : e.stub;
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
            var ch = GrpcChannelFactory.build(resp.getStore().getAddress(), tls, interceptors);
            return new Entry(ch, TikvGrpc.newBlockingStub(ch));
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
        try { e.ch.shutdownNow().awaitTermination(2, TimeUnit.SECONDS); }
        catch (InterruptedException ignored) { Thread.currentThread().interrupt(); }
    }

    private record Entry(ManagedChannel ch, TikvGrpc.TikvBlockingStub stub) {}
}
