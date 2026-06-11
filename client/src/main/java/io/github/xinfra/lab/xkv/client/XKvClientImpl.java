package io.github.xinfra.lab.xkv.client;

import io.github.xinfra.lab.xkv.client.config.ClientConfig;
import io.github.xinfra.lab.xkv.client.pd.PdClient;
import io.github.xinfra.lab.xkv.client.raw.RawKvClient;
import io.github.xinfra.lab.xkv.client.raw.RawKvClientImpl;
import io.github.xinfra.lab.xkv.client.region.RegionCacheImpl;
import io.github.xinfra.lab.xkv.client.region.RegionRequestSenderImpl;
import io.github.xinfra.lab.xkv.client.region.StoreChannelCache;

/**
 * Default {@link XKvClient} — wires {@link RegionCacheImpl} +
 * {@link StoreChannelCache} + {@link RegionRequestSenderImpl} +
 * {@link RawKvClientImpl} on top of a {@link PdClient} that handles
 * PD leader discovery and failover.
 *
 * <p>Construct via {@link XKvClient#create(ClientConfig)} which calls into
 * this class.
 */
public final class XKvClientImpl implements XKvClient {

    private final PdClient pdClient;
    private final StoreChannelCache storeCache;
    private final RegionCacheImpl regionCache;
    private final RegionRequestSenderImpl sender;
    private final RawKvClientImpl rawKv;
    private final ClientConfig config;

    public XKvClientImpl(ClientConfig config) {
        this.config = config;
        var tls = config.conn() != null ? config.conn().tls() : null;
        this.pdClient = new PdClient(config.pdEndpoints(), tls, config.authToken());
        this.storeCache = new StoreChannelCache(pdClient, tls, config.authToken());
        this.regionCache = new RegionCacheImpl(pdClient, config.regionCache());
        this.sender = new RegionRequestSenderImpl(regionCache, storeCache);
        this.rawKv = new RawKvClientImpl(sender, regionCache, config.backoff());
    }

    @Override public RawKvClient raw() { return rawKv; }

    public PdClient pdClient() { return pdClient; }
    public RegionCacheImpl regionCache() { return regionCache; }
    public StoreChannelCache storeCache() { return storeCache; }
    public ClientConfig config() { return config; }

    @Override
    public void close() {
        rawKv.close();
        regionCache.clear();
        storeCache.close();
        pdClient.close();
    }
}
