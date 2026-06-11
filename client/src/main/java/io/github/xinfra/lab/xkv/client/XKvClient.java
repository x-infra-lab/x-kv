package io.github.xinfra.lab.xkv.client;

import io.github.xinfra.lab.xkv.client.config.ClientConfig;
import io.github.xinfra.lab.xkv.client.config.ClientConfigLoader;
import io.github.xinfra.lab.xkv.client.raw.RawKvClient;

import java.io.IOException;

/**
 * Top-level raw KV client (no MVCC, no transactions). Intended for cache
 * workloads, ops tooling, feature flags. Coexists with {@link TxnClient}
 * inside the same JVM.
 */
public interface XKvClient extends AutoCloseable {

    RawKvClient raw();

    @Override void close();

    static XKvClient create(ClientConfig config) {
        return new XKvClientImpl(config);
    }

    static XKvClient create(String[] args) throws IOException {
        return create(ClientConfigLoader.load(args));
    }
}
