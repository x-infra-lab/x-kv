package io.github.xinfra.lab.xkv.client;

import io.github.xinfra.lab.xkv.client.config.ClientConfig;
import io.github.xinfra.lab.xkv.client.config.ClientConfig.RetryConfig;
import io.github.xinfra.lab.xkv.client.config.ClientConfigLoader;
import io.github.xinfra.lab.xkv.client.cop.CopClient;
import io.github.xinfra.lab.xkv.client.txn.Transaction;

import java.io.IOException;

/**
 * Top-level transactional client.
 *
 * <p>One {@code TxnClient} per JVM is intended; it owns the PD channel,
 * the TSO batcher's bidi stream, the region cache, and the per-store gRPC
 * channels. Callers spawn {@link Transaction}s off it via {@link #begin}.
 */
public interface TxnClient extends AutoCloseable {

    @FunctionalInterface
    interface TxnAction<T> {
        T execute(Transaction txn) throws Exception;
    }

    /** Start a new optimistic transaction. */
    Transaction begin();

    /** Start a new pessimistic transaction. */
    Transaction beginPessimistic();

    <T> T executeWithRetry(TxnAction<T> action);

    <T> T executeWithRetry(TxnAction<T> action, RetryConfig retryConfig);

    CopClient copClient();

    /** Block until in-flight async commits finish, then close all channels. */
    @Override void close();

    static TxnClient create(ClientConfig config) {
        return new TxnClientImpl(config);
    }

    static TxnClient create(String[] args) throws IOException {
        return create(ClientConfigLoader.load(args));
    }
}
