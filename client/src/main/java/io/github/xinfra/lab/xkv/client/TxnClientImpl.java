package io.github.xinfra.lab.xkv.client;

import io.github.xinfra.lab.xkv.client.config.ClientConfig;
import io.github.xinfra.lab.xkv.client.config.ClientConfig.RetryConfig;
import io.github.xinfra.lab.xkv.client.cop.CopClient;
import io.github.xinfra.lab.xkv.client.cop.CopClientImpl;
import io.github.xinfra.lab.xkv.client.error.KvClientException;
import io.github.xinfra.lab.xkv.client.pd.PdClient;
import io.github.xinfra.lab.xkv.client.region.BatchCommandsClient;
import io.github.xinfra.lab.xkv.client.region.RegionCacheImpl;
import io.github.xinfra.lab.xkv.client.region.RegionRequestSenderImpl;
import io.github.xinfra.lab.xkv.client.region.StoreChannelCache;
import io.github.xinfra.lab.xkv.client.tso.TsoBatcherImpl;
import io.github.xinfra.lab.xkv.client.txn.LockResolverImpl;
import io.github.xinfra.lab.xkv.client.txn.Transaction;
import io.github.xinfra.lab.xkv.client.txn.TransactionImpl;
import io.github.xinfra.lab.xkv.client.txn.TwoPhaseCommitterImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ThreadLocalRandom;

/**
 * Default {@link TxnClient}. Wires the full transactional client stack:
 * {@code PdClient → TsoBatcher → LockResolver → TwoPhaseCommitter → Transaction}.
 *
 * <p>Uses {@link PdClient} for PD leader discovery and failover.
 *
 * <p>Construct via {@link TxnClient#create(ClientConfig)}.
 */
public final class TxnClientImpl implements TxnClient {
    private static final Logger log = LoggerFactory.getLogger(TxnClientImpl.class);

    private final ClientConfig config;
    private final PdClient pdClient;
    private final StoreChannelCache storeCache;
    private final RegionCacheImpl regionCache;
    private final RegionRequestSenderImpl sender;
    private final TsoBatcherImpl tso;
    private final LockResolverImpl lockResolver;
    private final TwoPhaseCommitterImpl committer;
    private final CopClientImpl copClient;
    private final BatchCommandsClient batchClient;

    public TxnClientImpl(ClientConfig config) {
        this.config = config;
        var tls = config.conn() != null ? config.conn().tls() : null;
        this.pdClient = new PdClient(config.pdEndpoints(), tls, config.authToken());
        int poolSize = config.conn() != null ? config.conn().maxStoreConnections() : 1;
        this.storeCache = new StoreChannelCache(pdClient, tls, config.authToken(),
                config.grpcTimeout(), poolSize);
        this.batchClient = new BatchCommandsClient(storeCache);
        this.regionCache = new RegionCacheImpl(pdClient, config.regionCache());
        this.sender = new RegionRequestSenderImpl(regionCache, storeCache, batchClient);
        this.tso = new TsoBatcherImpl(pdClient, config.tso());
        this.lockResolver = new LockResolverImpl(sender, regionCache, tso, config.txn());
        this.committer = new TwoPhaseCommitterImpl(sender, regionCache, tso, config.backoff());
        this.copClient = new CopClientImpl(regionCache, storeCache,
                Runtime.getRuntime().availableProcessors() * 2, config.backoff());
    }

    @Override
    public Transaction begin() {
        long startTs;
        try { startTs = tso.getTimestamp().get(); }
        catch (Exception e) { throw new RuntimeException("TSO fetch failed at begin", e); }
        return new TransactionImpl(startTs, sender, regionCache, tso, committer, lockResolver,
                config.txn(), config.backoff());
    }

    @Override
    public Transaction beginPessimistic() {
        return begin();
    }

    @Override
    public <T> T executeWithRetry(TxnAction<T> action) {
        return executeWithRetry(action, RetryConfig.defaults());
    }

    @Override
    public <T> T executeWithRetry(TxnAction<T> action, RetryConfig retryConfig) {
        KvClientException lastEx = null;
        for (int attempt = 0; attempt <= retryConfig.maxRetries(); attempt++) {
            try (var txn = begin()) {
                T result = action.execute(txn);
                txn.commit();
                return result;
            } catch (KvClientException e) {
                if (isRetryable(e.category())) {
                    lastEx = e;
                    if (attempt < retryConfig.maxRetries()) {
                        backoff(attempt, retryConfig);
                        continue;
                    }
                }
                throw e;
            } catch (Exception e) {
                throw new KvClientException(KvClientException.Category.OTHER,
                        "txn action failed: " + e.getMessage(), e);
            }
        }
        throw lastEx;
    }

    private static boolean isRetryable(KvClientException.Category cat) {
        return cat == KvClientException.Category.WRITE_CONFLICT
                || cat == KvClientException.Category.KEY_LOCKED;
    }

    private static void backoff(int attempt, RetryConfig cfg) {
        long ceiling = Math.min(cfg.backoffCapMs(), cfg.backoffBaseMs() * (1L << attempt));
        long delay = ThreadLocalRandom.current().nextLong(1, Math.max(2, ceiling + 1));
        try { Thread.sleep(delay); }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new KvClientException(KvClientException.Category.OTHER, "interrupted during backoff", e);
        }
    }

    @Override
    public CopClient copClient() { return copClient; }

    public RegionCacheImpl regionCache() { return regionCache; }
    public StoreChannelCache storeCache() { return storeCache; }
    public TsoBatcherImpl tso() { return tso; }
    public PdClient pdClient() { return pdClient; }

    @Override
    public void close() {
        try { copClient.close(); } catch (Throwable e) {
            log.warn("copClient close failed: {}", e.getMessage(), e);
        }
        try { batchClient.close(); } catch (Throwable e) {
            log.warn("batchClient close failed: {}", e.getMessage(), e);
        }
        try { tso.close(); } catch (Throwable e) {
            log.warn("tso close failed: {}", e.getMessage(), e);
        }
        regionCache.clear();
        storeCache.close();
        pdClient.close();
    }
}
