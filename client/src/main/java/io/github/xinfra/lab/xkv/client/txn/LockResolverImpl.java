package io.github.xinfra.lab.xkv.client.txn;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import io.github.xinfra.lab.xkv.client.backoff.Backoffer;
import io.github.xinfra.lab.xkv.client.config.ClientConfig;
import io.github.xinfra.lab.xkv.client.region.RegionRequestSenderImpl;
import io.github.xinfra.lab.xkv.client.tso.TsoBatcher;
import io.github.xinfra.lab.xkv.proto.Errorpb;
import io.github.xinfra.lab.xkv.proto.Kvrpcpb;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * Default {@link LockResolver}.
 *
 * <h3>v1 fixes encoded structurally</h3>
 *
 * <ol>
 *   <li><b>Caffeine LRU cache</b> with bounded size + TTL — replaces v1's
 *       unbounded {@code ConcurrentHashMap} that grew until OOM.</li>
 *   <li><b>Per-{@code lock_ts} single-flight</b> via an in-flight
 *       {@code ConcurrentMap}: N concurrent readers that hit the same
 *       lock all await one CheckTxnStatus + ResolveLock pair.</li>
 *   <li><b>Honest CheckTxnStatus → ResolveLock chain</b> — when status
 *       returns NoAction (lock alive, TTL not expired) the resolver does
 *       NOT call ResolveLock. v1 force-rolled-back live txns and lost
 *       money in BankTransfer.</li>
 * </ol>
 */
public final class LockResolverImpl implements LockResolver {
    private static final Logger log = LoggerFactory.getLogger(LockResolverImpl.class);

    private final RegionRequestSenderImpl sender;
    private final TsoBatcher tso;
    private final ClientConfig.TxnConfig cfg;

    /** Cache: lock_ts → terminal verdict (txn either committed at X, or rolled back). */
    private final Cache<Long, Verdict> verdictCache;

    /** In-flight: lock_ts → ongoing resolve future, used for single-flight. */
    private final ConcurrentMap<Long, CompletableFuture<Verdict>> inFlight = new ConcurrentHashMap<>();

    public LockResolverImpl(RegionRequestSenderImpl sender,
                             TsoBatcher tso,
                             ClientConfig.TxnConfig cfg) {
        this.sender = sender;
        this.tso = tso;
        this.cfg = cfg;
        this.verdictCache = Caffeine.newBuilder()
                .maximumSize(cfg.resolverCacheSize())
                .expireAfterWrite(cfg.resolverCacheTtl())
                .build();
    }

    @Override
    public boolean resolveLock(Backoffer bo, long callerStartTs, Kvrpcpb.LockInfo lock) {
        var verdict = checkOrAwaitVerdict(bo, callerStartTs, lock);
        if (verdict == null || verdict.kind() == Verdict.Kind.ALIVE) {
            return false;
        }
        // verdict is COMMITTED(commitTs) or ROLLED_BACK
        long commitTs = verdict.kind() == Verdict.Kind.COMMITTED ? verdict.commitTs() : 0L;
        // Issue ResolveLock to drop the lock on the SEEN key (not necessarily primary).
        var resolveResp = sender.sendKeyed(lock.getKey().toByteArray(), bo,
                (stub, info) -> stub.kvResolveLock(Kvrpcpb.ResolveLockRequest.newBuilder()
                        .setContext(io.github.xinfra.lab.xkv.proto.Kvrpcpb.Context.newBuilder()
                                .setRegionId(info.region().getId())
                                .setRegionEpoch(info.region().getRegionEpoch())
                                .setPeer(info.leader())
                                .build())
                        .setStartVersion(lock.getLockVersion())
                        .setCommitVersion(commitTs)
                        .addKeys(lock.getKey())
                        .build()),
                Kvrpcpb.ResolveLockResponse::getRegionError);
        if (resolveResp.hasError()) {
            log.warn("ResolveLock returned KeyError: {}", resolveResp.getError());
            return false;
        }
        return true;
    }

    @Override
    public boolean resolveLocks(Backoffer bo, long callerStartTs, List<Kvrpcpb.LockInfo> locks) {
        boolean allResolved = true;
        for (var lock : locks) {
            if (!resolveLock(bo, callerStartTs, lock)) allResolved = false;
        }
        return allResolved;
    }

    @Override public void clearCache() { verdictCache.invalidateAll(); inFlight.clear(); }

    // =====================================================================

    private Verdict checkOrAwaitVerdict(Backoffer bo, long callerStartTs, Kvrpcpb.LockInfo lock) {
        long lockTs = lock.getLockVersion();
        var cached = verdictCache.getIfPresent(lockTs);
        if (cached != null) return cached;

        var ours = new CompletableFuture<Verdict>();
        var existing = inFlight.putIfAbsent(lockTs, ours);
        if (existing != null) {
            // Another thread is resolving the same lock_ts; await it.
            try { return existing.get(); }
            catch (Exception e) { return null; }
        }

        Verdict v;
        try {
            v = doCheckTxnStatus(bo, callerStartTs, lock);
            if (v != null && v.kind() != Verdict.Kind.ALIVE) {
                verdictCache.put(lockTs, v);
            }
            ours.complete(v);
        } catch (Throwable t) {
            ours.completeExceptionally(t);
            throw t;
        } finally {
            inFlight.remove(lockTs, ours);
        }
        return v;
    }

    private Verdict doCheckTxnStatus(Backoffer bo, long callerStartTs, Kvrpcpb.LockInfo lock) {
        long currentTs;
        try { currentTs = tso.getTimestamp().get(); }
        catch (Exception e) {
            log.warn("CheckTxnStatus: TSO fetch failed", e);
            return null;
        }
        long lockTs = lock.getLockVersion();
        var resp = sender.sendKeyed(lock.getPrimaryLock().toByteArray(), bo,
                (stub, info) -> stub.kvCheckTxnStatus(Kvrpcpb.CheckTxnStatusRequest.newBuilder()
                        .setContext(Kvrpcpb.Context.newBuilder()
                                .setRegionId(info.region().getId())
                                .setRegionEpoch(info.region().getRegionEpoch())
                                .setPeer(info.leader())
                                .build())
                        .setPrimaryKey(lock.getPrimaryLock())
                        .setLockTs(lockTs)
                        .setCallerStartTs(callerStartTs)
                        .setCurrentTs(currentTs)
                        .setRollbackIfNotExist(true)
                        .build()),
                Kvrpcpb.CheckTxnStatusResponse::getRegionError);
        if (resp.hasError()) {
            log.warn("CheckTxnStatus error: {}", resp.getError());
            return new Verdict(Verdict.Kind.ALIVE, 0);
        }

        var action = resp.getAction();
        long ct = resp.getCommitVersion();
        if (ct > 0) {
            return new Verdict(Verdict.Kind.COMMITTED, ct);
        }
        if (action == Kvrpcpb.Action.TtlExpireRollback
                || action == Kvrpcpb.Action.TtlExpirePessimisticRollback
                || action == Kvrpcpb.Action.LockNotExistRollback) {
            return new Verdict(Verdict.Kind.ROLLED_BACK, 0);
        }
        // NoAction with async-commit lock: escalate to CheckSecondaryLocks
        // to determine the true commit status.
        if (action == Kvrpcpb.Action.NoAction && resp.hasLockInfo()
                && resp.getLockInfo().getUseAsyncCommit()) {
            var lockInfo = resp.getLockInfo();
            long maxMinCommitTs = lockInfo.getMinCommitTs();

            for (var sec : lockInfo.getSecondariesList()) {
                var csResp = sender.sendKeyed(sec.toByteArray(), bo,
                        (stub, info) -> stub.kvCheckSecondaryLocks(
                                Kvrpcpb.CheckSecondaryLocksRequest.newBuilder()
                                        .setContext(Kvrpcpb.Context.newBuilder()
                                                .setRegionId(info.region().getId())
                                                .setRegionEpoch(info.region().getRegionEpoch())
                                                .setPeer(info.leader())
                                                .build())
                                        .setStartVersion(lockTs)
                                        .addKeys(sec)
                                        .build()),
                        Kvrpcpb.CheckSecondaryLocksResponse::getRegionError);

                if (csResp.getCommitTs() > 0) {
                    return new Verdict(Verdict.Kind.COMMITTED, csResp.getCommitTs());
                }
                if (csResp.getLocksCount() == 0) {
                    return new Verdict(Verdict.Kind.ROLLED_BACK, 0);
                }
                for (var l : csResp.getLocksList()) {
                    maxMinCommitTs = Math.max(maxMinCommitTs, l.getMinCommitTs());
                }
            }
            return new Verdict(Verdict.Kind.COMMITTED, maxMinCommitTs);
        }

        // NoAction → live, caller must backoff.
        return new Verdict(Verdict.Kind.ALIVE, 0);
    }

    /** Suppress unused warning. */
    @SuppressWarnings("unused")
    private static Errorpb.Error noopError() { return Errorpb.Error.getDefaultInstance(); }

    record Verdict(Kind kind, long commitTs) {
        enum Kind { COMMITTED, ROLLED_BACK, ALIVE }
    }
}
