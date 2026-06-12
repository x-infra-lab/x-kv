package io.github.xinfra.lab.xkv.client.txn;

import com.google.protobuf.ByteString;
import io.github.xinfra.lab.xkv.client.backoff.BackofferImpl;
import io.github.xinfra.lab.xkv.client.config.ClientConfig;
import io.github.xinfra.lab.xkv.client.error.KvClientException;
import io.github.xinfra.lab.xkv.client.region.RegionCache;
import io.github.xinfra.lab.xkv.client.region.RegionRequestSenderImpl;
import io.github.xinfra.lab.xkv.client.tso.TsoBatcher;
import io.github.xinfra.lab.xkv.proto.Kvrpcpb;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;

/**
 * Default {@link Transaction}.
 *
 * <p>Lifecycle states: {@code ACTIVE} → {@code COMMITTED} | {@code ROLLED_BACK}
 * | {@code UNKNOWN}. Operations after a terminal state throw.
 *
 * <h3>Read path</h3>
 *
 * <p>{@link #get} issues KvGet at {@code startTs}; on KeyError(locked)
 * the lock resolver decides verdict and (if not ALIVE) clears it; the
 * read retries within a backoff budget.
 *
 * <h3>Write buffering</h3>
 *
 * <p>{@link #put} / {@link #delete} buffer mutations locally. Same-key
 * later writes overwrite earlier ones (last-write-wins). At {@link #commit}
 * the buffer is flushed via {@link TwoPhaseCommitter}.
 */
public final class TransactionImpl implements Transaction {
    private static final Logger log = LoggerFactory.getLogger(TransactionImpl.class);

    private final long startTs;
    private final RegionRequestSenderImpl sender;
    private final RegionCache regionCache;
    private final TsoBatcher tso;
    private final TwoPhaseCommitter committer;
    private final LockResolver lockResolver;
    private final ClientConfig.TxnConfig cfg;
    private final ClientConfig.BackoffConfig backoffCfg;

    /** Mutation buffer ordered by key (TreeMap → deterministic primary selection). */
    private final TreeMap<byte[], Kvrpcpb.Mutation> buffer =
            new TreeMap<>(java.util.Arrays::compareUnsigned);

    /** Keys that hold pessimistic locks (need isPessimisticLock on prewrite). */
    private final Set<ByteString> pessimisticLockedKeys = new HashSet<>();

    /** Latest for_update_ts used in pessimistic locks. */
    private long forUpdateTs = 0;

    private State state = State.ACTIVE;

    public TransactionImpl(long startTs,
                            RegionRequestSenderImpl sender,
                            RegionCache regionCache,
                            TsoBatcher tso,
                            TwoPhaseCommitter committer,
                            LockResolver lockResolver,
                            ClientConfig.TxnConfig cfg,
                            ClientConfig.BackoffConfig backoffCfg) {
        this.startTs = startTs;
        this.sender = sender;
        this.regionCache = regionCache;
        this.tso = tso;
        this.committer = committer;
        this.lockResolver = lockResolver;
        this.cfg = cfg;
        this.backoffCfg = backoffCfg;
    }

    @Override public long startTs() { return startTs; }

    @Override
    public Optional<byte[]> get(byte[] key) {
        ensureActive();
        // Buffered write wins — read your own writes inside the txn.
        if (buffer.containsKey(key)) {
            var m = buffer.get(key);
            if (m.getOp() == Kvrpcpb.Op.Del) return Optional.empty();
            return Optional.of(m.getValue().toByteArray());
        }

        var bo = new BackofferImpl(backoffCfg);
        while (true) {
            var resp = sender.sendKeyed(key, bo,
                    (stub, info) -> stub.kvGet(Kvrpcpb.GetRequest.newBuilder()
                            .setContext(Kvrpcpb.Context.newBuilder()
                                    .setRegionId(info.region().getId())
                                    .setRegionEpoch(info.region().getRegionEpoch())
                                    .setPeer(info.leader())
                                    .build())
                            .setKey(ByteString.copyFrom(key))
                            .setVersion(startTs)
                            .build()),
                    Kvrpcpb.GetResponse::getRegionError);
            if (resp.hasError()) {
                if (resp.getError().hasLocked()) {
                    var lock = resp.getError().getLocked();
                    boolean resolved = lockResolver.resolveLock(bo, startTs, lock);
                    if (!resolved) {
                        bo.backoff(io.github.xinfra.lab.xkv.client.backoff.Backoffer.Reason.TXN_LOCK,
                                "lock alive");
                    }
                    continue;
                }
                throw mapKeyError(resp.getError());
            }
            if (resp.getNotFound()) return Optional.empty();
            return Optional.of(resp.getValue().toByteArray());
        }
    }

    @Override
    public Map<byte[], byte[]> batchGet(List<byte[]> keys) {
        ensureActive();
        var out = new LinkedHashMap<byte[], byte[]>(keys.size() * 2);
        for (var k : keys) {
            get(k).ifPresent(v -> out.put(k, v));
        }
        return out;
    }

    @Override
    public Iterable<KvPair> scan(byte[] start, byte[] end, int limit) {
        ensureActive();
        var allPairs = new ArrayList<KvPair>();
        byte[] cursor = start;
        while (allPairs.size() < limit) {
            int remaining = limit - allPairs.size();
            byte[] scanStart = cursor;
            var bo = new BackofferImpl(backoffCfg);
            boolean lockRetry;
            do {
                lockRetry = false;
                var resp = sender.sendKeyed(scanStart, bo,
                        (stub, info) -> {
                            byte[] regionEnd = info.region().getEndKey().toByteArray();
                            byte[] scanEnd = end;
                            if (regionEnd.length > 0 && (scanEnd == null || scanEnd.length == 0
                                    || java.util.Arrays.compareUnsigned(regionEnd, scanEnd) < 0)) {
                                scanEnd = regionEnd;
                            }
                            return stub.kvScan(Kvrpcpb.ScanRequest.newBuilder()
                                    .setContext(Kvrpcpb.Context.newBuilder()
                                            .setRegionId(info.region().getId())
                                            .setRegionEpoch(info.region().getRegionEpoch())
                                            .setPeer(info.leader())
                                            .build())
                                    .setStartKey(ByteString.copyFrom(scanStart))
                                    .setEndKey(ByteString.copyFrom(scanEnd == null ? new byte[0] : scanEnd))
                                    .setLimit(remaining)
                                    .setVersion(startTs)
                                    .build());
                        },
                        Kvrpcpb.ScanResponse::getRegionError);
                var locks = new ArrayList<Kvrpcpb.LockInfo>();
                for (var p : resp.getPairsList()) {
                    if (p.hasError() && p.getError().hasLocked()) {
                        locks.add(p.getError().getLocked());
                    }
                }
                if (!locks.isEmpty()) {
                    if (!lockResolver.resolveLocks(bo, startTs, locks)) {
                        bo.backoff(io.github.xinfra.lab.xkv.client.backoff.Backoffer.Reason.TXN_LOCK,
                                "scan-lock alive");
                    }
                    lockRetry = true;
                    continue;
                }
                for (var p : resp.getPairsList()) {
                    if (p.hasError()) continue;
                    allPairs.add(new KvPair(p.getKey().toByteArray(), p.getValue().toByteArray()));
                }
                // Advance to next region if this one is exhausted.
                if (resp.getPairsCount() < remaining) {
                    var info = regionCache.locateKey(scanStart).orElse(null);
                    if (info == null) break;
                    byte[] regionEnd = info.region().getEndKey().toByteArray();
                    if (regionEnd.length == 0) break;
                    if (end != null && end.length > 0
                            && java.util.Arrays.compareUnsigned(regionEnd, end) >= 0) break;
                    cursor = regionEnd;
                }
            } while (lockRetry);
            if (!lockRetry && allPairs.size() < limit) {
                var info = regionCache.locateKey(scanStart).orElse(null);
                if (info == null) break;
                byte[] regionEnd = info.region().getEndKey().toByteArray();
                if (regionEnd.length == 0) break;
                if (end != null && end.length > 0
                        && java.util.Arrays.compareUnsigned(regionEnd, end) >= 0) break;
                cursor = regionEnd;
            } else {
                break;
            }
        }
        return mergeWithBuffer(allPairs, start, end, limit);
    }

    @Override
    public Iterable<KvPair> reverseScan(byte[] start, byte[] end, int limit) {
        ensureActive();
        // start = exclusive upper bound, end = inclusive lower bound
        // (matches TiKV's reverse scan semantics)
        var bo = new BackofferImpl(backoffCfg);
        while (true) {
            var resp = sender.sendKeyed(
                    end != null && end.length > 0 ? end : start, bo,
                    (stub, info) -> stub.kvScan(Kvrpcpb.ScanRequest.newBuilder()
                            .setContext(Kvrpcpb.Context.newBuilder()
                                    .setRegionId(info.region().getId())
                                    .setRegionEpoch(info.region().getRegionEpoch())
                                    .setPeer(info.leader())
                                    .build())
                            .setStartKey(ByteString.copyFrom(start))
                            .setEndKey(ByteString.copyFrom(end == null ? new byte[0] : end))
                            .setLimit(limit)
                            .setVersion(startTs)
                            .setReverse(true)
                            .build()),
                    Kvrpcpb.ScanResponse::getRegionError);
            // Resolve any locks surfaced in the result.
            var locks = new ArrayList<Kvrpcpb.LockInfo>();
            for (var p : resp.getPairsList()) {
                if (p.hasError() && p.getError().hasLocked()) {
                    locks.add(p.getError().getLocked());
                }
            }
            if (!locks.isEmpty()) {
                if (!lockResolver.resolveLocks(bo, startTs, locks)) {
                    bo.backoff(io.github.xinfra.lab.xkv.client.backoff.Backoffer.Reason.TXN_LOCK,
                            "reverse-scan-lock alive");
                }
                continue;
            }
            var result = new ArrayList<KvPair>(resp.getPairsCount());
            for (var p : resp.getPairsList()) {
                if (p.hasError()) continue;
                result.add(new KvPair(p.getKey().toByteArray(), p.getValue().toByteArray()));
            }
            return mergeWithBufferReverse(result, start, end, limit);
        }
    }

    @Override
    public void put(byte[] key, byte[] value) {
        ensureActive();
        buffer.put(key, Kvrpcpb.Mutation.newBuilder()
                .setOp(Kvrpcpb.Op.Put)
                .setKey(ByteString.copyFrom(key))
                .setValue(ByteString.copyFrom(value))
                .build());
    }

    @Override
    public void delete(byte[] key) {
        ensureActive();
        buffer.put(key, Kvrpcpb.Mutation.newBuilder()
                .setOp(Kvrpcpb.Op.Del)
                .setKey(ByteString.copyFrom(key))
                .build());
    }

    @Override
    public void insert(byte[] key, byte[] value) {
        ensureActive();
        buffer.put(key, Kvrpcpb.Mutation.newBuilder()
                .setOp(Kvrpcpb.Op.Insert)
                .setKey(ByteString.copyFrom(key))
                .setValue(ByteString.copyFrom(value))
                .build());
    }

    @Override
    public void lockKeysForUpdate(List<byte[]> keys) {
        ensureActive();
        if (keys.isEmpty()) return;

        // Acquire a fresh for_update_ts from TSO. TiKV does this per
        // lockKeysForUpdate call so readers at a TS between startTs and
        // forUpdateTs see a consistent snapshot.
        long futTs;
        try { futTs = tso.getTimestamp().get(); }
        catch (Exception e) { throw new KvClientException(KvClientException.Category.OTHER, "TSO failed", e); }
        this.forUpdateTs = futTs;

        // Determine primary: use existing buffer's first key if available,
        // otherwise first key in the lock request.
        byte[] primary;
        if (!buffer.isEmpty()) {
            primary = buffer.firstKey();
        } else {
            var sorted = new ArrayList<>(keys);
            sorted.sort(Arrays::compareUnsigned);
            primary = sorted.get(0);
        }

        // Group keys by region.
        var byRegionId = new java.util.HashMap<Long, List<byte[]>>();
        for (var k : keys) {
            var info = regionCache.locateKey(k).orElse(null);
            long rid = info == null ? 0L : info.region().getId();
            byRegionId.computeIfAbsent(rid, x -> new ArrayList<>()).add(k);
        }

        for (var group : byRegionId.values()) {
            acquirePessimisticLocksForGroup(group, primary, futTs);
        }
    }

    private void acquirePessimisticLocksForGroup(List<byte[]> keys, byte[] primary, long futTs) {
        var bo = new BackofferImpl(backoffCfg);
        while (true) {
            byte[] routeKey = keys.get(0);
            var resp = sender.sendKeyed(routeKey, bo,
                    (stub, info) -> {
                        var b = Kvrpcpb.PessimisticLockRequest.newBuilder()
                                .setContext(Kvrpcpb.Context.newBuilder()
                                        .setRegionId(info.region().getId())
                                        .setRegionEpoch(info.region().getRegionEpoch())
                                        .setPeer(info.leader())
                                        .build())
                                .setStartVersion(startTs)
                                .setForUpdateTs(futTs)
                                .setLockTtl(cfg.defaultLockTtl().toMillis())
                                .setPrimaryLock(ByteString.copyFrom(primary))
                                .setIsFirstLock(pessimisticLockedKeys.isEmpty())
                                .setWaitTimeout(cfg.pessimisticWaitTimeoutMs());
                        for (var k : keys) {
                            b.addMutations(Kvrpcpb.Mutation.newBuilder()
                                    .setOp(Kvrpcpb.Op.PessimisticLock)
                                    .setKey(ByteString.copyFrom(k))
                                    .build());
                        }
                        return stub.kvPessimisticLock(b.build());
                    },
                    Kvrpcpb.PessimisticLockResponse::getRegionError);

            if (resp.getErrorsCount() == 0) {
                for (var k : keys) pessimisticLockedKeys.add(ByteString.copyFrom(k));
                return;
            }

            // Check for deadlock — non-retryable.
            for (var err : resp.getErrorsList()) {
                if (err.hasDeadlock()) {
                    throw new KvClientException(KvClientException.Category.DEADLOCK, err.getDeadlock().toString());
                }
            }
            // Check for write conflict — non-retryable for pessimistic lock.
            for (var err : resp.getErrorsList()) {
                if (err.hasConflict()) {
                    throw new KvClientException(
                            KvClientException.Category.WRITE_CONFLICT,
                            err.getConflict().toString());
                }
            }
            // KeyError.locked — the key is held by another txn. Backoff and retry.
            boolean hasLock = false;
            for (var err : resp.getErrorsList()) {
                if (err.hasLocked()) {
                    hasLock = true;
                    lockResolver.resolveLock(bo, startTs, err.getLocked());
                }
            }
            if (hasLock) {
                bo.backoff(io.github.xinfra.lab.xkv.client.backoff.Backoffer.Reason.TXN_LOCK,
                        "pessimistic lock blocked");
                continue;
            }
            // Unknown error — surface it.
            throw new KvClientException(KvClientException.Category.OTHER,
                    "pessimistic lock failed: " + resp.getErrorsList());
        }
    }

    @Override
    public long commit() {
        ensureActive();
        if (buffer.isEmpty()) {
            state = State.COMMITTED;
            return startTs;     // empty txn — no-op
        }
        var mutations = new ArrayList<>(buffer.values());
        var ctx = new TwoPhaseCommitter.TxnContext(
                startTs, cfg.defaultLockTtl().toMillis(),
                /* primary= */ null,         // committer picks first
                cfg.enableOnePc() && mutations.size() == 1 && pessimisticLockedKeys.isEmpty(),
                cfg.enableAsyncCommit() && pessimisticLockedKeys.isEmpty(),
                /* maxCommitTs= */ 0,
                forUpdateTs,
                mutations.size(),
                Set.copyOf(pessimisticLockedKeys));
        try {
            var result = committer.commit(ctx, mutations).get();
            switch (result.state()) {
                case COMMITTED:
                    state = State.COMMITTED;
                    return result.commitTs();
                case ROLLED_BACK:
                    state = State.ROLLED_BACK;
                    throw new KvClientException(KvClientException.Category.WRITE_CONFLICT,
                            "txn rolled back: " + result.message());
                case UNKNOWN:
                default:
                    state = State.UNKNOWN;
                    throw new KvClientException(KvClientException.Category.UNKNOWN_COMMIT_STATE,
                            "commit unknown: " + result.message());
            }
        } catch (KvClientException e) {
            throw e;
        } catch (Exception e) {
            state = State.UNKNOWN;
            throw new KvClientException(KvClientException.Category.UNKNOWN_COMMIT_STATE, e.getMessage(), e);
        }
    }

    @Override
    public void rollback() {
        if (state != State.ACTIVE) return;
        if (buffer.isEmpty() && pessimisticLockedKeys.isEmpty()) {
            state = State.ROLLED_BACK;
            return;
        }

        // Pessimistic-only keys (locked but never prewritten) use
        // kvPessimisticRollback — no ROLLBACK record in WRITE CF.
        var pessOnlyKeys = new ArrayList<ByteString>();
        for (var pk : pessimisticLockedKeys) {
            if (!buffer.containsKey(pk.toByteArray())) {
                pessOnlyKeys.add(pk);
            }
        }
        if (!pessOnlyKeys.isEmpty()) {
            var grouped = groupByRegion(pessOnlyKeys);
            for (var entry : grouped.entrySet()) {
                var regionKeys = entry.getValue();
                try {
                    var bo = new BackofferImpl(backoffCfg);
                    sender.sendKeyed(regionKeys.get(0).toByteArray(), bo,
                            (stub, info) -> {
                                var b = Kvrpcpb.PessimisticRollbackRequest.newBuilder()
                                        .setContext(Kvrpcpb.Context.newBuilder()
                                                .setRegionId(info.region().getId())
                                                .setRegionEpoch(info.region().getRegionEpoch())
                                                .setPeer(info.leader())
                                                .build())
                                        .setStartVersion(startTs)
                                        .setForUpdateTs(forUpdateTs);
                                for (var k : regionKeys) b.addKeys(k);
                                return stub.kvPessimisticRollback(b.build());
                            },
                            Kvrpcpb.PessimisticRollbackResponse::getRegionError);
                } catch (Throwable t) {
                    log.debug("pessimistic rollback error (TTL cleanup will handle): {}", t.getMessage());
                }
            }
        }

        // Prewritten keys (in buffer) use kvBatchRollback.
        if (!buffer.isEmpty()) {
            var keyBs = new ArrayList<ByteString>();
            for (var k : buffer.keySet()) keyBs.add(ByteString.copyFrom(k));
            var grouped = groupByRegion(keyBs);
            for (var entry : grouped.entrySet()) {
                var regionKeys = entry.getValue();
                try {
                    var bo = new BackofferImpl(backoffCfg);
                    sender.sendKeyed(regionKeys.get(0).toByteArray(), bo,
                            (stub, info) -> {
                                var b = Kvrpcpb.BatchRollbackRequest.newBuilder()
                                        .setContext(Kvrpcpb.Context.newBuilder()
                                                .setRegionId(info.region().getId())
                                                .setRegionEpoch(info.region().getRegionEpoch())
                                                .setPeer(info.leader())
                                                .build())
                                        .setStartVersion(startTs);
                                for (var k : regionKeys) b.addKeys(k);
                                return stub.kvBatchRollback(b.build());
                            },
                            Kvrpcpb.BatchRollbackResponse::getRegionError);
                } catch (Throwable t) {
                    log.debug("rollback RPC error (resolver will clean up): {}", t.getMessage());
                }
            }
        }
        state = State.ROLLED_BACK;
    }

    @Override
    public void close() {
        if (state == State.ACTIVE) rollback();
    }

    // =====================================================================

    private void ensureActive() {
        if (state != State.ACTIVE) {
            throw new IllegalStateException("txn already terminal: " + state);
        }
    }

    private List<KvPair> mergeWithBuffer(List<KvPair> serverPairs, byte[] start, byte[] end, int limit) {
        if (buffer.isEmpty()) return serverPairs;
        var merged = new TreeMap<byte[], byte[]>(java.util.Arrays::compareUnsigned);
        for (var p : serverPairs) merged.put(p.key(), p.value());
        var subBufferStart = start;
        var subBufferEnd = end == null ? null : end;
        for (var e : buffer.entrySet()) {
            var k = e.getKey();
            if (java.util.Arrays.compareUnsigned(k, subBufferStart) < 0) continue;
            if (subBufferEnd != null && java.util.Arrays.compareUnsigned(k, subBufferEnd) >= 0) continue;
            var m = e.getValue();
            if (m.getOp() == Kvrpcpb.Op.Del) merged.remove(k);
            else merged.put(k, m.getValue().toByteArray());
        }
        var out = new ArrayList<KvPair>(merged.size());
        int n = 0;
        for (var e : merged.entrySet()) {
            if (n++ >= limit) break;
            out.add(new KvPair(e.getKey(), e.getValue()));
        }
        return out;
    }

    private List<KvPair> mergeWithBufferReverse(List<KvPair> serverPairs, byte[] start, byte[] end, int limit) {
        if (buffer.isEmpty()) return serverPairs;
        // start = exclusive upper bound, end = inclusive lower bound
        var merged = new TreeMap<byte[], byte[]>(java.util.Arrays::compareUnsigned);
        for (var p : serverPairs) merged.put(p.key(), p.value());
        for (var e : buffer.entrySet()) {
            var k = e.getKey();
            if (java.util.Arrays.compareUnsigned(k, start) >= 0) continue;
            if (end != null && end.length > 0 && java.util.Arrays.compareUnsigned(k, end) < 0) continue;
            var m = e.getValue();
            if (m.getOp() == Kvrpcpb.Op.Del) merged.remove(k);
            else merged.put(k, m.getValue().toByteArray());
        }
        var out = new ArrayList<KvPair>(merged.size());
        int n = 0;
        for (var e : merged.descendingMap().entrySet()) {
            if (n++ >= limit) break;
            out.add(new KvPair(e.getKey(), e.getValue()));
        }
        return out;
    }

    private static KvClientException mapKeyError(Kvrpcpb.KeyError ke) {
        var msg = ke.toString();
        if (ke.hasConflict()) return new KvClientException(
                KvClientException.Category.WRITE_CONFLICT, msg);
        if (ke.hasDeadlock()) return new KvClientException(
                KvClientException.Category.DEADLOCK, msg);
        if (ke.hasAlreadyExist()) return new KvClientException(
                KvClientException.Category.ALREADY_EXIST, msg);
        if (ke.hasTxnNotFound()) return new KvClientException(
                KvClientException.Category.TXN_NOT_FOUND, msg);
        if (ke.hasCommitTsExpired()) return new KvClientException(
                KvClientException.Category.COMMIT_TS_EXPIRED, msg);
        if (ke.hasCommitTsTooLarge()) return new KvClientException(
                KvClientException.Category.COMMIT_TS_TOO_LARGE, msg);
        if (ke.hasAssertionFailed()) return new KvClientException(
                KvClientException.Category.ASSERTION_FAILED, msg);
        return new KvClientException(KvClientException.Category.OTHER, msg);
    }

    private Map<Long, List<ByteString>> groupByRegion(List<ByteString> keys) {
        var groups = new LinkedHashMap<Long, List<ByteString>>();
        for (var k : keys) {
            var info = regionCache.locateKey(k.toByteArray()).orElse(null);
            long regionId = info != null ? info.region().getId() : 0L;
            groups.computeIfAbsent(regionId, id -> new ArrayList<>()).add(k);
        }
        return groups;
    }

    private enum State { ACTIVE, COMMITTED, ROLLED_BACK, UNKNOWN }

    /** Suppress unused. */
    @SuppressWarnings("unused")
    private static List<Object> emptyList() { return Collections.emptyList(); }
}
