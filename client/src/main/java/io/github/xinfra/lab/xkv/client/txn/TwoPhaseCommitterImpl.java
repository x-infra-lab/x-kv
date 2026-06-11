package io.github.xinfra.lab.xkv.client.txn;

import com.google.protobuf.ByteString;
import io.github.xinfra.lab.xkv.client.backoff.Backoffer;
import io.github.xinfra.lab.xkv.client.backoff.BackofferImpl;
import io.github.xinfra.lab.xkv.client.config.ClientConfig;
import io.github.xinfra.lab.xkv.client.region.RegionCache;
import io.github.xinfra.lab.xkv.client.region.RegionRequestSenderImpl;
import io.github.xinfra.lab.xkv.client.tso.TsoBatcher;
import io.github.xinfra.lab.xkv.proto.Kvrpcpb;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.CompletableFuture;

/**
 * Default {@link TwoPhaseCommitter}.
 *
 * <h3>v2 commit-state contract (the v1 fix)</h3>
 *
 * <p>Three terminal states:
 * <ul>
 *   <li>{@link CommitState#COMMITTED} — primary's KvCommit was acknowledged
 *       successfully.</li>
 *   <li>{@link CommitState#ROLLED_BACK} — prewrite was rejected (write
 *       conflict / key locked) before any commit attempted.</li>
 *   <li>{@link CommitState#UNKNOWN} — primary's KvCommit RPC failed in a
 *       way that does NOT distinguish "wasn't applied" from "applied but
 *       response lost". Caller MUST surface this state to the user; a
 *       background resolver eventually decides.</li>
 * </ul>
 *
 * <p>v1 collapsed UNKNOWN into ROLLED_BACK and lost transactions — the
 * single most data-corrupting bug. v2 surfaces it explicitly.
 *
 * <h3>Multi-region 2PC</h3>
 *
 * <p>Prewrite and commit are issued per-region, grouped by
 * {@code RegionCache.locateKey}. Primary region is prewritten first,
 * then secondaries. Commit follows the same order.
 */
public final class TwoPhaseCommitterImpl implements TwoPhaseCommitter {
    private static final Logger log = LoggerFactory.getLogger(TwoPhaseCommitterImpl.class);

    private final RegionRequestSenderImpl sender;
    private final RegionCache regionCache;
    private final TsoBatcher tso;
    private final ClientConfig.BackoffConfig backoffCfg;

    public TwoPhaseCommitterImpl(RegionRequestSenderImpl sender,
                                  RegionCache regionCache,
                                  TsoBatcher tso,
                                  ClientConfig.BackoffConfig backoffCfg) {
        this.sender = sender;
        this.regionCache = regionCache;
        this.tso = tso;
        this.backoffCfg = backoffCfg;
    }

    @Override
    public CompletableFuture<CommitResult> commit(TxnContext ctx, List<Kvrpcpb.Mutation> mutations) {
        if (mutations.isEmpty()) {
            return CompletableFuture.completedFuture(CommitResult.committed(ctx.startTs()));
        }
        return CompletableFuture.supplyAsync(() -> commitSync(ctx, mutations));
    }

    private CommitResult commitSync(TxnContext ctx, List<Kvrpcpb.Mutation> mutations) {
        // Pick primary: lex-smallest key (matches MvccTxn's idempotent ordering).
        var sorted = new ArrayList<>(mutations);
        sorted.sort((a, b) -> com.google.protobuf.ByteString.unsignedLexicographicalComparator()
                .compare(a.getKey(), b.getKey()));
        byte[] primary = ctx.primaryKey() != null
                ? ctx.primaryKey()
                : sorted.get(0).getKey().toByteArray();

        // === Phase 1: Prewrite (per-region batches) ===
        var mutsByRegion = groupMutationsByRegion(sorted);

        // Prewrite primary-region batch first (contains the primary key).
        // If primary prewrite fails, the whole txn is rejected.
        long onePcTs = 0;
        long primaryMinCommitTs = 0;
        for (var entry : mutsByRegion.entrySet()) {
            boolean isPrimary = entry.getValue().stream()
                    .anyMatch(m -> Arrays.equals(m.getKey().toByteArray(), primary));
            if (!isPrimary) continue;

            var resp = doPrewriteGroup(ctx, primary, entry.getValue());
            if (resp.getErrorsCount() > 0) {
                return CommitResult.rolledBack("prewrite errors: " + resp.getErrorsList());
            }
            if (resp.getOnePcCommitTs() > 0) {
                onePcTs = resp.getOnePcCommitTs();
            }
            primaryMinCommitTs = resp.getMinCommitTs();
            break;
        }

        // Prewrite secondary-region batches.
        for (var entry : mutsByRegion.entrySet()) {
            boolean isPrimary = entry.getValue().stream()
                    .anyMatch(m -> Arrays.equals(m.getKey().toByteArray(), primary));
            if (isPrimary) continue;

            var resp = doPrewriteGroup(ctx, primary, entry.getValue());
            if (resp.getErrorsCount() > 0) {
                return CommitResult.rolledBack("prewrite errors: " + resp.getErrorsList());
            }
        }

        // 1PC short-circuit
        if (onePcTs > 0) {
            return CommitResult.committed(onePcTs);
        }

        // === Phase 2: Commit ===
        long commitTs;
        try { commitTs = tso.getTimestamp().get(); }
        catch (Exception e) {
            return CommitResult.unknown("commitTs fetch failed: " + e.getMessage());
        }

        if (ctx.useAsyncCommit() && primaryMinCommitTs > 0) {
            commitTs = Math.max(commitTs, primaryMinCommitTs);
        }

        // Group keys by region for commit phase.
        var allKeys = new ArrayList<ByteString>(sorted.size());
        for (var m : sorted) allKeys.add(m.getKey());
        var keysByRegion = groupKeysByRegion(allKeys);

        // Commit primary first. If primary commit fails with a known
        // permanent error (rolled back), we can return ROLLED_BACK. If it
        // fails with anything else (network / unknown), return UNKNOWN.
        boolean primaryCommitted = false;
        CommitResult primaryFailure = null;
        for (var entry : keysByRegion.entrySet()) {
            // Primary is in one of these groups; do it FIRST.
            boolean isPrimaryGroup = entry.getValue().stream()
                    .anyMatch(k -> java.util.Arrays.equals(k.toByteArray(), primary));
            if (!isPrimaryGroup) continue;

            var commitBo = new BackofferImpl(backoffCfg);
            try {
                var commitResp = doCommitGroup(commitBo, ctx.startTs(), commitTs, entry.getValue());
                if (commitResp.hasError()) {
                    var ke = commitResp.getError();
                    if (ke.getAbort().contains("rolled back")
                            || ke.hasCommitTsExpired()
                            || ke.hasTxnNotFound()) {
                        return CommitResult.rolledBack(ke.toString());
                    }
                    primaryFailure = CommitResult.unknown("primary commit error: " + ke);
                } else {
                    primaryCommitted = true;
                }
            } catch (Throwable t) {
                primaryFailure = CommitResult.unknown("primary commit RPC: " + t.getMessage());
            }
            break;
        }
        if (!primaryCommitted) {
            return primaryFailure != null ? primaryFailure : CommitResult.unknown("primary not committed");
        }

        // Async secondaries (Phase 5 simplification: still synchronous serial).
        // The contract: secondary failure does NOT change the txn outcome —
        // primary commit decided. Secondaries are best-effort cleanup that
        // a background resolver can finish later.
        for (var entry : keysByRegion.entrySet()) {
            boolean isPrimaryGroup = entry.getValue().stream()
                    .anyMatch(k -> java.util.Arrays.equals(k.toByteArray(), primary));
            if (isPrimaryGroup) continue;
            try {
                var bo = new BackofferImpl(backoffCfg);
                doCommitGroup(bo, ctx.startTs(), commitTs, entry.getValue());
            } catch (Throwable t) {
                log.debug("secondary commit failed (lock resolver will clean up): {}", t.getMessage());
            }
        }

        return CommitResult.committed(commitTs);
    }

    private Kvrpcpb.CommitResponse doCommitGroup(Backoffer bo, long startTs, long commitTs, List<ByteString> keys) {
        byte[] sample = keys.get(0).toByteArray();
        return sender.sendKeyed(sample, bo,
                (stub, info) -> {
                    var b = Kvrpcpb.CommitRequest.newBuilder()
                            .setContext(Kvrpcpb.Context.newBuilder()
                                    .setRegionId(info.region().getId())
                                    .setRegionEpoch(info.region().getRegionEpoch())
                                    .setPeer(info.leader())
                                    .build())
                            .setStartVersion(startTs)
                            .setCommitVersion(commitTs);
                    for (var k : keys) b.addKeys(k);
                    return stub.kvCommit(b.build());
                },
                Kvrpcpb.CommitResponse::getRegionError);
    }

    private Kvrpcpb.PrewriteResponse doPrewriteGroup(TxnContext ctx, byte[] primary,
                                                      List<Kvrpcpb.Mutation> muts) {
        byte[] routeKey = muts.get(0).getKey().toByteArray();
        var bo = new BackofferImpl(backoffCfg);
        return sender.sendKeyed(routeKey, bo,
                (stub, info) -> {
                    var b = Kvrpcpb.PrewriteRequest.newBuilder()
                            .setContext(Kvrpcpb.Context.newBuilder()
                                    .setRegionId(info.region().getId())
                                    .setRegionEpoch(info.region().getRegionEpoch())
                                    .setPeer(info.leader())
                                    .build())
                            .setStartVersion(ctx.startTs())
                            .setLockTtl(ctx.lockTtlMs())
                            .setPrimaryLock(ByteString.copyFrom(primary))
                            .setTxnSize(ctx.txnSize())
                            .setForUpdateTs(ctx.forUpdateTs())
                            .setUseAsyncCommit(ctx.useAsyncCommit())
                            .setTryOnePc(ctx.tryOnePc())
                            .setMaxCommitTs(ctx.maxCommitTs());
                    for (var m : muts) b.addMutations(m);
                    // Set is_pessimistic_lock parallel array: non-zero forUpdateTs
                    // for mutations that hold a pessimistic lock.
                    var pessKeys = ctx.pessimisticLockedKeys();
                    if (pessKeys != null && !pessKeys.isEmpty()) {
                        for (var m : muts) {
                            b.addIsPessimisticLock(
                                    pessKeys.contains(m.getKey()) ? ctx.forUpdateTs() : 0L);
                        }
                    }
                    return stub.kvPrewrite(b.build());
                },
                Kvrpcpb.PrewriteResponse::getRegionError);
    }

    private Map<byte[], List<Kvrpcpb.Mutation>> groupMutationsByRegion(List<Kvrpcpb.Mutation> sorted) {
        var byRegionId = new HashMap<Long, List<Kvrpcpb.Mutation>>();
        for (var m : sorted) {
            byte[] k = m.getKey().toByteArray();
            var info = regionCache.locateKey(k).orElse(null);
            long rid = info == null ? 0L : info.region().getId();
            byRegionId.computeIfAbsent(rid, x -> new ArrayList<>()).add(m);
        }
        var ordered = new TreeMap<byte[], List<Kvrpcpb.Mutation>>(Arrays::compareUnsigned);
        for (var e : byRegionId.values()) {
            byte[] startKey = e.get(0).getKey().toByteArray();
            ordered.put(startKey, e);
        }
        return ordered;
    }

    private Map<byte[], List<ByteString>> groupKeysByRegion(List<ByteString> keys) {
        var byRegionId = new HashMap<Long, List<ByteString>>();
        for (var k : keys) {
            byte[] kb = k.toByteArray();
            var info = regionCache.locateKey(kb).orElse(null);
            long rid = info == null ? 0L : info.region().getId();
            byRegionId.computeIfAbsent(rid, x -> new ArrayList<>()).add(k);
        }
        var ordered = new TreeMap<byte[], List<ByteString>>(Arrays::compareUnsigned);
        for (var e : byRegionId.values()) {
            byte[] startKey = e.get(0).toByteArray();
            ordered.put(startKey, e);
        }
        return ordered;
    }
}
