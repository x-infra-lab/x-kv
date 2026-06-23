package io.github.xinfra.lab.xkv.kv.raft;

import io.github.xinfra.lab.xkv.kv.engine.StorageEngine;

import java.util.EnumMap;
import java.util.Map;

/**
 * Routes apply calls by {@link ProposalCodec.Kind} to a per-kind handler.
 *
 * <p>Built so the same {@link RegionPeerImpl} can serve raw KV + MVCC +
 * admin entries without a giant switch in the caller. The peer supplies
 * one composite handler at startup; this dispatcher fans out to the
 * handler registered for each entry's kind.
 */
public final class CompositeApplyHandler implements ApplyHandler {

    private final Map<ProposalCodec.Kind, ApplyHandler> handlers =
            new EnumMap<>(ProposalCodec.Kind.class);

    public CompositeApplyHandler register(ProposalCodec.Kind kind, ApplyHandler handler) {
        handlers.put(kind, handler);
        return this;
    }

    @Override
    public Result apply(ProposalCodec.Decoded decoded, StorageEngine.WriteBatch batch) {
        var h = handlers.get(decoded.kind());
        if (h == null) return Result.err("no handler for kind: " + decoded.kind());
        return h.apply(decoded, batch);
    }

    public static CompositeApplyHandler defaultFor(StorageEngine engine) {
        return defaultFor(engine, new io.github.xinfra.lab.xkv.kv.mvcc.MaxTsTracker());
    }

    public static CompositeApplyHandler defaultFor(StorageEngine engine,
            io.github.xinfra.lab.xkv.kv.mvcc.MaxTsTracker maxTs) {
        return defaultFor(engine, new io.github.xinfra.lab.xkv.kv.mvcc.ConcurrencyManager(maxTs));
    }

    public static CompositeApplyHandler defaultFor(StorageEngine engine,
            io.github.xinfra.lab.xkv.kv.mvcc.ConcurrencyManager cm) {
        return defaultFor(engine, cm, 0, null);
    }

    public static CompositeApplyHandler defaultFor(StorageEngine engine,
            io.github.xinfra.lab.xkv.kv.mvcc.ConcurrencyManager cm,
            long regionId,
            io.github.xinfra.lab.xkv.kv.cdc.CdcEventBus cdcEventBus) {
        return defaultFor(engine, cm, regionId, cdcEventBus, null);
    }

    public static CompositeApplyHandler defaultFor(StorageEngine engine,
            io.github.xinfra.lab.xkv.kv.mvcc.ConcurrencyManager cm,
            long regionId,
            io.github.xinfra.lab.xkv.kv.cdc.CdcEventBus cdcEventBus,
            io.github.xinfra.lab.xkv.kv.mvcc.InMemoryLockTable inMemoryLockTable) {
        var c = new CompositeApplyHandler();
        var raw = new RawKvApplyHandler(engine);
        var mvcc = new MvccApplyHandler(engine, cm, regionId, cdcEventBus);
        if (inMemoryLockTable != null) mvcc.setInMemoryLockTable(inMemoryLockTable);
        c.register(ProposalCodec.Kind.RAW_PUT, raw);
        c.register(ProposalCodec.Kind.RAW_DELETE, raw);
        c.register(ProposalCodec.Kind.RAW_DELETE_RANGE, raw);
        c.register(ProposalCodec.Kind.RAW_CAS, raw);
        c.register(ProposalCodec.Kind.MVCC_PREWRITE, mvcc);
        c.register(ProposalCodec.Kind.MVCC_COMMIT, mvcc);
        c.register(ProposalCodec.Kind.MVCC_ROLLBACK, mvcc);
        c.register(ProposalCodec.Kind.MVCC_PESSIMISTIC_LOCK, mvcc);
        c.register(ProposalCodec.Kind.MVCC_PESSIMISTIC_ROLLBACK, mvcc);
        c.register(ProposalCodec.Kind.MVCC_RESOLVE, mvcc);
        c.register(ProposalCodec.Kind.MVCC_GC, mvcc);
        c.register(ProposalCodec.Kind.MVCC_CHECK_TXN_STATUS, mvcc);
        c.register(ProposalCodec.Kind.MVCC_TXN_HEARTBEAT, mvcc);
        c.register(ProposalCodec.Kind.MVCC_CHECK_SECONDARY_LOCKS, mvcc);
        c.register(ProposalCodec.Kind.TXN_DELETE_RANGE, mvcc);
        return c;
    }

    /**
     * Attach admin handlers (compact log, future split/merge) — needs the
     * per-region raft engine so it can mutate first_index. Call after
     * {@link #defaultFor} but before the region peer starts applying.
     */
    public CompositeApplyHandler withAdmin(
            io.github.xinfra.lab.xkv.kv.engine.PerRegionRaftEngine raftEngine) {
        return withAdmin(raftEngine, null, null);
    }

    /**
     * Full admin wiring: same as {@link #withAdmin(io.github.xinfra.lab.xkv.kv.engine.PerRegionRaftEngine)}
     * plus the storage + split observer so {@code ADMIN_SPLIT} can persist
     * child region descriptors and trigger the host {@code Store} to refresh
     * the parent's in-memory descriptor and spawn a peer for each child.
     */
    public CompositeApplyHandler withAdmin(
            io.github.xinfra.lab.xkv.kv.engine.PerRegionRaftEngine raftEngine,
            StorageEngine storage,
            AdminApplyHandler.SplitObserver splitObserver) {
        return withAdmin(raftEngine, storage, splitObserver, null);
    }

    /** Full admin wiring including the merge observer. */
    public CompositeApplyHandler withAdmin(
            io.github.xinfra.lab.xkv.kv.engine.PerRegionRaftEngine raftEngine,
            StorageEngine storage,
            AdminApplyHandler.SplitObserver splitObserver,
            AdminApplyHandler.MergeObserver mergeObserver) {
        var admin = new AdminApplyHandler(raftEngine, storage, splitObserver, mergeObserver);
        register(ProposalCodec.Kind.ADMIN_COMPACT_LOG, admin);
        register(ProposalCodec.Kind.ADMIN_SPLIT, admin);
        register(ProposalCodec.Kind.ADMIN_COMMIT_MERGE, admin);
        register(ProposalCodec.Kind.ADMIN_PREPARE_MERGE, admin);
        register(ProposalCodec.Kind.ADMIN_ROLLBACK_MERGE, admin);
        return this;
    }
}
