package io.github.xinfra.lab.xkv.kv.raft;

import io.github.xinfra.lab.raft.proto.Eraftpb;
import io.github.xinfra.lab.xkv.kv.engine.PerRegionRaftEngine;
import io.github.xinfra.lab.xkv.kv.engine.StorageEngine;
import io.github.xinfra.lab.xkv.kv.mvcc.ConcurrencyManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Async apply thread pool — decouples committed-entry application from the
 * raft poller thread.
 *
 * <p>The poller calls {@link #submit} with a batch of committed entries for a
 * region. The ApplyWorker runs the per-entry apply logic (WriteBatch, engine
 * write, ConcurrencyManager latch acquisition) on a dedicated pool, then
 * invokes the {@link ApplyCallback} to fan out proposal results and drain
 * ReadIndex waiters.
 *
 * <p>Entries for the same region are serialized: a new batch is not started
 * until the previous one completes. This preserves the per-region apply
 * ordering invariant.
 */
public final class ApplyWorker {
    private static final Logger log = LoggerFactory.getLogger(ApplyWorker.class);

    private final ExecutorService pool;
    private final ConcurrentHashMap<Long, AtomicBoolean> regionBusy = new ConcurrentHashMap<>();

    public ApplyWorker(int threads) {
        this.pool = Executors.newFixedThreadPool(threads, r -> {
            var t = new Thread(r, "apply-worker");
            t.setDaemon(true);
            return t;
        });
    }

    /**
     * Submit a batch of committed entries for async application.
     *
     * @return true if the task was submitted; false if the region has a pending
     *         apply (caller must buffer and retry after the current apply completes)
     */
    public boolean submit(ApplyTask task) {
        var busy = regionBusy.computeIfAbsent(task.regionId, k -> new AtomicBoolean(false));
        if (!busy.compareAndSet(false, true)) {
            return false;
        }
        pool.execute(() -> {
            try {
                executeApply(task);
            } catch (Throwable t) {
                log.error("region={} async apply failed", task.regionId, t);
                task.callback.onError(t);
            } finally {
                busy.set(false);
                task.callback.onComplete();
            }
        });
        return true;
    }

    private void executeApply(ApplyTask task) {
        var pending = new ArrayList<RegionMailbox.PendingDispatch>();
        var confChanges = new ArrayList<Eraftpb.ConfChangeV2>();
        var postFlushCallbacks = new ArrayList<Runnable>();
        boolean wroteAnything = false;

        for (var entry : task.entries) {
            long idx = entry.getIndex();
            if (idx <= task.raftEngine.appliedIndex()) continue;

            if (applyOneEntry(entry, pending, confChanges, postFlushCallbacks, idx, task)) {
                wroteAnything = true;
            }
        }

        // Max_ts persistence.
        if (task.cm != null) {
            long inMemory = task.cm.maxTs().current();
            if (inMemory > task.raftEngine.persistedMaxTs()) {
                try (var mtsBatch = task.storage.newWriteBatch()) {
                    task.raftEngine.saveMaxTs(inMemory, mtsBatch);
                    task.storage.write(mtsBatch, false);
                    wroteAnything = true;
                }
            }
        }

        if (wroteAnything) {
            task.storage.flushWal(true);
        }

        for (var cb : postFlushCallbacks) {
            try { cb.run(); }
            catch (Throwable t) { log.warn("region={} postFlush callback failed", task.regionId, t); }
        }

        task.callback.onApplied(pending, confChanges);
    }

    private boolean applyOneEntry(Eraftpb.Entry entry,
                                   List<RegionMailbox.PendingDispatch> pending,
                                   List<Eraftpb.ConfChangeV2> confChanges,
                                   List<Runnable> postFlushCallbacks,
                                   long idx,
                                   ApplyTask task) {
        if (entry.getEntryType() == Eraftpb.EntryType.EntryConfChange) {
            try {
                var cc = Eraftpb.ConfChange.parseFrom(entry.getData());
                var ccv2 = Eraftpb.ConfChangeV2.newBuilder()
                        .addChanges(Eraftpb.ConfChangeSingle.newBuilder()
                                .setType(cc.getChangeType())
                                .setNodeId(cc.getNodeId()))
                        .build();
                confChanges.add(ccv2);
            } catch (Throwable t) {
                log.warn("region={} conf-change parse failed: {}", task.regionId, t.getMessage());
            }
            try (var batch = task.storage.newWriteBatch()) {
                task.raftEngine.saveAppliedIndex(idx, batch);
                task.storage.write(batch, false);
            }
            return true;
        }
        if (entry.getEntryType() == Eraftpb.EntryType.EntryConfChangeV2) {
            try {
                confChanges.add(Eraftpb.ConfChangeV2.parseFrom(entry.getData()));
            } catch (Throwable t) {
                log.warn("region={} conf-change-v2 parse failed: {}", task.regionId, t.getMessage());
            }
            try (var batch = task.storage.newWriteBatch()) {
                task.raftEngine.saveAppliedIndex(idx, batch);
                task.storage.write(batch, false);
            }
            return true;
        }
        if (entry.getData().size() == 0) {
            try (var batch = task.storage.newWriteBatch()) {
                task.raftEngine.saveAppliedIndex(idx, batch);
                task.storage.write(batch, false);
            }
            return true;
        }

        ProposalCodec.Decoded decoded;
        try { decoded = ProposalCodec.decode(entry.getData().toByteArray()); }
        catch (Throwable t) {
            log.warn("region={} decode failed: {}", task.regionId, t.getMessage());
            try (var batch = task.storage.newWriteBatch()) {
                task.raftEngine.saveAppliedIndex(idx, batch);
                task.storage.write(batch, false);
            }
            return true;
        }

        if (task.raftEngine.isMerging() && !isMergeAdminKind(decoded.kind())) {
            try (var batch = task.storage.newWriteBatch()) {
                task.raftEngine.saveAppliedIndex(idx, batch);
                task.storage.write(batch, false);
            }
            if (decoded.proposeSeq() != 0) {
                pending.add(new RegionMailbox.PendingDispatch(decoded.proposeSeq(),
                        RegionPeer.ApplyResult.err("region merging — write rejected")));
            }
            return true;
        }

        var keys = ProposalKeyScope.peekKeys(decoded);
        java.util.function.Supplier<Void> work = () -> {
            try (var batch = task.storage.newWriteBatch()) {
                ApplyHandler.Result r = task.applyHandler.apply(decoded, batch);
                var outcome = r.success()
                        ? RegionPeer.ApplyResult.ok(r.response())
                        : RegionPeer.ApplyResult.err(r.errorMessage());
                if (decoded.proposeSeq() != 0) {
                    pending.add(new RegionMailbox.PendingDispatch(decoded.proposeSeq(), outcome));
                }
                if (r.postFlush() != null) {
                    postFlushCallbacks.add(r.postFlush());
                }
                task.raftEngine.saveAppliedIndex(idx, batch);
                task.storage.write(batch, false);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
            return null;
        };
        if (task.cm == null) {
            work.get();
        } else if (keys.isEmpty()) {
            task.cm.withWriter(work);
        } else {
            task.cm.withWriter(keys.get(), work);
        }
        return true;
    }

    private static boolean isMergeAdminKind(ProposalCodec.Kind kind) {
        return kind == ProposalCodec.Kind.ADMIN_PREPARE_MERGE
                || kind == ProposalCodec.Kind.ADMIN_COMMIT_MERGE
                || kind == ProposalCodec.Kind.ADMIN_ROLLBACK_MERGE;
    }

    public void shutdown() {
        pool.shutdown();
        try { pool.awaitTermination(5, TimeUnit.SECONDS); }
        catch (InterruptedException e) { Thread.currentThread().interrupt(); }
    }

    public record ApplyTask(
            long regionId,
            List<Eraftpb.Entry> entries,
            ApplyHandler applyHandler,
            PerRegionRaftEngine raftEngine,
            StorageEngine storage,
            ConcurrencyManager cm,
            ApplyCallback callback) {}

    public interface ApplyCallback {
        void onApplied(List<RegionMailbox.PendingDispatch> pending,
                       List<Eraftpb.ConfChangeV2> confChanges);
        void onError(Throwable t);
        void onComplete();
    }
}
