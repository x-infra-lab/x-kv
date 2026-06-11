package io.github.xinfra.lab.xkv.kv.engine;

import io.github.xinfra.lab.xkv.proto.KvServerpb;

import java.nio.file.Path;
import java.util.function.Consumer;

/**
 * Snapshot generation + application.
 *
 * <h3>Atomicity contract (lesson from v1)</h3>
 *
 * <p>Both sides of a snapshot exchange MUST present a single, consistent
 * point-in-time view across all three CFs:
 *
 * <ol>
 *   <li><b>Sender</b> — open ONE {@link StorageEngine.Snapshot} handle;
 *       bind every CF iterator to it via {@code ReadOptions.snapshot(snap)}.
 *       Never let one CF's iterator outlive the snapshot. The v1
 *       {@code SnapshotBuilder} created three independent snapshots — receivers
 *       observed mismatched states (a lock referencing a value not in the
 *       default CF, etc).</li>
 *
 *   <li><b>Receiver</b> — stage all chunks to a temp directory; verify CRC;
 *       only then atomically swap by ingesting all CFs in one
 *       {@code IngestExternalFile} call. Never {@code deleteRange} +
 *       {@code ingest} in two steps — a crash between them loses the range.
 *       This was the v1 {@code SnapshotApplier} bug.</li>
 * </ol>
 */
public interface SnapshotEngine {

    /**
     * Build a snapshot of {@code regionId}'s key range and stream chunks via
     * {@code chunkSink}. The implementation MUST:
     * <ul>
     *   <li>open exactly ONE {@code StorageEngine.Snapshot}</li>
     *   <li>iterate every CF against that one snapshot</li>
     *   <li>terminate cleanly on iterator exhaustion (eof=true on last chunk)</li>
     * </ul>
     */
    void buildAndStream(long regionId,
                        long term,
                        long index,
                        byte[] startKey,
                        byte[] endKey,
                        Consumer<KvServerpb.SnapshotChunk> chunkSink);

    /**
     * Receive a snapshot and atomically install it into {@code regionId}.
     *
     * <p>The receiver collects all chunks for all CFs into a staging
     * directory. Only when EOF arrives for every CF does it call
     * {@link StorageEngine#ingestSst} once per CF inside one outer
     * write batch (containing also the new {@code RaftEngine.SnapshotMeta}
     * and the truncate of stale data). On any failure mid-receive the
     * staging directory is discarded and the operation retries.
     */
    void receiveAndInstall(long regionId, Iterable<KvServerpb.SnapshotChunk> chunks);

    /** Local checkpoint directory used for staging — distinct per region. */
    Path checkpointDirFor(long regionId);
}
