package io.github.xinfra.lab.xkv.kv.engine;

import com.google.protobuf.ByteString;
import io.github.xinfra.lab.xkv.proto.KvServerpb;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.github.xinfra.lab.xkv.kv.mvcc.MvccKey;

import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.Consumer;
import java.util.zip.CRC32C;

/**
 * Default {@link SnapshotEngine}.
 *
 * <h3>Wire format</h3>
 *
 * <p>For each CF (default / lock / write):
 * <ul>
 *   <li>A first chunk carrying the {@link KvServerpb.SnapshotMeta} (only on
 *       chunk 0 of the very first CF).</li>
 *   <li>One or more data chunks. The {@code data} field is a concatenation
 *       of {@code [4B keyLen][key][4B valueLen][value]} tuples. Chunk size
 *       caps at {@link #CHUNK_BYTE_TARGET}.</li>
 *   <li>The last chunk for the CF has {@code eof=true}; the receiver flushes
 *       that CF's staging buffer on observing it.</li>
 * </ul>
 *
 * <h3>Atomicity</h3>
 *
 * <p>Generation: ONE {@code StorageEngine.Snapshot} pinned at start; all
 * three CF iterators bind to it via {@code ReadOptions.snapshot}. The CFs
 * therefore reflect the same physical instant in time.
 *
 * <p>Application: the receiver buffers the full snapshot in memory (small
 * data sets — production-scale snapshots would use SST + IngestExternalFile,
 * tracked by the {@code StorageEngine.ingestSst} SPI), then applies one
 * atomic {@code WriteBatch} containing:
 *   1. {@code deleteRange} of [startKey, endKey) on each CF (wipe stale)
 *   2. all new key/value puts for each CF (the snapshot's data)
 * The batch flushes once with {@code sync=true}. Atomic from the disk's
 * perspective — a crash mid-install replays the install on next start.
 */
public final class SnapshotEngineImpl implements SnapshotEngine {
    private static final Logger log = LoggerFactory.getLogger(SnapshotEngineImpl.class);

    /** Target uncompressed byte size per chunk; actual chunks may exceed by one record. */
    public static final int CHUNK_BYTE_TARGET = 64 * 1024;

    private static final String[] DATA_CFS = { "default", "lock", "write" };

    private final StorageEngine storage;
    private final Path checkpointRoot;

    public SnapshotEngineImpl(StorageEngine storage, Path checkpointRoot) {
        this.storage = storage;
        this.checkpointRoot = checkpointRoot;
    }

    @Override
    public Path checkpointDirFor(long regionId) { return checkpointRoot.resolve("region-" + regionId); }

    @Override
    public void buildAndStream(long regionId,
                                long term,
                                long index,
                                byte[] startKey,
                                byte[] endKey,
                                Consumer<KvServerpb.SnapshotChunk> chunkSink) {
        long start = System.currentTimeMillis();
        try (var snap = storage.newSnapshot()) {
            var meta = KvServerpb.SnapshotMeta.newBuilder()
                    .setRegionId(regionId)
                    .setRaftTerm(term)
                    .setRaftIndex(index)
                    .setStartKey(ByteString.copyFrom(startKey))
                    .setEndKey(endKey == null ? ByteString.EMPTY : ByteString.copyFrom(endKey))
                    .setGeneratedAtMs(start);
            for (var cfName : DATA_CFS) meta.addCfNames(cfName);

            boolean metaEmitted = false;
            long totalBytes = 0;
            for (String cfName : DATA_CFS) {
                var cf = cfFor(cfName);
                long bytes = streamCf(cf, cfName, snap, startKey, endKey, regionId, meta, metaEmitted, chunkSink);
                metaEmitted = true;
                totalBytes += bytes;
            }
            meta.setTotalSize(totalBytes);
            log.info("snapshot built region={} term={} index={} bytes={} ms={}",
                    regionId, term, index, totalBytes, System.currentTimeMillis() - start);
        }
    }

    /** Emit chunks for one CF; return total payload bytes shipped. */
    private long streamCf(StorageEngine.Cf cf,
                          String cfName,
                          StorageEngine.Snapshot snap,
                          byte[] startKey,
                          byte[] endKey,
                          long regionId,
                          KvServerpb.SnapshotMeta.Builder meta,
                          boolean metaAlreadyEmitted,
                          Consumer<KvServerpb.SnapshotChunk> sink) {
        var ro = storage.newReadOptions().snapshot(snap);
        long bytes = 0;
        int chunkIndex = 0;
        var buf = new java.io.ByteArrayOutputStream(CHUNK_BYTE_TARGET);
        boolean firstEmit = true;
        try (var it = storage.newIterator(cf, ro)) {
            byte[] from = (startKey == null || startKey.length == 0)
                    ? new byte[]{0}
                    : MvccKey.lockKey(startKey);
            byte[] upperBound = (endKey != null && endKey.length > 0)
                    ? MvccKey.lockKey(endKey)
                    : null;
            for (it.seek(from); it.isValid(); it.next()) {
                if (upperBound != null
                        && Arrays.compareUnsigned(it.key(), upperBound) >= 0) break;
                byte[] k = it.key();
                byte[] v = it.value();
                appendTuple(buf, k, v);
                if (buf.size() >= CHUNK_BYTE_TARGET) {
                    sink.accept(buildChunk(regionId, cfName, chunkIndex++,
                            buf.toByteArray(), /* eof= */ false,
                            firstEmit && !metaAlreadyEmitted ? meta : null));
                    bytes += buf.size();
                    buf.reset();
                    firstEmit = false;
                }
            }
        }
        // Final chunk for this CF — always emit, even if empty, so receiver
        // sees EOF and flushes the CF buffer.
        sink.accept(buildChunk(regionId, cfName, chunkIndex,
                buf.toByteArray(), /* eof= */ true,
                firstEmit && !metaAlreadyEmitted ? meta : null));
        bytes += buf.size();
        return bytes;
    }

    private static KvServerpb.SnapshotChunk buildChunk(long regionId,
                                                       String cfName,
                                                       int chunkIndex,
                                                       byte[] data,
                                                       boolean eof,
                                                       KvServerpb.SnapshotMeta.Builder meta) {
        var crc = new CRC32C();
        crc.update(data);
        var b = KvServerpb.SnapshotChunk.newBuilder()
                .setCf(cfName)
                .setChunkIndex(chunkIndex)
                .setEof(eof)
                .setCrc32C(crc.getValue())
                .setData(ByteString.copyFrom(data));
        if (meta != null) b.setMeta(meta.build());
        return b.build();
    }

    private static void appendTuple(java.io.ByteArrayOutputStream sink, byte[] k, byte[] v) {
        var bb = ByteBuffer.allocate(8 + k.length + v.length);
        bb.putInt(k.length).put(k).putInt(v.length).put(v);
        sink.writeBytes(bb.array());
    }

    @Override
    public void receiveAndInstall(long regionId, Iterable<KvServerpb.SnapshotChunk> chunks) {
        // Collect per-CF buffers + the meta (first non-empty one wins).
        KvServerpb.SnapshotMeta meta = null;
        var byCf = new java.util.HashMap<String, List<byte[]>>();
        var seenEof = new java.util.HashSet<String>();
        for (var chunk : chunks) {
            if (chunk.hasMeta() && meta == null) meta = chunk.getMeta();
            // Verify CRC.
            var crc = new CRC32C();
            crc.update(chunk.getData().toByteArray());
            if (crc.getValue() != chunk.getCrc32C()) {
                throw new IllegalStateException(
                        "snapshot chunk CRC mismatch cf=" + chunk.getCf() + " idx=" + chunk.getChunkIndex());
            }
            byCf.computeIfAbsent(chunk.getCf(), k -> new ArrayList<>())
                    .add(chunk.getData().toByteArray());
            if (chunk.getEof()) seenEof.add(chunk.getCf());
        }
        if (meta == null) throw new IllegalStateException("snapshot has no meta chunk");
        for (var cfName : DATA_CFS) {
            if (!seenEof.contains(cfName)) {
                throw new IllegalStateException("snapshot incomplete for cf=" + cfName);
            }
        }

        byte[] startKey = meta.getStartKey().toByteArray();
        byte[] endKey = meta.getEndKey().isEmpty() ? null : meta.getEndKey().toByteArray();

        try (var batch = storage.newWriteBatch()) {
            byte[] lower = (startKey.length == 0)
                    ? new byte[]{0}
                    : MvccKey.lockKey(startKey);
            byte[] upper;
            if (endKey == null) {
                upper = new byte[256];
                Arrays.fill(upper, (byte) 0xFF);
            } else {
                upper = MvccKey.lockKey(endKey);
            }
            for (var cfName : DATA_CFS) {
                batch.deleteRange(cfFor(cfName), lower, upper);
            }
            // Insert fresh data.
            for (var cfName : DATA_CFS) {
                var cf = cfFor(cfName);
                for (var chunkBytes : byCf.getOrDefault(cfName, List.of())) {
                    decodeAndPut(chunkBytes, cf, batch);
                }
            }
            storage.write(batch, /* sync= */ true);
        }
        log.info("snapshot installed region={} index={} ({}B)",
                regionId, meta.getRaftIndex(), meta.getTotalSize());
    }

    private void decodeAndPut(byte[] data, StorageEngine.Cf cf, StorageEngine.WriteBatch batch) {
        var bb = ByteBuffer.wrap(data);
        while (bb.hasRemaining()) {
            int kLen = bb.getInt();
            byte[] k = new byte[kLen]; bb.get(k);
            int vLen = bb.getInt();
            byte[] v = new byte[vLen]; bb.get(v);
            batch.put(cf, k, v);
        }
    }

    private static StorageEngine.Cf cfFor(String cfName) {
        return switch (cfName) {
            case "default" -> StorageEngine.Cf.DEFAULT;
            case "lock" -> StorageEngine.Cf.LOCK;
            case "write" -> StorageEngine.Cf.WRITE;
            default -> throw new IllegalArgumentException("unknown CF: " + cfName);
        };
    }
}
