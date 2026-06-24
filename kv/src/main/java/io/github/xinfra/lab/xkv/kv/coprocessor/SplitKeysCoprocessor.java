package io.github.xinfra.lab.xkv.kv.coprocessor;

import com.google.protobuf.ByteString;
import io.github.xinfra.lab.xkv.kv.engine.StorageEngine;
import io.github.xinfra.lab.xkv.kv.mvcc.MvccKey;
import io.github.xinfra.lab.xkv.proto.Coprocessor.Request;
import io.github.xinfra.lab.xkv.proto.Coprocessor.Response;
import io.github.xinfra.lab.xkv.proto.Coprocessor.StreamResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.Consumer;

/**
 * Batch-split-region coprocessor (tp=4).
 *
 * <p>Scans the region's WRITE CF to find {@code N-1} split keys that divide
 * the region into {@code N} roughly equal parts by byte size. Used by the
 * PD scheduler to pre-compute split points for batch splitting instead of
 * simple binary split.
 *
 * <p>Algorithm:
 * <ol>
 *   <li>Estimate total region size via {@code approximateSize(WRITE CF)}.</li>
 *   <li>Compute target chunk size = {@code totalSize / splitCount}.</li>
 *   <li>Iterate WRITE CF, track accumulated bytes. Each time the running
 *       total crosses a chunk boundary, record the current user key as a
 *       split point.</li>
 *   <li>Deduplicate: skip if the split key equals the previous or equals
 *       start/end keys.</li>
 * </ol>
 *
 * <p>Response data is a serialized {@code SplitRegionResponse} proto containing
 * the discovered split keys.
 */
public final class SplitKeysCoprocessor implements Coprocessor {
    private static final Logger log = LoggerFactory.getLogger(SplitKeysCoprocessor.class);

    private static final int DEFAULT_SPLIT_COUNT = 2;
    private static final int MAX_SPLIT_KEYS = 100;

    private final StorageEngine engine;

    public SplitKeysCoprocessor(StorageEngine engine) {
        this.engine = engine;
    }

    @Override
    public int requestType() {
        return 4;
    }

    @Override
    public Response handle(Request req) {
        try {
            int splitCount = req.getPagingSize() > 0 ? (int) req.getPagingSize() : DEFAULT_SPLIT_COUNT;
            splitCount = Math.min(splitCount, MAX_SPLIT_KEYS + 1);

            byte[] startKey = null;
            byte[] endKey = null;
            if (req.getRangesCount() > 0) {
                var range = req.getRanges(0);
                if (!range.getStart().isEmpty()) startKey = range.getStart().toByteArray();
                if (!range.getEnd().isEmpty()) endKey = range.getEnd().toByteArray();
            }

            List<byte[]> splitKeys = findSplitKeys(startKey, endKey, splitCount);

            return Response.newBuilder()
                    .setData(ByteString.copyFrom(encodeSplitKeys(splitKeys)))
                    .build();
        } catch (Throwable t) {
            log.warn("SplitKeysCoprocessor error", t);
            return Response.newBuilder().setOtherError(t.getMessage()).build();
        }
    }

    @Override
    public void handleStream(Request req, Consumer<StreamResponse> sink) {
        Response resp = handle(req);
        sink.accept(StreamResponse.newBuilder()
                .setData(resp.getData())
                .build());
    }

    static byte[] encodeSplitKeys(List<byte[]> keys) {
        int totalLen = 4;
        for (byte[] k : keys) totalLen += 4 + k.length;
        ByteBuffer buf = ByteBuffer.allocate(totalLen).order(ByteOrder.BIG_ENDIAN);
        buf.putInt(keys.size());
        for (byte[] k : keys) {
            buf.putInt(k.length);
            buf.put(k);
        }
        return buf.array();
    }

    public static List<byte[]> decodeSplitKeys(byte[] data) {
        ByteBuffer buf = ByteBuffer.wrap(data).order(ByteOrder.BIG_ENDIAN);
        int count = buf.getInt();
        List<byte[]> keys = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            int len = buf.getInt();
            byte[] key = new byte[len];
            buf.get(key);
            keys.add(key);
        }
        return keys;
    }

    List<byte[]> findSplitKeys(byte[] startKey, byte[] endKey, int splitCount) {
        if (splitCount <= 1) return List.of();

        byte[] seekStart = (startKey != null) ? MvccKey.lockKey(startKey) : new byte[]{0};
        byte[] seekEnd = (endKey != null && endKey.length > 0)
                ? MvccKey.lockKey(endKey) : null;

        long totalSize = engine.approximateSize(StorageEngine.Cf.WRITE, seekStart,
                seekEnd != null ? seekEnd : new byte[]{(byte) 0xFF, (byte) 0xFF, (byte) 0xFF});

        if (totalSize <= 0) {
            totalSize = exactSize(seekStart, seekEnd);
        }
        if (totalSize <= 0) return List.of();

        long chunkSize = totalSize / splitCount;
        if (chunkSize <= 0) return List.of();

        List<byte[]> keys = new ArrayList<>();
        long accumulated = 0;
        int nextChunkIndex = 1;
        byte[] lastUserKey = null;

        try (var ro = engine.newReadOptions();
             var it = engine.newIterator(StorageEngine.Cf.WRITE, ro)) {
            for (it.seek(seekStart); it.isValid() && keys.size() < splitCount - 1; it.next()) {
                byte[] mvccKey = it.key();

                if (seekEnd != null && Arrays.compareUnsigned(mvccKey, seekEnd) >= 0) break;

                accumulated += mvccKey.length + it.value().length;

                if (accumulated >= chunkSize * nextChunkIndex) {
                    byte[] userKey;
                    try {
                        userKey = MvccKey.userKey(mvccKey);
                    } catch (Throwable t) {
                        continue;
                    }

                    if (lastUserKey != null && Arrays.equals(userKey, lastUserKey)) continue;
                    if (startKey != null && Arrays.equals(userKey, startKey)) continue;
                    if (endKey != null && Arrays.equals(userKey, endKey)) continue;

                    keys.add(userKey);
                    lastUserKey = userKey;
                    nextChunkIndex++;
                }
            }
        }

        return keys;
    }

    private long exactSize(byte[] seekStart, byte[] seekEnd) {
        long size = 0;
        try (var ro = engine.newReadOptions();
             var it = engine.newIterator(StorageEngine.Cf.WRITE, ro)) {
            for (it.seek(seekStart); it.isValid(); it.next()) {
                byte[] k = it.key();
                if (seekEnd != null && Arrays.compareUnsigned(k, seekEnd) >= 0) break;
                size += k.length + it.value().length;
            }
        }
        return size;
    }
}
