package io.github.xinfra.lab.xkv.kv.coprocessor;

import com.google.protobuf.ByteString;
import io.github.xinfra.lab.xkv.kv.engine.StorageEngine;
import io.github.xinfra.lab.xkv.kv.mvcc.KeyLockedException;
import io.github.xinfra.lab.xkv.kv.mvcc.MvccReader;
import io.github.xinfra.lab.xkv.proto.Coprocessor.Request;
import io.github.xinfra.lab.xkv.proto.Coprocessor.Response;
import io.github.xinfra.lab.xkv.proto.Coprocessor.StreamResponse;
import io.github.xinfra.lab.xkv.proto.Kvrpcpb;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.function.Consumer;

/**
 * Generic table-scan coprocessor (tp=0).
 *
 * <p>Scans MVCC data across the requested key ranges at the given
 * {@code start_ts} and returns key-value pairs visible at that snapshot.
 * This matches TiKV's KV-layer coprocessor behavior: the SQL operator
 * tree in {@code Request.data} is opaque to this layer — it only does
 * the scan + MVCC visibility filter.
 *
 * <p>Wire format for {@code Response.data}: protobuf-serialized
 * {@code SelectResponse} containing {@code repeated KvPair}. Since we
 * don't have the tipb SelectResponse proto, we use a simple binary
 * format: {@code [4B count][N x [4B keyLen][key][4B valLen][val]]}.
 */
public final class TableScanCoprocessor implements Coprocessor {
    private static final Logger log = LoggerFactory.getLogger(TableScanCoprocessor.class);

    private static final int DEFAULT_SCAN_LIMIT = 1024;
    private static final int STREAM_CHUNK_SIZE = 256;

    private final StorageEngine engine;

    public TableScanCoprocessor(StorageEngine engine) {
        this.engine = engine;
    }

    @Override
    public int requestType() { return 0; }

    @Override
    public Response handle(Request req) {
        long startTs = req.getStartTs();
        int limit = req.getPagingSize() > 0 ? (int) req.getPagingSize() : DEFAULT_SCAN_LIMIT;

        try (var snapshot = engine.newSnapshot();
             var reader = new MvccReader(engine, snapshot, false)) {
            var dataBuilder = new KvPairEncoder();

            int total = 0;
            for (var range : req.getRangesList()) {
                if (total >= limit) break;
                byte[] start = range.getStart().toByteArray();
                byte[] end = range.getEnd().toByteArray();
                int rangeLimit = limit - total;
                var result = reader.scan(start, end, rangeLimit, startTs);
                for (var pair : result.pairs()) {
                    dataBuilder.add(pair.key(), pair.value());
                    total++;
                }
                if (result.lockError() != null) {
                    var e = result.lockError();
                    return Response.newBuilder()
                            .setData(ByteString.copyFrom(dataBuilder.encode()))
                            .setLocked(Kvrpcpb.KeyError.newBuilder()
                                    .setLocked(toLockInfo(e.key(), e.lock())))
                            .build();
                }
            }

            return Response.newBuilder()
                    .setData(ByteString.copyFrom(dataBuilder.encode()))
                    .build();
        } catch (Throwable t) {
            return Response.newBuilder()
                    .setOtherError(t.getMessage())
                    .build();
        }
    }

    @Override
    public void handleStream(Request req, Consumer<StreamResponse> sink) {
        long startTs = req.getStartTs();
        int pagingSize = req.getPagingSize() > 0 ? (int) req.getPagingSize() : Integer.MAX_VALUE;

        try (var snapshot = engine.newSnapshot();
             var reader = new MvccReader(engine, snapshot, false)) {
            int total = 0;

            for (var range : req.getRangesList()) {
                if (total >= pagingSize) break;
                byte[] start = range.getStart().toByteArray();
                byte[] end = range.getEnd().toByteArray();

                byte[] cursor = start;
                boolean hitLock = false;
                while (total < pagingSize) {
                    int chunkLimit = Math.min(STREAM_CHUNK_SIZE, pagingSize - total);
                    var result = reader.scan(cursor, end, chunkLimit, startTs);
                    if (!result.pairs().isEmpty()) {
                        var enc = new KvPairEncoder();
                        for (var pair : result.pairs()) {
                            enc.add(pair.key(), pair.value());
                        }
                        sink.accept(StreamResponse.newBuilder()
                                .setData(ByteString.copyFrom(enc.encode()))
                                .build());
                        total += result.pairs().size();
                    }
                    if (result.lockError() != null) {
                        var e = result.lockError();
                        sink.accept(StreamResponse.newBuilder()
                                .setLocked(Kvrpcpb.KeyError.newBuilder()
                                        .setLocked(toLockInfo(e.key(), e.lock())))
                                .build());
                        hitLock = true;
                        break;
                    }
                    if (result.pairs().isEmpty() || result.pairs().size() < chunkLimit) break;

                    byte[] lastKey = result.pairs().get(result.pairs().size() - 1).key();
                    cursor = nextKey(lastKey);
                }
                if (hitLock) break;
            }
        } catch (Throwable t) {
            sink.accept(StreamResponse.newBuilder()
                    .setOtherError(t.getMessage())
                    .build());
        }
    }

    private static Kvrpcpb.LockInfo toLockInfo(byte[] key,
                                                io.github.xinfra.lab.xkv.kv.mvcc.Lock lock) {
        var b = Kvrpcpb.LockInfo.newBuilder()
                .setPrimaryLock(ByteString.copyFrom(lock.primary()))
                .setLockVersion(lock.startTs())
                .setKey(ByteString.copyFrom(key))
                .setLockTtl(lock.ttlMs())
                .setTxnSize(lock.txnSize())
                .setMinCommitTs(lock.minCommitTs())
                .setUseAsyncCommit(lock.useAsyncCommit())
                .setLockForUpdateTs(lock.forUpdateTs());
        switch (lock.type()) {
            case PUT -> b.setLockType(Kvrpcpb.Op.Put);
            case DELETE -> b.setLockType(Kvrpcpb.Op.Del);
            case LOCK -> b.setLockType(Kvrpcpb.Op.Lock);
            case PESSIMISTIC -> b.setLockType(Kvrpcpb.Op.PessimisticLock);
        }
        return b.build();
    }

    private static byte[] nextKey(byte[] key) {
        var result = new byte[key.length + 1];
        System.arraycopy(key, 0, result, 0, key.length);
        return result;
    }

    /**
     * Binary encoder for scan results: {@code [4B count][N x [4B keyLen][key][4B valLen][val]]}.
     */
    static final class KvPairEncoder {
        private final java.util.List<byte[]> keys = new java.util.ArrayList<>();
        private final java.util.List<byte[]> values = new java.util.ArrayList<>();
        private int totalBytes = 4; // 4 bytes for count header

        void add(byte[] key, byte[] value) {
            keys.add(key);
            values.add(value);
            totalBytes += 4 + key.length + 4 + value.length;
        }

        byte[] encode() {
            byte[] out = new byte[totalBytes];
            int off = 0;
            int count = keys.size();
            out[off++] = (byte) (count >>> 24);
            out[off++] = (byte) (count >>> 16);
            out[off++] = (byte) (count >>> 8);
            out[off++] = (byte) count;
            for (int i = 0; i < count; i++) {
                byte[] k = keys.get(i);
                out[off++] = (byte) (k.length >>> 24);
                out[off++] = (byte) (k.length >>> 16);
                out[off++] = (byte) (k.length >>> 8);
                out[off++] = (byte) k.length;
                System.arraycopy(k, 0, out, off, k.length);
                off += k.length;

                byte[] v = values.get(i);
                out[off++] = (byte) (v.length >>> 24);
                out[off++] = (byte) (v.length >>> 16);
                out[off++] = (byte) (v.length >>> 8);
                out[off++] = (byte) v.length;
                System.arraycopy(v, 0, out, off, v.length);
                off += v.length;
            }
            return out;
        }
    }
}
