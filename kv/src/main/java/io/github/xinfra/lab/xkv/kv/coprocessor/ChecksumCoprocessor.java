package io.github.xinfra.lab.xkv.kv.coprocessor;

import com.google.protobuf.ByteString;
import io.github.xinfra.lab.xkv.kv.engine.StorageEngine;
import io.github.xinfra.lab.xkv.kv.mvcc.MvccReader;
import io.github.xinfra.lab.xkv.proto.Coprocessor.Request;
import io.github.xinfra.lab.xkv.proto.Coprocessor.Response;
import io.github.xinfra.lab.xkv.proto.Coprocessor.StreamResponse;
import io.github.xinfra.lab.xkv.kv.coprocessor.dag.VecDeadlineOp;
import io.github.xinfra.lab.xkv.proto.Tipb;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.function.Consumer;
import java.util.zip.CRC32;

/**
 * Checksum coprocessor (tp=5). Computes CRC32 over all key-value pairs
 * in the requested ranges at the given snapshot timestamp.
 * Used by TiDB's {@code ADMIN CHECK TABLE} to verify data integrity.
 */
public final class ChecksumCoprocessor implements Coprocessor {
    private static final Logger log = LoggerFactory.getLogger(ChecksumCoprocessor.class);

    private static final int SCAN_BATCH_SIZE = 1024;

    private final StorageEngine engine;

    public ChecksumCoprocessor(StorageEngine engine) {
        this.engine = engine;
    }

    @Override
    public int requestType() {
        return 5;
    }

    @Override
    public Response handle(Request req) {
        long startTs = req.getStartTs();
        long deadlineMs = SQLScanCoprocessor.extractDeadlineMs(req);
        long startNanos = System.nanoTime();
        long deadlineNanos = deadlineMs > 0 ? deadlineMs * 1_000_000L : 0;

        try (var snapshot = engine.newSnapshot();
             var reader = new MvccReader(engine, snapshot, false)) {

            CRC32 crc = new CRC32();
            long totalKvs = 0;
            long totalBytes = 0;

            for (var range : req.getRangesList()) {
                byte[] cursor = range.getStart().toByteArray();
                byte[] end = range.getEnd().toByteArray();

                while (true) {
                    if (deadlineNanos > 0 && System.nanoTime() - startNanos > deadlineNanos) {
                        throw new VecDeadlineOp.DeadlineExceededException(deadlineMs);
                    }

                    var result = reader.scan(cursor, end, SCAN_BATCH_SIZE, startTs);
                    var pairs = result.pairs();
                    if (pairs.isEmpty()) break;

                    for (var pair : pairs) {
                        crc.update(pair.key());
                        if (pair.value() != null) {
                            crc.update(pair.value());
                            totalBytes += pair.key().length + pair.value().length;
                        } else {
                            totalBytes += pair.key().length;
                        }
                        totalKvs++;
                    }

                    if (result.lockError() != null) break;
                    if (pairs.size() < SCAN_BATCH_SIZE) break;

                    byte[] lastKey = pairs.get(pairs.size() - 1).key();
                    cursor = nextKey(lastKey);
                }
            }

            long elapsedMs = (System.nanoTime() - startNanos) / 1_000_000L;
            Tipb.ChecksumResponse checksumResp = Tipb.ChecksumResponse.newBuilder()
                    .setChecksum(crc.getValue())
                    .setTotalKvs(totalKvs)
                    .setTotalBytes(totalBytes)
                    .build();

            return Response.newBuilder()
                    .setData(ByteString.copyFrom(checksumResp.toByteArray()))
                    .setExecDetailsMs(elapsedMs)
                    .build();
        } catch (Throwable t) {
            log.warn("ChecksumCoprocessor error", t);
            return Response.newBuilder().setOtherError(t.getMessage()).build();
        }
    }

    @Override
    public void handleStream(Request req, Consumer<StreamResponse> sink) {
        Response resp = handle(req);
        sink.accept(StreamResponse.newBuilder().setData(resp.getData()).build());
    }

    private static byte[] nextKey(byte[] key) {
        byte[] result = new byte[key.length + 1];
        System.arraycopy(key, 0, result, 0, key.length);
        return result;
    }
}
