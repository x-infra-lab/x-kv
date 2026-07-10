package io.github.xinfra.lab.xkv.kv.coprocessor.dag;

import io.github.xinfra.lab.xkv.kv.coprocessor.eval.CopRow;
import io.github.xinfra.lab.xkv.kv.coprocessor.eval.CopRowDecoder;
import io.github.xinfra.lab.xkv.kv.mvcc.KeyLockedException;
import io.github.xinfra.lab.xkv.kv.mvcc.MvccReader;
import io.github.xinfra.lab.xkv.proto.Coprocessor;
import io.github.xinfra.lab.xkv.proto.Tipb;

import java.util.List;

/**
 * Vectorized table-scan operator. Produces {@link CopChunk}s by reading
 * batches directly from the MVCC layer, decoding all rows in one sweep.
 */
public final class VecTableScanOp implements VecOperator {

    private final MvccReader reader;
    private final Tipb.DAGRequest dagReq;
    private final List<Coprocessor.KeyRange> ranges;
    private final long startTs;
    private final boolean descScan;

    private int rangeIdx;
    private byte[] cursor;
    private byte[] rangeEnd;
    private boolean exhausted;
    private KeyLockedException lockErr;

    public VecTableScanOp(MvccReader reader, Tipb.DAGRequest dagReq,
                           List<Coprocessor.KeyRange> ranges, long startTs) {
        this.reader = reader;
        this.dagReq = dagReq;
        this.ranges = ranges;
        this.startTs = startTs;
        this.descScan = dagReq.getDescScan();
    }

    @Override
    public void open() {
        rangeIdx = descScan ? ranges.size() - 1 : 0;
        exhausted = false;
        advanceToNextRange();
    }

    @Override
    public CopChunk nextChunk(int batchSize) {
        while (!exhausted) {
            if (cursor == null) {
                if (!advanceToNextRange()) {
                    exhausted = true;
                    return null;
                }
                continue;
            }

            MvccReader.ScanResult result;
            if (descScan) {
                result = reader.reverseScan(cursor, rangeEnd, batchSize, startTs);
            } else {
                result = reader.scan(cursor, rangeEnd, batchSize, startTs);
            }
            List<MvccReader.KvPair> pairs = result.pairs();

            if (result.lockError() != null) {
                lockErr = result.lockError();
                exhausted = true;
                if (pairs.isEmpty()) return null;
            }

            if (pairs.isEmpty()) {
                cursor = null;
                continue;
            }

            CopChunk chunk = new CopChunk(pairs.size());
            for (MvccReader.KvPair pair : pairs) {
                CopRow row = CopRowDecoder.decode(pair.key(), pair.value(), dagReq);
                chunk.add(new CopRecord(pair.key(), pair.value(), row));
            }

            if (lockErr != null) {
                cursor = null;
            } else if (pairs.size() < batchSize) {
                cursor = null;
            } else {
                byte[] lastKey = pairs.get(pairs.size() - 1).key();
                if (descScan) {
                    cursor = lastKey;
                } else {
                    cursor = nextKey(lastKey);
                }
            }

            return chunk;
        }
        return null;
    }

    @Override
    public KeyLockedException lockError() { return lockErr; }

    @Override
    public void close() {
        cursor = null;
    }

    private boolean advanceToNextRange() {
        if (descScan) {
            if (rangeIdx < 0) return false;
            Coprocessor.KeyRange range = ranges.get(rangeIdx--);
            // For reverse scan: cursor = upper bound (exclusive), rangeEnd = lower bound (inclusive)
            cursor = range.getEnd().toByteArray();
            rangeEnd = range.getStart().toByteArray();
        } else {
            if (rangeIdx >= ranges.size()) return false;
            Coprocessor.KeyRange range = ranges.get(rangeIdx++);
            cursor = range.getStart().toByteArray();
            rangeEnd = range.getEnd().toByteArray();
        }
        return true;
    }

    private static byte[] nextKey(byte[] key) {
        byte[] result = new byte[key.length + 1];
        System.arraycopy(key, 0, result, 0, key.length);
        return result;
    }
}
