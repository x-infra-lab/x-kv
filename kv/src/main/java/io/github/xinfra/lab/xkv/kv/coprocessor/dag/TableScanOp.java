package io.github.xinfra.lab.xkv.kv.coprocessor.dag;

import io.github.xinfra.lab.xkv.kv.coprocessor.eval.CopRow;
import io.github.xinfra.lab.xkv.kv.coprocessor.eval.CopRowDecoder;
import io.github.xinfra.lab.xkv.kv.mvcc.MvccReader;
import io.github.xinfra.lab.xkv.proto.Coprocessor;
import io.github.xinfra.lab.xkv.proto.Tipb;

import java.util.List;

public final class TableScanOp implements CopOperator {

    private final MvccReader reader;
    private final Tipb.DAGRequest dagReq;
    private final List<Coprocessor.KeyRange> ranges;
    private final long startTs;
    private final int scanBatchSize;

    private int rangeIdx;
    private byte[] cursor;
    private byte[] rangeEnd;
    private List<MvccReader.KvPair> currentBatch;
    private int batchPos;

    public TableScanOp(MvccReader reader, Tipb.DAGRequest dagReq,
                        List<Coprocessor.KeyRange> ranges,
                        long startTs, int scanBatchSize) {
        this.reader = reader;
        this.dagReq = dagReq;
        this.ranges = ranges;
        this.startTs = startTs;
        this.scanBatchSize = scanBatchSize;
    }

    @Override
    public void open() {
        rangeIdx = 0;
        currentBatch = null;
        batchPos = 0;
        advanceToNextRange();
    }

    @Override
    public CopRecord next() {
        while (true) {
            if (currentBatch != null && batchPos < currentBatch.size()) {
                MvccReader.KvPair pair = currentBatch.get(batchPos++);
                CopRow row = CopRowDecoder.decode(pair.key(), pair.value(), dagReq);
                return new CopRecord(pair.key(), pair.value(), row);
            }
            if (!fetchNextBatch()) return null;
        }
    }

    @Override
    public void close() {
        currentBatch = null;
    }

    private boolean fetchNextBatch() {
        while (true) {
            if (cursor != null) {
                MvccReader.ScanResult result = reader.scan(cursor, rangeEnd, scanBatchSize, startTs);
                if (!result.pairs().isEmpty()) {
                    currentBatch = result.pairs();
                    batchPos = 0;
                    if (result.pairs().size() < scanBatchSize) {
                        cursor = null;
                    } else {
                        byte[] lastKey = result.pairs().get(result.pairs().size() - 1).key();
                        cursor = nextKey(lastKey);
                    }
                    return true;
                }
                cursor = null;
            }
            if (!advanceToNextRange()) return false;
        }
    }

    private boolean advanceToNextRange() {
        if (rangeIdx >= ranges.size()) return false;
        Coprocessor.KeyRange range = ranges.get(rangeIdx++);
        cursor = range.getStart().toByteArray();
        rangeEnd = range.getEnd().toByteArray();
        return true;
    }

    private static byte[] nextKey(byte[] key) {
        byte[] result = new byte[key.length + 1];
        System.arraycopy(key, 0, result, 0, key.length);
        return result;
    }
}
