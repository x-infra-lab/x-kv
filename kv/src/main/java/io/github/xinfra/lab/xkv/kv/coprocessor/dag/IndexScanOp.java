package io.github.xinfra.lab.xkv.kv.coprocessor.dag;

import io.github.xinfra.lab.xkv.kv.coprocessor.eval.CopDatum;
import io.github.xinfra.lab.xkv.kv.coprocessor.eval.CopRow;
import io.github.xinfra.lab.xkv.kv.coprocessor.eval.TidbKeyCodec;
import io.github.xinfra.lab.xkv.kv.mvcc.MvccReader;
import io.github.xinfra.lab.xkv.proto.Coprocessor;
import io.github.xinfra.lab.xkv.proto.Tipb;

import java.util.List;

/**
 * Scans index entries and decodes indexed column values + row handle.
 *
 * <p>For non-unique index: handle is stored in the MVCC value (8 bytes).
 * <p>For unique index: handle is the last 8 bytes of the index key.
 *
 * <p>Each record's {@link CopRow} contains the indexed column values followed
 * by the handle as the last element. This allows {@link IndexLookupOp} to
 * extract the handle for the double-read.
 */
public final class IndexScanOp implements CopOperator {

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

    public IndexScanOp(MvccReader reader, Tipb.DAGRequest dagReq,
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
                CopRow row = decodeIndexEntry(pair.key(), pair.value());
                return new CopRecord(pair.key(), pair.value(), row);
            }
            if (!fetchNextBatch()) return null;
        }
    }

    @Override
    public void close() {
        currentBatch = null;
    }

    private CopRow decodeIndexEntry(byte[] key, byte[] value) {
        List<Tipb.ColumnInfo> indexColumns = dagReq.getIndexColumnsList();
        CopDatum[] colValues = TidbKeyCodec.decodeIndexColumns(key, indexColumns);

        long handle;
        if (value != null && value.length >= 8) {
            handle = TidbKeyCodec.decodeHandleFromValue(value);
        } else {
            handle = TidbKeyCodec.decodeInt64(key, key.length - 8);
        }

        CopDatum[] values = new CopDatum[colValues.length + 1];
        System.arraycopy(colValues, 0, values, 0, colValues.length);
        values[colValues.length] = CopDatum.of(handle);
        return new CopRow(values);
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
