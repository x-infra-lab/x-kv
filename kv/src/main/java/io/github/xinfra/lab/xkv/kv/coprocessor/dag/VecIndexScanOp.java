package io.github.xinfra.lab.xkv.kv.coprocessor.dag;

import io.github.xinfra.lab.xkv.kv.coprocessor.eval.CopDatum;
import io.github.xinfra.lab.xkv.kv.coprocessor.eval.CopRow;
import io.github.xinfra.lab.xkv.kv.coprocessor.eval.TidbKeyCodec;
import io.github.xinfra.lab.xkv.kv.mvcc.MvccReader;
import io.github.xinfra.lab.xkv.proto.Coprocessor;
import io.github.xinfra.lab.xkv.proto.Tipb;

import java.util.List;

/**
 * Vectorized index-scan operator. Scans index key ranges in batches,
 * decoding indexed column values and handles per chunk.
 */
public final class VecIndexScanOp implements VecOperator {

    private final MvccReader reader;
    private final Tipb.DAGRequest dagReq;
    private final List<Coprocessor.KeyRange> ranges;
    private final long startTs;

    private int rangeIdx;
    private byte[] cursor;
    private byte[] rangeEnd;
    private boolean exhausted;

    public VecIndexScanOp(MvccReader reader, Tipb.DAGRequest dagReq,
                           List<Coprocessor.KeyRange> ranges, long startTs) {
        this.reader = reader;
        this.dagReq = dagReq;
        this.ranges = ranges;
        this.startTs = startTs;
    }

    @Override
    public void open() {
        rangeIdx = 0;
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

            MvccReader.ScanResult result = reader.scan(cursor, rangeEnd, batchSize, startTs);
            List<MvccReader.KvPair> pairs = result.pairs();

            if (pairs.isEmpty()) {
                cursor = null;
                continue;
            }

            CopChunk chunk = new CopChunk(pairs.size());
            List<Tipb.ColumnInfo> indexColumns = dagReq.getIndexColumnsList();
            for (MvccReader.KvPair pair : pairs) {
                CopRow row = decodeIndexEntry(pair.key(), pair.value(), indexColumns);
                chunk.add(new CopRecord(pair.key(), pair.value(), row));
            }

            if (pairs.size() < batchSize) {
                cursor = null;
            } else {
                byte[] lastKey = pairs.get(pairs.size() - 1).key();
                cursor = nextKey(lastKey);
            }

            return chunk;
        }
        return null;
    }

    @Override
    public void close() {
        cursor = null;
    }

    private CopRow decodeIndexEntry(byte[] key, byte[] value,
                                     List<Tipb.ColumnInfo> indexColumns) {
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
