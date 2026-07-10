package io.github.xinfra.lab.xkv.kv.coprocessor.dag;

import io.github.xinfra.lab.xkv.kv.coprocessor.eval.CopRow;
import io.github.xinfra.lab.xkv.kv.coprocessor.eval.CopRowDecoder;
import io.github.xinfra.lab.xkv.kv.coprocessor.eval.TidbKeyCodec;
import io.github.xinfra.lab.xkv.kv.mvcc.MvccReader;
import io.github.xinfra.lab.xkv.proto.Tipb;

import java.util.ArrayList;

/**
 * Vectorized index-lookup (double-read) operator. Takes chunks of index
 * entries from the child, extracts handles, does batch point lookups to
 * fetch full row data, and returns decoded rows.
 */
public final class VecIndexLookupOp implements VecOperator {

    private static final int MAX_EMPTY_ROUNDS = 10_000;

    private final VecOperator child;
    private final MvccReader reader;
    private final Tipb.DAGRequest dagReq;
    private final long tableId;
    private final long startTs;

    public VecIndexLookupOp(VecOperator child, MvccReader reader,
                             Tipb.DAGRequest dagReq, long startTs) {
        this.child = child;
        this.reader = reader;
        this.dagReq = dagReq;
        this.tableId = dagReq.getTableId();
        this.startTs = startTs;
    }

    @Override
    public void open() {
        child.open();
    }

    @Override
    public CopChunk nextChunk(int batchSize) {
        for (int attempts = 0; attempts < MAX_EMPTY_ROUNDS; attempts++) {
            CopChunk indexChunk = child.nextChunk(batchSize);
            if (indexChunk == null) return null;

            var result = new ArrayList<CopRecord>(indexChunk.size());
            for (int i = 0; i < indexChunk.size(); i++) {
                CopRecord indexRecord = indexChunk.get(i);
                CopRow indexRow = indexRecord.row();
                long handle = indexRow.get(indexRow.size() - 1).toLong();

                byte[] recordKey = TidbKeyCodec.encodeRecordKey(tableId, handle);
                var valueOpt = reader.get(recordKey, startTs);
                if (valueOpt.isEmpty()) continue;

                byte[] rowValue = valueOpt.get();
                CopRow fullRow = CopRowDecoder.decode(recordKey, rowValue, dagReq);
                result.add(new CopRecord(recordKey, rowValue, fullRow));
            }

            if (!result.isEmpty()) {
                return new CopChunk(result);
            }
        }
        return null;
    }

    @Override
    public void close() {
        child.close();
    }
}
