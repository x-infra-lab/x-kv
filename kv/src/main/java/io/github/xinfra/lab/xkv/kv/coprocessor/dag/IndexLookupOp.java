package io.github.xinfra.lab.xkv.kv.coprocessor.dag;

import io.github.xinfra.lab.xkv.kv.coprocessor.eval.CopRow;
import io.github.xinfra.lab.xkv.kv.coprocessor.eval.CopRowDecoder;
import io.github.xinfra.lab.xkv.kv.coprocessor.eval.TidbKeyCodec;
import io.github.xinfra.lab.xkv.kv.mvcc.MvccReader;
import io.github.xinfra.lab.xkv.proto.Tipb;

/**
 * Double-read operator: takes handles from an {@link IndexScanOp} child,
 * performs point lookups for the full row data, and decodes the row.
 *
 * <p>The child's {@link CopRow} has index column values followed by the
 * handle as the last element. This operator reads the row at
 * {@code t{tableId}_r{handle}} and returns the full decoded row.
 */
public final class IndexLookupOp implements CopOperator {

    private final CopOperator child;
    private final MvccReader reader;
    private final Tipb.DAGRequest dagReq;
    private final long tableId;
    private final long startTs;

    public IndexLookupOp(CopOperator child, MvccReader reader,
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
    public CopRecord next() {
        while (true) {
            CopRecord indexRecord = child.next();
            if (indexRecord == null) return null;

            CopRow indexRow = indexRecord.row();
            long handle = indexRow.get(indexRow.size() - 1).toLong();

            byte[] recordKey = TidbKeyCodec.encodeRecordKey(tableId, handle);
            var valueOpt = reader.get(recordKey, startTs);
            if (valueOpt.isEmpty()) continue;

            byte[] rowValue = valueOpt.get();
            CopRow fullRow = CopRowDecoder.decode(recordKey, rowValue, dagReq);
            return new CopRecord(recordKey, rowValue, fullRow);
        }
    }

    @Override
    public void close() {
        child.close();
    }
}
