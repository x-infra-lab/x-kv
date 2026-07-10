package io.github.xinfra.lab.xkv.kv.coprocessor.dag;

import io.github.xinfra.lab.xkv.kv.coprocessor.eval.CopDatum;
import io.github.xinfra.lab.xkv.kv.coprocessor.eval.CopRow;
import io.github.xinfra.lab.xkv.kv.coprocessor.eval.ExprEvaluator;
import io.github.xinfra.lab.xkv.kv.mvcc.KeyLockedException;
import io.github.xinfra.lab.xkv.proto.Tipb;

import java.util.List;

/**
 * Vectorized projection operator. Evaluates a list of expressions
 * against each input row, producing output rows with computed columns.
 */
public final class VecProjectionOp implements VecOperator {

    private final VecOperator child;
    private final List<Tipb.Expr> projections;

    public VecProjectionOp(VecOperator child, List<Tipb.Expr> projections) {
        this.child = child;
        this.projections = projections;
    }

    @Override
    public void open() {
        child.open();
    }

    @Override
    public CopChunk nextChunk(int batchSize) {
        CopChunk input = child.nextChunk(batchSize);
        if (input == null) return null;

        CopChunk output = new CopChunk(input.size());
        for (int i = 0; i < input.size(); i++) {
            CopRecord record = input.get(i);
            CopRow srcRow = record.row();

            CopDatum[] projected = new CopDatum[projections.size()];
            for (int j = 0; j < projections.size(); j++) {
                projected[j] = ExprEvaluator.eval(projections.get(j), srcRow);
            }

            output.add(new CopRecord(record.key(), record.value(), new CopRow(projected)));
        }
        return output;
    }

    @Override
    public KeyLockedException lockError() {
        return child.lockError();
    }

    @Override
    public void close() {
        child.close();
    }
}
