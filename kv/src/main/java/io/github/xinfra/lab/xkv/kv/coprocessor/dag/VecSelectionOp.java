package io.github.xinfra.lab.xkv.kv.coprocessor.dag;

import io.github.xinfra.lab.xkv.kv.coprocessor.eval.ExprEvaluator;
import io.github.xinfra.lab.xkv.proto.Tipb;

import java.util.ArrayList;
import java.util.List;

/**
 * Vectorized selection (WHERE filter) operator. Evaluates conditions
 * against each record in the chunk and returns only matching rows.
 */
public final class VecSelectionOp implements VecOperator {

    private final VecOperator child;
    private final List<Tipb.Expr> conditions;

    public VecSelectionOp(VecOperator child, List<Tipb.Expr> conditions) {
        this.child = child;
        this.conditions = conditions;
    }

    @Override
    public void open() {
        child.open();
    }

    @Override
    public CopChunk nextChunk(int batchSize) {
        while (true) {
            CopChunk input = child.nextChunk(batchSize);
            if (input == null) return null;

            var filtered = new ArrayList<CopRecord>(input.size());
            for (int i = 0; i < input.size(); i++) {
                CopRecord record = input.get(i);
                if (ExprEvaluator.passesFilter(record.row(), conditions)) {
                    filtered.add(record);
                }
            }

            if (!filtered.isEmpty()) {
                return new CopChunk(filtered);
            }
        }
    }

    @Override
    public void close() {
        child.close();
    }
}
