package io.github.xinfra.lab.xkv.kv.coprocessor.dag;

import io.github.xinfra.lab.xkv.kv.coprocessor.eval.ExprEvaluator;
import io.github.xinfra.lab.xkv.proto.Tipb;

import java.util.List;

public final class SelectionOp implements CopOperator {

    private final CopOperator child;
    private final List<Tipb.Expr> conditions;

    public SelectionOp(CopOperator child, List<Tipb.Expr> conditions) {
        this.child = child;
        this.conditions = conditions;
    }

    @Override
    public void open() {
        child.open();
    }

    @Override
    public CopRecord next() {
        CopRecord record;
        while ((record = child.next()) != null) {
            if (ExprEvaluator.passesFilter(record.row(), conditions)) {
                return record;
            }
        }
        return null;
    }

    @Override
    public void close() {
        child.close();
    }
}
