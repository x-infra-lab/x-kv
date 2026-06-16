package io.github.xinfra.lab.xkv.kv.coprocessor.dag;

public final class LimitOp implements CopOperator {

    private final CopOperator child;
    private final int limit;
    private int count;

    public LimitOp(CopOperator child, int limit) {
        this.child = child;
        this.limit = limit;
    }

    @Override
    public void open() {
        child.open();
        count = 0;
    }

    @Override
    public CopRecord next() {
        if (count >= limit) return null;
        CopRecord record = child.next();
        if (record != null) count++;
        return record;
    }

    @Override
    public void close() {
        child.close();
    }
}
