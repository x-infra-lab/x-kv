package io.github.xinfra.lab.xkv.kv.coprocessor.eval;

public final class CopRow {
    private final CopDatum[] values;

    public CopRow(CopDatum[] values) {
        this.values = values;
    }

    public CopDatum get(int index) {
        if (index < 0 || index >= values.length) return CopDatum.nil();
        return values[index];
    }

    public int size() {
        return values.length;
    }
}
