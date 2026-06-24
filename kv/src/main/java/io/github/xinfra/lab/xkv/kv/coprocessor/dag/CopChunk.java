package io.github.xinfra.lab.xkv.kv.coprocessor.dag;

import java.util.ArrayList;
import java.util.List;

/**
 * A batch of {@link CopRecord} rows used for chunk-based (vectorized)
 * operator execution. Reduces per-row virtual dispatch overhead.
 */
public final class CopChunk {

    private final List<CopRecord> records;

    public CopChunk(List<CopRecord> records) {
        this.records = records;
    }

    public CopChunk(int capacity) {
        this.records = new ArrayList<>(capacity);
    }

    public List<CopRecord> records() {
        return records;
    }

    public int size() {
        return records.size();
    }

    public boolean isEmpty() {
        return records.isEmpty();
    }

    public CopRecord get(int index) {
        return records.get(index);
    }

    public void add(CopRecord record) {
        records.add(record);
    }

    public CopChunk truncate(int maxSize) {
        if (records.size() <= maxSize) return this;
        return new CopChunk(new ArrayList<>(records.subList(0, maxSize)));
    }
}
