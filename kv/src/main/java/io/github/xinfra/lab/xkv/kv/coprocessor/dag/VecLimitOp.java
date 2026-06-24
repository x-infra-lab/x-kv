package io.github.xinfra.lab.xkv.kv.coprocessor.dag;

/**
 * Vectorized limit operator. Passes through chunks from the child
 * until the cumulative row count reaches the limit, truncating the
 * final chunk if necessary.
 */
public final class VecLimitOp implements VecOperator {

    private final VecOperator child;
    private final int limit;
    private int emitted;

    public VecLimitOp(VecOperator child, int limit) {
        this.child = child;
        this.limit = limit;
    }

    @Override
    public void open() {
        child.open();
        emitted = 0;
    }

    @Override
    public CopChunk nextChunk(int batchSize) {
        if (emitted >= limit) return null;

        int remaining = limit - emitted;
        int fetchSize = Math.min(batchSize, remaining);

        CopChunk chunk = child.nextChunk(fetchSize);
        if (chunk == null) return null;

        if (chunk.size() > remaining) {
            chunk = chunk.truncate(remaining);
        }
        emitted += chunk.size();
        return chunk;
    }

    @Override
    public void close() {
        child.close();
    }
}
