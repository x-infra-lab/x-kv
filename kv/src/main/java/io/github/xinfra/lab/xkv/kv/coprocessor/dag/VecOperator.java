package io.github.xinfra.lab.xkv.kv.coprocessor.dag;

import io.github.xinfra.lab.xkv.kv.mvcc.KeyLockedException;

/**
 * Vectorized (chunk-based) operator interface.
 *
 * <p>Returns batches of records instead of one-at-a-time, amortizing
 * virtual dispatch and enabling batch evaluation in downstream operators.
 */
public interface VecOperator extends AutoCloseable {

    void open();

    /**
     * Returns the next chunk of up to {@code batchSize} records,
     * or {@code null} when the input is exhausted.
     */
    CopChunk nextChunk(int batchSize);

    default KeyLockedException lockError() { return null; }

    @Override
    void close();
}
