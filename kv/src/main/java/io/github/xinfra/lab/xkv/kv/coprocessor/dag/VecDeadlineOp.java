package io.github.xinfra.lab.xkv.kv.coprocessor.dag;

import io.github.xinfra.lab.xkv.kv.mvcc.KeyLockedException;

/**
 * Deadline-enforcing wrapper. Checks elapsed wall-clock time before
 * each {@code nextChunk} call and throws {@link DeadlineExceededException}
 * if the request has exceeded its allowed duration.
 */
public final class VecDeadlineOp implements VecOperator {

    private final VecOperator child;
    private final long deadlineNanos;
    private long startNanos;

    public VecDeadlineOp(VecOperator child, long maxDurationMs) {
        this.child = child;
        this.deadlineNanos = maxDurationMs * 1_000_000L;
    }

    @Override
    public void open() {
        startNanos = System.nanoTime();
        child.open();
    }

    @Override
    public CopChunk nextChunk(int batchSize) {
        checkDeadline();
        return child.nextChunk(batchSize);
    }

    @Override
    public KeyLockedException lockError() {
        return child.lockError();
    }

    @Override
    public void close() {
        child.close();
    }

    public long elapsedMs() {
        return (System.nanoTime() - startNanos) / 1_000_000L;
    }

    private void checkDeadline() {
        if (System.nanoTime() - startNanos > deadlineNanos) {
            throw new DeadlineExceededException(deadlineNanos / 1_000_000L);
        }
    }

    public static final class DeadlineExceededException extends RuntimeException {
        private final long limitMs;

        public DeadlineExceededException(long limitMs) {
            super("coprocessor deadline exceeded: limit=" + limitMs + "ms");
            this.limitMs = limitMs;
        }

        public long limitMs() { return limitMs; }
    }
}
