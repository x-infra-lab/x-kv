package io.github.xinfra.lab.xkv.kv.coprocessor.dag;

import java.util.concurrent.atomic.AtomicLong;

public final class CopMemTracker {

    public static final long DEFAULT_QUOTA = 256L * 1024 * 1024; // 256MB

    private final long quota;
    private final AtomicLong consumed = new AtomicLong();

    public CopMemTracker() {
        this(DEFAULT_QUOTA);
    }

    public CopMemTracker(long quota) {
        this.quota = quota;
    }

    public void track(long bytes) {
        long now = consumed.addAndGet(bytes);
        if (now > quota) {
            throw new MemoryQuotaExceededException(now, quota);
        }
    }

    public long consumed() { return consumed.get(); }
    public long quota() { return quota; }

    public static final class MemoryQuotaExceededException extends RuntimeException {
        private final long consumed;
        private final long quota;

        public MemoryQuotaExceededException(long consumed, long quota) {
            super("coprocessor memory quota exceeded: consumed=" + consumed + " quota=" + quota);
            this.consumed = consumed;
            this.quota = quota;
        }

        public long consumed() { return consumed; }
        public long quota() { return quota; }
    }
}
