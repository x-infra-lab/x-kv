package io.github.xinfra.lab.xkv.kv.mvcc;

/**
 * Thrown when prewrite finds a committed write at {@code commitTs >= startTs}
 * — the caller's snapshot is stale.
 */
public final class WriteConflictException extends RuntimeException {
    private final long startTs;
    private final long conflictCommitTs;
    private final byte[] key;

    public WriteConflictException(byte[] key, long startTs, long conflictCommitTs) {
        super("write conflict on key (startTs=" + startTs + ", conflictCommitTs=" + conflictCommitTs + ")");
        this.key = key;
        this.startTs = startTs;
        this.conflictCommitTs = conflictCommitTs;
    }

    public byte[] key() { return key; }
    public long startTs() { return startTs; }
    public long conflictCommitTs() { return conflictCommitTs; }
}
