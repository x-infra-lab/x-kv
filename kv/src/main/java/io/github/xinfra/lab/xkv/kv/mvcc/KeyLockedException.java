package io.github.xinfra.lab.xkv.kv.mvcc;

/**
 * Thrown when a snapshot read encounters a lock from a concurrent
 * transaction with {@code lock.startTs <= caller.startTs}. The client
 * (or its lock resolver) decides whether to wait, abort, or proceed.
 */
public final class KeyLockedException extends RuntimeException {
    private final byte[] key;
    private final Lock lock;

    public KeyLockedException(byte[] key, Lock lock) {
        super("key locked: startTs=" + lock.startTs());
        this.key = key;
        this.lock = lock;
    }

    public byte[] key() { return key; }
    public Lock lock() { return lock; }
}
