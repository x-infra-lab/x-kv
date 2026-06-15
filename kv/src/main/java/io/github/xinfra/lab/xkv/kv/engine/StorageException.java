package io.github.xinfra.lab.xkv.kv.engine;

import org.rocksdb.RocksDBException;
import org.rocksdb.Status;

public class StorageException extends RuntimeException {

    public StorageException(String message) {
        super(message);
    }

    public StorageException(String message, Throwable cause) {
        super(message, cause);
    }

    public static StorageException from(String operation, RocksDBException e) {
        Status status = e.getStatus();
        if (status != null) {
            return switch (status.getCode()) {
                case IOError -> new IOError(operation, e);
                case Corruption -> new Corruption(operation, e);
                default -> {
                    if (status.getSubCode() == Status.SubCode.NoSpace) {
                        yield new OutOfSpace(operation, e);
                    }
                    yield new StorageException(operation + " failed", e);
                }
            };
        }
        return new StorageException(operation + " failed", e);
    }

    public static final class IOError extends StorageException {
        public IOError(String operation, RocksDBException cause) {
            super(operation + " failed: I/O error", cause);
        }
    }

    public static final class Corruption extends StorageException {
        public Corruption(String operation, RocksDBException cause) {
            super(operation + " failed: data corruption", cause);
        }
    }

    public static final class OutOfSpace extends StorageException {
        public OutOfSpace(String operation, RocksDBException cause) {
            super(operation + " failed: no space left", cause);
        }
    }
}
