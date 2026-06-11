package io.github.xinfra.lab.xkv.client.error;

/**
 * Root unchecked exception for client-side failures. Sub-classes carry
 * machine-readable categorization so callers can branch without parsing
 * messages — that was a v1 footgun ({@code e.getMessage().contains("write
 * conflict")} pattern in BankTransferTxnTest).
 */
public class KvClientException extends RuntimeException {

    private final Category category;

    public KvClientException(Category category, String message) {
        super(message);
        this.category = category;
    }

    public KvClientException(Category category, String message, Throwable cause) {
        super(message, cause);
        this.category = category;
    }

    public Category category() { return category; }

    public enum Category {
        WRITE_CONFLICT,
        KEY_LOCKED,            // briefly; resolver will retry
        DEADLOCK,
        ALREADY_EXIST,
        TXN_NOT_FOUND,
        COMMIT_TS_EXPIRED,
        COMMIT_TS_TOO_LARGE,
        ASSERTION_FAILED,
        NOT_LEADER,
        EPOCH_NOT_MATCH,
        REGION_NOT_FOUND,
        SERVER_BUSY,
        DISK_FULL,
        NETWORK,
        BACKOFF_EXCEEDED,
        UNKNOWN_COMMIT_STATE,  // the v1 third-state fix; caller should NOT
                               // retry the txn — a background resolver will
                               // determine the actual outcome.
        OTHER
    }
}
