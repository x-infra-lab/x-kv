package io.github.xinfra.lab.xkv.common.util;

import org.slf4j.Logger;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

public final class CloseUtils {

    private CloseUtils() {}

    public static void closeQuietly(Logger log, String label, AutoCloseable closeable) {
        if (closeable == null) return;
        try {
            closeable.close();
        } catch (Exception e) {
            log.warn("failed to close {}: {}", label, e.getMessage(), e);
        }
    }

    public static void shutdownQuietly(Logger log, String label,
                                       ExecutorService executor, long timeout, TimeUnit unit) {
        if (executor == null) return;
        executor.shutdownNow();
        try {
            if (!executor.awaitTermination(timeout, unit)) {
                log.warn("{} did not terminate within {} {}", label, timeout, unit);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            log.warn("{} shutdown interrupted", label);
        }
    }
}
