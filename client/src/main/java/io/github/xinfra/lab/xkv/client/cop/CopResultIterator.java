package io.github.xinfra.lab.xkv.client.cop;

import io.github.xinfra.lab.xkv.client.error.KvClientException;

import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ExecutionException;

final class CopResultIterator implements Iterator<CopClient.RegionCopResult> {

    private final CompletionService<CopClient.RegionCopResult> cs;
    private int remaining;

    CopResultIterator(CompletionService<CopClient.RegionCopResult> cs, int total) {
        this.cs = cs;
        this.remaining = total;
    }

    @Override
    public boolean hasNext() {
        return remaining > 0;
    }

    @Override
    public CopClient.RegionCopResult next() {
        if (remaining <= 0) {
            throw new NoSuchElementException();
        }
        try {
            CopClient.RegionCopResult result = cs.take().get();
            remaining--;
            return result;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new KvClientException(KvClientException.Category.OTHER,
                    "coprocessor request interrupted", e);
        } catch (ExecutionException e) {
            remaining--;
            Throwable cause = e.getCause();
            if (cause instanceof RuntimeException re) throw re;
            throw new KvClientException(KvClientException.Category.OTHER,
                    "coprocessor request failed: " + cause.getMessage(), cause);
        }
    }
}
