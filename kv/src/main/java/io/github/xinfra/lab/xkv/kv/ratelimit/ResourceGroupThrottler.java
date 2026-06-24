package io.github.xinfra.lab.xkv.kv.ratelimit;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

public final class ResourceGroupThrottler {

    public static final String DEFAULT_GROUP = "default";

    private final ConcurrentHashMap<String, TokenBucket> buckets = new ConcurrentHashMap<>();

    public void updateSettings(String groupName, long fillRate, long burstLimit) {
        if (DEFAULT_GROUP.equals(groupName) && fillRate == 0 && burstLimit == 0) {
            buckets.remove(groupName);
            return;
        }
        buckets.compute(groupName, (k, existing) -> {
            if (existing != null) {
                existing.update(fillRate, burstLimit);
                return existing;
            }
            return new TokenBucket(fillRate, burstLimit);
        });
    }

    public void removeGroup(String groupName) {
        buckets.remove(groupName);
    }

    public boolean tryConsume(String groupName, long cost) {
        if (groupName == null || groupName.isEmpty()) {
            groupName = DEFAULT_GROUP;
        }
        var bucket = buckets.get(groupName);
        if (bucket == null) {
            return true;
        }
        return bucket.tryConsume(cost);
    }

    public int groupCount() {
        return buckets.size();
    }

    static final class TokenBucket {
        private volatile long fillRate;
        private volatile long burstLimit;
        private final AtomicLong tokens;
        private volatile long lastRefillNanos;

        TokenBucket(long fillRate, long burstLimit) {
            this.fillRate = fillRate;
            this.burstLimit = burstLimit;
            this.tokens = new AtomicLong(burstLimit);
            this.lastRefillNanos = System.nanoTime();
        }

        void update(long newFillRate, long newBurstLimit) {
            this.fillRate = newFillRate;
            this.burstLimit = newBurstLimit;
        }

        boolean tryConsume(long cost) {
            refill();
            long current = tokens.get();
            while (current >= cost) {
                if (tokens.compareAndSet(current, current - cost)) {
                    return true;
                }
                current = tokens.get();
            }
            return false;
        }

        long availableTokens() {
            refill();
            return tokens.get();
        }

        private void refill() {
            long now = System.nanoTime();
            long elapsed = now - lastRefillNanos;
            if (elapsed <= 0) return;

            long rate = fillRate;
            if (rate <= 0) return;

            long newTokens = (elapsed * rate) / 1_000_000_000L;
            if (newTokens <= 0) return;

            lastRefillNanos = now;
            long limit = burstLimit;
            tokens.updateAndGet(current -> Math.min(current + newTokens, limit));
        }
    }
}
