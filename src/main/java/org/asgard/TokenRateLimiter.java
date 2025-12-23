package org.asgard;

import java.util.ArrayDeque;

public final class TokenRateLimiter {
    private static final long WINDOW_MS = 60_000;
    private final long tokensPerMinute;
    private final long maxContextTokens;
    private final ArrayDeque<TokenRecord> window = new ArrayDeque<>();

    private record TokenRecord(long timestampMs, long tokens) {}

    public TokenRateLimiter(long tokensPerMinute) {
        this(tokensPerMinute, 250_000); // Default max context (under typical 272k limit)
    }

    public TokenRateLimiter(long tokensPerMinute, long maxContextTokens) {
        this.tokensPerMinute = tokensPerMinute;
        this.maxContextTokens = maxContextTokens;
    }

    public synchronized void waitForCapacity(long estimatedTokens, long asn) throws InterruptedException {
        pruneExpired();
        final long currentUsage = currentWindowTokens();
        final long needed = estimatedTokens - (tokensPerMinute - currentUsage);

        if (needed <= 0) return; // Have capacity

        // Find how long to wait until enough tokens expire
        final var waitMs = calculateWaitTime(needed);
        if (waitMs <= 0) return;

        System.err.printf("AS%d: Rate limit (%,d + %,d > %,d TPM), waiting %.1fs%n",
                asn, currentUsage, estimatedTokens, tokensPerMinute, waitMs / 1000.0);
        Thread.sleep(waitMs);
        pruneExpired();
    }

    private long calculateWaitTime(long tokensNeeded) {
        if (window.isEmpty()) return 0;
        final var now = System.currentTimeMillis();
        long accumulated = 0;
        for (final var record : window) {
            accumulated += record.tokens;
            if (accumulated >= tokensNeeded) {
                // This record's expiry will free enough tokens
                return Math.max(0, (record.timestampMs + WINDOW_MS) - now + 100);
            }
        }
        // Need to wait for all records to expire
        final var last = ((TokenRecord) window.toArray()[window.size() - 1]);
        return Math.max(0, (last.timestampMs + WINDOW_MS) - now + 100);
    }

    public synchronized void recordTokens(long tokens) {
        window.addLast(new TokenRecord(System.currentTimeMillis(), tokens));
    }

    public long tokensPerMinute() {
        return tokensPerMinute;
    }

    public long maxContextTokens() {
        return maxContextTokens;
    }

    private void pruneExpired() {
        final var cutoff = System.currentTimeMillis() - WINDOW_MS;
        while (!window.isEmpty() && window.peekFirst().timestampMs < cutoff) {
            window.pollFirst();
        }
    }

    private long currentWindowTokens() {
        return window.stream().mapToLong(TokenRecord::tokens).sum();
    }
}
