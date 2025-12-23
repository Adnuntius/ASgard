package org.asgard;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class TokenRateLimiterTest {

    @Test
    void recordsAndTracksTokens() throws InterruptedException {
        final var limiter = new TokenRateLimiter(1000);
        limiter.recordTokens(100);
        limiter.recordTokens(200);
        // Should not wait since we're under limit
        final var start = System.currentTimeMillis();
        limiter.waitForCapacity(500, 1);
        final var elapsed = System.currentTimeMillis() - start;
        assertThat(elapsed).isLessThan(100);
    }

    @Test
    void tokensPerMinuteReturnsConfiguredLimit() {
        final var limiter = new TokenRateLimiter(200_000);
        assertThat(limiter.tokensPerMinute()).isEqualTo(200_000);
    }

    @Test
    void maxContextTokensReturnsConfiguredLimit() {
        final var limiter = new TokenRateLimiter(200_000, 150_000);
        assertThat(limiter.maxContextTokens()).isEqualTo(150_000);
    }

    @Test
    void defaultMaxContextTokensIs250k() {
        final var limiter = new TokenRateLimiter(100_000);
        assertThat(limiter.maxContextTokens()).isEqualTo(250_000);
    }
}
