package org.asgard;

import java.time.Duration;

public record Config(String model, Duration registryTtl) {
    private static final String DEFAULT_MODEL = "gpt-5-nano";
    private static final Duration DEFAULT_TTL = Duration.ofDays(7);

    public static Config defaultConfig() {
        return new Config(DEFAULT_MODEL, DEFAULT_TTL);
    }
}
