package org.asgard;

public record Config(String model, String arinApiKey) {
    private static final String DEFAULT_MODEL = "gpt-5-nano";

    public static Config defaultConfig() {
        return new Config(DEFAULT_MODEL, null);
    }
}
