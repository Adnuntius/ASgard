package org.asgard;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;

public final class ConfigManager {
    private final Path configFile;

    public ConfigManager(Path configFile) {
        this.configFile = configFile;
    }

    public Config loadOrCreate() {
        if (!Files.exists(configFile)) {
            final var config = Config.defaultConfig();
            write(config);
            return config;
        }
        try {
            final var values = parse(Files.readAllLines(configFile, StandardCharsets.UTF_8));
            final var model = values.getOrDefault("model", Config.defaultConfig().model());
            final var ttlHours = parseLong(values.get("registryTtlHours"), Config.defaultConfig().registryTtl().toHours());
            return new Config(model, Duration.ofHours(ttlHours));
        } catch (IOException ex) {
            final var config = Config.defaultConfig();
            write(config);
            return config;
        }
    }

    private Map<String, String> parse(Iterable<String> lines) {
        final var map = new HashMap<String, String>();
        for (final var line : lines) {
            if (line == null) continue;
            final var trimmed = line.trim();
            if (trimmed.isEmpty() || trimmed.startsWith("#")) continue;
            final var parts = trimmed.split("=", 2);
            if (parts.length == 2) map.put(parts[0].trim(), parts[1].trim());
        }
        return map;
    }

    private long parseLong(String value, long fallback) {
        if (value == null || value.isBlank()) return fallback;
        try {
            return Long.parseLong(value);
        } catch (NumberFormatException ex) {
            return fallback;
        }
    }

    private void write(Config config) {
        var content = """
                # asgard.conf
                model=%s
                registryTtlHours=%d
                """.formatted(config.model(), config.registryTtl().toHours());
        try {
            Files.writeString(configFile, content, StandardCharsets.UTF_8,
                    java.nio.file.StandardOpenOption.CREATE, java.nio.file.StandardOpenOption.TRUNCATE_EXISTING);
        } catch (IOException ex) {
            throw new IllegalStateException("Failed to write config to " + configFile, ex);
        }
    }
}
