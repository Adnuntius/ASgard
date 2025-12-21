package org.asgard;

import java.nio.file.Files;
import java.nio.file.Path;

public final class StatePaths {
    private final Path baseDir;

    public StatePaths(Path baseDir) {
        this.baseDir = baseDir;
    }

    public Path baseDir() {
        return baseDir;
    }

    public Path configFile() {
        return baseDir.resolve("asgard.conf");
    }

    public Path cacheDir() {
        return baseDir.resolve("cache");
    }

    public Path allocationsDir() {
        return cacheDir().resolve("allocations");
    }

    public Path registryCacheDir() {
        return cacheDir().resolve("registry");
    }

    public Path registryDatabaseFile() {
        return registryCacheDir().resolve("registry-cache.ndjson");
    }

    public Path logsDir() {
        return baseDir.resolve("logs");
    }

    public Path outputFile(Path override) {
        return override != null ? override : baseDir.resolve("asn-classifications.tsv");
    }

    public void ensureDirectories() {
        try {
            Files.createDirectories(baseDir);
            Files.createDirectories(cacheDir());
            Files.createDirectories(allocationsDir());
            Files.createDirectories(registryCacheDir());
            Files.createDirectories(logsDir());
        } catch (Exception ex) {
            throw new IllegalStateException("Unable to initialize state directories under " + baseDir, ex);
        }
    }
}
