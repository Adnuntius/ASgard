package org.asgard;

import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;

public final class AllocationCache {
    private final AsnRegistryClient registryClient;
    private final ObjectMapper mapper;
    private final Path allocationsDir;

    public AllocationCache(AsnRegistryClient registryClient, ObjectMapper mapper, Path allocationsDir) {
        this.registryClient = registryClient;
        this.mapper = mapper;
        this.allocationsDir = allocationsDir;
    }

    public List<AsnAllocation> loadAllocations(Config config) throws IOException, InterruptedException {
        Files.createDirectories(allocationsDir);
        var latest = newestFile();
        if (latest.isPresent() && !isStale(latest.get(), config.registryTtl())) {
            return readAllocations(latest.get());
        }
        var allocations = registryClient.fetchAllocations();
        var file = writeAllocations(allocations);
        System.out.println("Wrote allocation cache " + file);
        return allocations;
    }

    private Optional<Path> newestFile() throws IOException {
        try (var stream = Files.list(allocationsDir)) {
            return stream.filter(p -> p.getFileName().toString().startsWith("allocations-"))
                    .max(Comparator.comparingLong(p -> p.toFile().lastModified()));
        }
    }

    private boolean isStale(Path file, Duration ttl) throws IOException {
        var age = Duration.between(Files.getLastModifiedTime(file).toInstant(), Instant.now());
        return age.compareTo(ttl) > 0;
    }

    private List<AsnAllocation> readAllocations(Path file) throws IOException {
        var allocations = new ArrayList<AsnAllocation>();
        try (var reader = Files.newBufferedReader(file, StandardCharsets.UTF_8)) {
            String line;
            while ((line = reader.readLine()) != null) {
                var node = mapper.readTree(line);
                allocations.add(mapper.treeToValue(node, AsnAllocation.class));
            }
        }
        System.out.println("Loaded cached allocations from " + file);
        return allocations;
    }

    private Path writeAllocations(List<AsnAllocation> allocations) throws IOException {
        var filename = "allocations-" + DateTimeFormatter.ofPattern("yyyyMMdd'T'HHmmss'Z'")
                .withZone(java.time.ZoneOffset.UTC)
                .format(Instant.now()) + ".ndjson";
        var file = allocationsDir.resolve(filename);
        try (var writer = Files.newBufferedWriter(file, StandardCharsets.UTF_8)) {
            for (var allocation : allocations) {
                writer.write(mapper.writeValueAsString(allocation));
                writer.newLine();
            }
        }
        return file;
    }
}
