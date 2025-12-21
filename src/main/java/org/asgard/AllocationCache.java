package org.asgard;

import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
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

    public List<AsnAllocation> loadAllocations(boolean refresh) throws IOException, InterruptedException {
        Files.createDirectories(allocationsDir);
        final var latest = newestFile();
        if (latest.isPresent() && !refresh) {
            return readAllocations(latest.get());
        }
        if (latest.isEmpty()) {
            System.out.println("No cached allocations found, downloading fresh copy.");
        } else if (refresh) {
            System.out.println("Allocation refresh requested, downloading new data.");
        }
        final var allocations = registryClient.fetchAllocations();
        final var file = writeAllocations(allocations);
        System.out.println("Wrote allocation cache " + file);
        return allocations;
    }

    private Optional<Path> newestFile() throws IOException {
        try (var stream = Files.list(allocationsDir)) {
            return stream.filter(p -> p.getFileName().toString().startsWith("allocations-"))
                    .max(Comparator.comparingLong(p -> p.toFile().lastModified()));
        }
    }

    private List<AsnAllocation> readAllocations(Path file) throws IOException {
        final var allocations = new ArrayList<AsnAllocation>();
        try (var reader = Files.newBufferedReader(file, StandardCharsets.UTF_8)) {
            String line;
            while ((line = reader.readLine()) != null) {
                final var node = mapper.readTree(line);
                allocations.add(mapper.treeToValue(node, AsnAllocation.class));
            }
        }
        System.out.println("Loaded cached allocations from " + file);
        return allocations;
    }

    private Path writeAllocations(List<AsnAllocation> allocations) throws IOException {
        final var filename = "allocations-" + DateTimeFormatter.ofPattern("yyyyMMdd'T'HHmmss'Z'")
                .withZone(java.time.ZoneOffset.UTC)
                .format(Instant.now()) + ".ndjson";
        final var file = allocationsDir.resolve(filename);
        try (var writer = Files.newBufferedWriter(file, StandardCharsets.UTF_8)) {
            for (var allocation : allocations) {
                writer.write(mapper.writeValueAsString(allocation));
                writer.newLine();
            }
        }
        return file;
    }
}
