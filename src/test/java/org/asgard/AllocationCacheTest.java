package org.asgard;

import com.fasterxml.jackson.databind.ObjectMapper;
import okhttp3.mockwebserver.MockResponse;
import okhttp3.mockwebserver.MockWebServer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.net.http.HttpClient;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.LocalDate;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class AllocationCacheTest {
    private static final ObjectMapper MAPPER = ObjectMapperFactory.create();

    @TempDir
    Path tempDir;

    @Test
    void usesCachedAllocationsWhenRefreshNotRequested() throws Exception {
        final var cached = List.of(new AsnAllocation(65000, 1, "arin", "US", "allocated",
                LocalDate.of(2024, 1, 1)));
        writeCache(cached);

        try (var server = new MockWebServer()) {
            server.enqueue(registryResponse("arin|US|asn|65010|1|20240101|allocated"));
            server.start();

            final var cache = new AllocationCache(clientFor(server), MAPPER, tempDir);
            final var allocations = cache.loadAllocations(false);

            assertThat(allocations).containsExactlyElementsOf(cached);
            assertThat(server.getRequestCount()).isZero();
        }
    }

    @Test
    void refreshDownloadsLatestAllocationsEvenWhenCacheExists() throws Exception {
        final var cached = List.of(new AsnAllocation(65000, 1, "arin", "US", "allocated",
                LocalDate.of(2024, 1, 1)));
        writeCache(cached);

        try (var server = new MockWebServer()) {
            server.enqueue(registryResponse("""
                    arin|US|asn|65010|2|20240101|allocated
                    """));
            server.start();

            final var cache = new AllocationCache(clientFor(server), MAPPER, tempDir);
            final var allocations = cache.loadAllocations(true);

            assertThat(server.getRequestCount()).isEqualTo(1);
            assertThat(allocations).hasSize(1);
            assertThat(allocations.get(0).startAsn()).isEqualTo(65010);
            assertThat(latestCacheFile()).isPresent();
        }
    }

    @Test
    void downloadsAllocationsWhenCacheMissing() throws Exception {
        try (var server = new MockWebServer()) {
            server.enqueue(registryResponse("""
                    arin|US|asn|65020|1|20240101|allocated
                    """));
            server.start();

            final var cache = new AllocationCache(clientFor(server), MAPPER, tempDir);
            final var allocations = cache.loadAllocations(false);

            assertThat(server.getRequestCount()).isEqualTo(1);
            assertThat(allocations.get(0).startAsn()).isEqualTo(65020);
            assertThat(latestCacheFile()).isPresent();
        }
    }

    private MockResponse registryResponse(String body) {
        return new MockResponse().setBody(body).addHeader("Content-Type", "text/plain");
    }

    private AsnRegistryClient clientFor(MockWebServer server) {
        return new AsnRegistryClient(HttpClient.newHttpClient(),
                List.of(server.url("/delegated").uri()), Duration.ofSeconds(2));
    }

    private void writeCache(List<AsnAllocation> allocations) throws Exception {
        final var file = tempDir.resolve("allocations-primed.ndjson");
        try (var writer = Files.newBufferedWriter(file, StandardCharsets.UTF_8)) {
            for (final var allocation : allocations) {
                writer.write(MAPPER.writeValueAsString(allocation));
                writer.newLine();
            }
        }
    }

    private java.util.Optional<Path> latestCacheFile() throws Exception {
        try (var files = Files.list(tempDir)) {
            return files.filter(path -> path.getFileName().toString().startsWith("allocations-")).findFirst();
        }
    }
}
