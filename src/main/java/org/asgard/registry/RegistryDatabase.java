package org.asgard.registry;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.asgard.AsnMetadata;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

public final class RegistryDatabase {
    private final Path databaseFile;
    private final Map<Long, AsnMetadata> byAsn;

    public RegistryDatabase(Path databaseFile, ObjectMapper mapper) {
        this.databaseFile = databaseFile;
        this.byAsn = load(databaseFile, mapper);
    }

    public Optional<AsnMetadata> lookup(long asn) {
        return Optional.ofNullable(byAsn.get(asn));
    }

    public boolean isEmpty() {
        return byAsn.isEmpty();
    }

    private Map<Long, AsnMetadata> load(Path file, ObjectMapper mapper) {
        if (!Files.exists(file)) {
            System.out.printf("Registry cache missing at %s; no preloaded ASN details available.%n", file);
            return Map.of();
        }
        final var results = new HashMap<Long, AsnMetadata>();
        try (var reader = Files.newBufferedReader(file, StandardCharsets.UTF_8)) {
            String line;
            while ((line = reader.readLine()) != null) {
                if (line.isBlank() || line.startsWith("#")) continue;
                final var node = mapper.readTree(line);
                final var metadata = mapper.treeToValue(node, AsnMetadata.class);
                results.put(metadata.asn(), metadata);
            }
            System.out.printf("Loaded %,d cached registry entries from %s%n", results.size(), file);
            return Map.copyOf(results);
        } catch (IOException ex) {
            throw new IllegalStateException("Failed to load registry cache at " + file, ex);
        }
    }
}
