package org.asgard;

import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public final class OpenAiRequestLogger {
    private final Path logDir;
    private final ObjectMapper mapper;
    private final Map<Long, AsnLogEntry> pending = new ConcurrentHashMap<>();

    public OpenAiRequestLogger(Path logDir, ObjectMapper mapper) {
        this.logDir = logDir;
        this.mapper = mapper;
    }

    public void logRequest(AsnMetadata metadata, String summary, String requestBody) {
        pending.put(metadata.asn(), new AsnLogEntry(
                Instant.now(), metadata, summary, requestBody, null, 0, null, 0));
    }

    public void logResponse(AsnMetadata metadata, int status, String responseBody,
                            FinalClassification classification, long approxTokens) {
        final var existing = pending.remove(metadata.asn());
        final var entry = existing != null
                ? new AsnLogEntry(existing.requestTime(), existing.metadata(), existing.summary(),
                existing.requestBody(), Instant.now(), status, responseBody, approxTokens)
                : new AsnLogEntry(null, metadata, null, null, Instant.now(), status, responseBody, approxTokens);
        writeAsnLog(metadata.asn(), entry, classification);
    }

    private void writeAsnLog(long asn, AsnLogEntry entry, FinalClassification classification) {
        try {
            Files.createDirectories(logDir);
            final var logFile = logDir.resolve("asn-" + asn + ".json");
            final var root = mapper.createArrayNode();

            // Request entry
            if (entry.requestTime() != null) {
                final var reqNode = mapper.createObjectNode();
                reqNode.put("timestamp", entry.requestTime().toString());
                reqNode.put("event", "request");
                reqNode.put("asn", asn);
                reqNode.set("metadata", mapper.valueToTree(entry.metadata()));
                reqNode.put("summary", entry.summary());
                reqNode.put("requestBody", entry.requestBody());
                root.add(reqNode);
            }

            // Response entry
            if (entry.responseTime() != null) {
                final var respNode = mapper.createObjectNode();
                respNode.put("timestamp", entry.responseTime().toString());
                respNode.put("event", "response");
                respNode.put("asn", asn);
                respNode.put("status", entry.status());
                respNode.put("responseBody", entry.responseBody());
                respNode.put("approxPromptTokens", entry.approxTokens());
                if (classification != null) {
                    respNode.put("name", classification.name());
                    respNode.put("organization", classification.organization());
                    respNode.put("category", classification.category());
                }
                root.add(respNode);
            }

            Files.writeString(logFile, mapper.writerWithDefaultPrettyPrinter().writeValueAsString(root),
                    StandardCharsets.UTF_8);
        } catch (IOException ex) {
            System.err.println("Failed to write OpenAI log for ASN " + asn + ": " + ex.getMessage());
        }
    }

    private record AsnLogEntry(Instant requestTime, AsnMetadata metadata, String summary, String requestBody,
                               Instant responseTime, int status, String responseBody, long approxTokens) {
    }
}
