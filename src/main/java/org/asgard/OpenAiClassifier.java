package org.asgard;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;

public final class OpenAiClassifier {
    public static final int COMPLETION_TOKENS = 256;
    private final HttpClient httpClient;
    private final ObjectMapper objectMapper;
    private final URI baseUri;
    private final String model;
    private final String apiKey;
    private final Duration timeout;
    private final boolean verbose;
    private final OpenAiRequestLogger requestLogger;

    public record ClassificationResponse(FinalClassification classification, long approximatePromptTokens, String rawResponseBody) {
    }

    public OpenAiClassifier(HttpClient httpClient, ObjectMapper objectMapper, URI baseUri, String model,
                            String apiKey, Duration timeout) {
        this(httpClient, objectMapper, baseUri, model, apiKey, timeout, false, null);
    }

    public OpenAiClassifier(HttpClient httpClient, ObjectMapper objectMapper, URI baseUri, String model,
                            String apiKey, Duration timeout, boolean verbose) {
        this(httpClient, objectMapper, baseUri, model, apiKey, timeout, verbose, null);
    }

    public OpenAiClassifier(HttpClient httpClient, ObjectMapper objectMapper, URI baseUri, String model,
                            String apiKey, Duration timeout, boolean verbose, OpenAiRequestLogger requestLogger) {
        this.httpClient = httpClient;
        this.objectMapper = objectMapper;
        this.baseUri = baseUri.toString().endsWith("/") ? baseUri : URI.create(baseUri + "/");
        this.model = model;
        this.apiKey = apiKey;
        this.timeout = timeout;
        this.verbose = verbose;
        this.requestLogger = requestLogger;
    }

    public ClassificationResponse classifyWithUsage(AsnMetadata metadata) throws IOException, InterruptedException {
        if (apiKey == null || apiKey.isBlank()) throw new IllegalStateException("OpenAI API key is missing");
        final var summary = metadataSummary(metadata);
        final var requestBody = buildRequest(summary);
        if (verbose) {
            System.out.println("\n=== OpenAI Request for AS" + metadata.asn() + " ===");
            System.out.println(requestBody);
        }
        if (requestLogger != null) {
            requestLogger.logRequest(metadata, summary, requestBody);
        }
        final var request = HttpRequest.newBuilder(baseUri.resolve("chat/completions"))
                .timeout(timeout)
                .header("Content-Type", "application/json")
                .header("Authorization", "Bearer " + apiKey)
                .POST(HttpRequest.BodyPublishers.ofString(requestBody, StandardCharsets.UTF_8))
                .build();
        final var approxTokens = Math.round(requestBody.length() / 4.0);
        final var response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
        final var responseBody = response.body();
        if (verbose) {
            System.out.printf("\n=== OpenAI Response for AS%d (HTTP %d) ===%n", metadata.asn(), response.statusCode());
            System.out.println(responseBody);
        }
        if (response.statusCode() >= 300) {
            if (requestLogger != null) {
                requestLogger.logResponse(metadata, response.statusCode(), responseBody, null, approxTokens);
            }
            throw new IOException("OpenAI classification failed: " + response.statusCode() + " " + responseBody);
        }
        final var classification = parseResponse(metadata, responseBody);
        if (verbose) {
            System.out.printf("\n=== Parsed Classification for AS%d ===%n", metadata.asn());
            System.out.printf("Name: %s%nOrganization: %s%nCategory: %s%n",
                    classification.name(), classification.organization(), classification.category());
        }
        if (requestLogger != null) {
            requestLogger.logResponse(metadata, response.statusCode(), responseBody, classification, approxTokens);
        }
        return new ClassificationResponse(classification, approxTokens, responseBody);
    }

    public FinalClassification classify(AsnMetadata metadata) throws IOException, InterruptedException {
        return classifyWithUsage(metadata).classification();
    }

    private String buildRequest(String summary) throws IOException {
        final var root = objectMapper.createObjectNode();
        root.put("model", model);
        root.put("max_completion_tokens", COMPLETION_TOKENS);
        root.put("reasoning_effort", "minimal");
        final ArrayNode messages = root.putArray("messages");
        messages.add(message("system", Taxonomy.prompt()));
        messages.add(message("user", summary));
        return objectMapper.writeValueAsString(root);
    }

    private String metadataSummary(AsnMetadata metadata) {
        final var seen = new java.util.HashSet<String>();
        final var builder = new StringBuilder();
        append(builder, "ASN", String.valueOf(metadata.asn()), seen);
        append(builder, "Handle", metadata.handle(), seen);
        append(builder, "Name", metadata.name(), seen);
        append(builder, "Status", join(metadata.statuses()), seen);
        append(builder, "Registered", registrationYear(metadata.registrationDate()), seen);
        append(builder, "Registry", metadata.registry(), seen);
        append(builder, "Country", metadata.country(), seen);
        append(builder, "Kind", metadata.type(), seen);
        for (final var entity : allEntities(metadata)) {
            append(builder, "Organization", entity.organization(), seen);
            append(builder, "Entity", entity.name(), seen);
            append(builder, "Address", entity.address(), seen);
            append(builder, "Kind", entity.kind(), seen);
            append(builder, "Emails", join(entity.emails()), seen);
            if (!entity.remarks().isEmpty()) {
                entity.remarks().forEach(r -> append(builder, "Remark", r, seen));
            }
        }
        if (!metadata.remarks().isEmpty()) {
            metadata.remarks().forEach(r -> append(builder, "Remark", r, seen));
        }
        return builder.toString().trim();
    }

    private boolean include(String value, java.util.Set<String> seen) {
        if (value == null || value.isBlank()) return false;
        final var normalized = value.trim().toLowerCase();
        return seen.add(normalized);
    }

    private ObjectNode message(String role, String content) {
        final var node = objectMapper.createObjectNode();
        node.put("role", role);
        node.put("content", content);
        return node;
    }

    private FinalClassification parseResponse(AsnMetadata metadata, String body) throws IOException {
        final var root = objectMapper.readTree(body);
        final var firstChoice = root.path("choices").path(0);
        final var finishReason = firstChoice.path("finish_reason").asText();
        final var content = firstChoice.path("message").path("content").asText();
        if ("length".equals(finishReason)) {
            throw new IOException("OpenAI response truncated");
        } else if (content == null || content.isBlank()) {
            throw new IOException("Empty OpenAI response (finish_reason: " + finishReason + ")");
        }
        final var category = normalizeCategory(content);
        final var name = metadata.name() != null && !metadata.name().isBlank()
                ? metadata.name() : "Unknown";
        final var organization = metadata.entityForClassification().orElse("Unknown");
        return new FinalClassification(metadata.asn(), name, organization, category);
    }

    private String normalizeCategory(String content) {
        final var trimmed = content == null ? "" : content.trim();
        if (Taxonomy.categories().contains(trimmed)) return trimmed;
        // try to parse json with category field if model returns structured output
        try {
            final var node = objectMapper.readTree(trimmed);
            final var extracted = node.path("category").asText();
            if (Taxonomy.categories().contains(extracted)) return extracted;
        } catch (Exception ignored) {
        }
        // Extract category from common patterns like "Category: X", "Classification: X", or "- Category: X"
        final var categoryPattern = java.util.regex.Pattern.compile(
                "(?:^|\\n)\\s*-?\\s*(?:Category|Classification):\\s*(\\w+)",
                java.util.regex.Pattern.CASE_INSENSITIVE);
        final var matcher = categoryPattern.matcher(trimmed);
        if (matcher.find()) {
            final var extracted = matcher.group(1);
            for (final var cat : Taxonomy.categories()) {
                if (cat.equalsIgnoreCase(extracted)) return cat;
            }
        }
        // Fallback: check if content starts with or prominently contains a category name
        // Only match category as a word boundary to avoid false matches in reasoning text
        for (final var cat : Taxonomy.categories()) {
            final var wordBoundary = java.util.regex.Pattern.compile(
                    "\\b" + cat + "\\b", java.util.regex.Pattern.CASE_INSENSITIVE);
            final var wordMatcher = wordBoundary.matcher(trimmed);
            if (wordMatcher.find() && wordMatcher.start() < 50) {
                // Only accept if the match appears near the start (likely the classification, not reasoning)
                return cat;
            }
        }
        return "Unknown";
    }

    private void append(StringBuilder builder, String label, String value, java.util.Set<String> seen) {
        if (!include(value, seen)) return;
        builder.append(label).append(": ").append(value.trim()).append('\n');
    }

    private java.util.List<AsnMetadata.Entity> allEntities(AsnMetadata metadata) {
        final var list = new java.util.ArrayList<AsnMetadata.Entity>();
        if (metadata.registrant() != null) list.add(metadata.registrant());
        list.addAll(metadata.contacts());
        return list;
    }

    private String join(java.util.List<String> values) {
        if (values == null || values.isEmpty()) return null;
        return String.join(", ", values);
    }

    private String registrationYear(String registrationDate) {
        if (registrationDate == null || registrationDate.isBlank()) return null;
        return registrationDate.length() >= 4 ? registrationDate.substring(0, 4) : registrationDate;
    }

}
