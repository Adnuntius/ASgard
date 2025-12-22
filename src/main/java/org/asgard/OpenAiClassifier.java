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

    private static final int MAX_RETRIES = 5;
    private static final long INITIAL_RETRY_DELAY_MS = 2000;
    private static final long MAX_RETRY_DELAY_MS = 30000;
    private static final int EXTENDED_COMPLETION_TOKENS = 512;

    public ClassificationResponse classifyWithUsage(AsnMetadata metadata) throws IOException, InterruptedException {
        return classifyWithUsage(metadata, COMPLETION_TOKENS, false);
    }

    private ClassificationResponse classifyWithUsage(AsnMetadata metadata, int maxTokens, boolean isRetryWithMoreTokens)
            throws IOException, InterruptedException {
        if (apiKey == null || apiKey.isBlank()) throw new IllegalStateException("OpenAI API key is missing");
        final var summary = metadataSummary(metadata);
        final var requestBody = buildRequest(summary, maxTokens);
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

        HttpResponse<String> response = null;
        IOException lastException = null;
        for (int attempt = 0; attempt <= MAX_RETRIES; attempt++) {
            try {
                response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
                if (!isRetryableStatus(response.statusCode())) break;
                lastException = new IOException("OpenAI returned " + response.statusCode() + ": " + response.body());
            } catch (IOException ex) {
                lastException = ex;
            }
            if (attempt < MAX_RETRIES) {
                final var delay = Math.min(INITIAL_RETRY_DELAY_MS * (1L << attempt), MAX_RETRY_DELAY_MS);
                System.err.printf("AS%d: Retry %d/%d in %.1fs (%s)%n",
                        metadata.asn(), attempt + 1, MAX_RETRIES, delay / 1000.0,
                        lastException.getMessage().split("\n")[0]);
                Thread.sleep(delay);
            }
        }
        if (response == null || isRetryableStatus(response.statusCode())) {
            if (requestLogger != null && response != null) {
                requestLogger.logResponse(metadata, response.statusCode(), response.body(), null, approxTokens);
            }
            throw lastException != null ? lastException : new IOException("OpenAI request failed after retries");
        }

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

        // Check for truncation and retry with more tokens if needed
        final var parseResult = tryParseResponse(metadata, responseBody);
        if (parseResult.truncatedWithNoCategory && !isRetryWithMoreTokens) {
            System.err.printf("AS%d: Response truncated with no category found, retrying with %d tokens%n",
                    metadata.asn(), EXTENDED_COMPLETION_TOKENS);
            return classifyWithUsage(metadata, EXTENDED_COMPLETION_TOKENS, true);
        }
        if (parseResult.classification == null) {
            throw new IOException(parseResult.error);
        }

        final var classification = parseResult.classification;
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

    private static boolean isRetryableStatus(int status) {
        return status == 429 || status == 500 || status == 502 || status == 503 || status == 504;
    }

    public FinalClassification classify(AsnMetadata metadata) throws IOException, InterruptedException {
        return classifyWithUsage(metadata).classification();
    }

    private String buildRequest(String summary, int maxTokens) throws IOException {
        final var root = objectMapper.createObjectNode();
        root.put("model", model);
        root.put("max_completion_tokens", maxTokens);
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

    private record ParseResult(FinalClassification classification, boolean truncatedWithNoCategory, String error) {
        static ParseResult success(FinalClassification classification) {
            return new ParseResult(classification, false, null);
        }
        static ParseResult truncatedNoCategory(String error) {
            return new ParseResult(null, true, error);
        }
        static ParseResult failure(String error) {
            return new ParseResult(null, false, error);
        }
    }

    private ParseResult tryParseResponse(AsnMetadata metadata, String body) {
        try {
            final var root = objectMapper.readTree(body);
            final var firstChoice = root.path("choices").path(0);
            final var finishReason = firstChoice.path("finish_reason").asText();
            final var content = firstChoice.path("message").path("content").asText();
            final var truncated = "length".equals(finishReason);

            if (content == null || content.isBlank()) {
                return ParseResult.failure("Empty OpenAI response (finish_reason: " + finishReason + ")");
            }

            final var category = normalizeCategory(content);
            if (truncated && "Unknown".equals(category)) {
                return ParseResult.truncatedNoCategory("OpenAI response truncated and no category found in: " +
                        content.substring(0, Math.min(100, content.length())) + "...");
            }

            final var name = metadata.name() != null && !metadata.name().isBlank()
                    ? metadata.name() : "Unknown";
            final var organization = metadata.entityForClassification().orElse("Unknown");
            return ParseResult.success(new FinalClassification(metadata.asn(), name, organization, category));
        } catch (Exception e) {
            return ParseResult.failure("Failed to parse OpenAI response: " + e.getMessage());
        }
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
        // Extract category from common patterns like "Category: X", "Classification: X"
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
        // Match "- CategoryName:" pattern (model lists category with its definition)
        for (final var cat : Taxonomy.categories()) {
            final var catPattern = java.util.regex.Pattern.compile(
                    "(?:^|\\n)\\s*-\\s*" + cat + "\\s*:",
                    java.util.regex.Pattern.CASE_INSENSITIVE);
            if (catPattern.matcher(trimmed).find()) return cat;
        }
        // Fallback: find first category mentioned as a complete word in the first 100 chars
        final var prefix = trimmed.length() > 100 ? trimmed.substring(0, 100) : trimmed;
        for (final var cat : Taxonomy.categories()) {
            final var wordBoundary = java.util.regex.Pattern.compile(
                    "\\b" + cat + "\\b", java.util.regex.Pattern.CASE_INSENSITIVE);
            if (wordBoundary.matcher(prefix).find()) return cat;
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
