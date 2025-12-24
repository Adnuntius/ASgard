package org.asgard;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.knuddels.jtokkit.Encodings;
import com.knuddels.jtokkit.api.Encoding;
import com.knuddels.jtokkit.api.EncodingRegistry;
import com.knuddels.jtokkit.api.EncodingType;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;

public final class OpenAiClassifier {
    public static final int COMPLETION_TOKENS = 512;
    private static final EncodingRegistry ENCODING_REGISTRY = Encodings.newDefaultEncodingRegistry();
    private static final Encoding TOKENIZER = ENCODING_REGISTRY.getEncoding(EncodingType.CL100K_BASE);

    private final HttpClient httpClient;
    private final ObjectMapper objectMapper;
    private final URI baseUri;
    private final String model;
    private final String apiKey;
    private final Duration timeout;
    private final boolean verbose;
    private final OpenAiRequestLogger requestLogger;
    private final TokenRateLimiter rateLimiter;

    public record ClassificationResponse(FinalClassification classification, long approximatePromptTokens, String rawResponseBody) {
    }

    public OpenAiClassifier(HttpClient httpClient, ObjectMapper objectMapper, URI baseUri, String model,
                            String apiKey, Duration timeout) {
        this(httpClient, objectMapper, baseUri, model, apiKey, timeout, false, null, null);
    }

    public OpenAiClassifier(HttpClient httpClient, ObjectMapper objectMapper, URI baseUri, String model,
                            String apiKey, Duration timeout, boolean verbose) {
        this(httpClient, objectMapper, baseUri, model, apiKey, timeout, verbose, null, null);
    }

    public OpenAiClassifier(HttpClient httpClient, ObjectMapper objectMapper, URI baseUri, String model,
                            String apiKey, Duration timeout, boolean verbose, OpenAiRequestLogger requestLogger) {
        this(httpClient, objectMapper, baseUri, model, apiKey, timeout, verbose, requestLogger, null);
    }

    public OpenAiClassifier(HttpClient httpClient, ObjectMapper objectMapper, URI baseUri, String model,
                            String apiKey, Duration timeout, boolean verbose, OpenAiRequestLogger requestLogger,
                            TokenRateLimiter rateLimiter) {
        this.httpClient = httpClient;
        this.objectMapper = objectMapper;
        this.baseUri = baseUri.toString().endsWith("/") ? baseUri : URI.create(baseUri + "/");
        this.model = model;
        this.apiKey = apiKey;
        this.timeout = timeout;
        this.verbose = verbose;
        this.requestLogger = requestLogger;
        this.rateLimiter = rateLimiter;
    }

    private static final int MAX_RETRIES = 5;
    private static final long DEFAULT_RETRY_DELAY_MS = 5000;

    public ClassificationResponse classifyWithUsage(AsnMetadata metadata) throws IOException, InterruptedException {
        if (apiKey == null || apiKey.isBlank()) throw new IllegalStateException("OpenAI API key is missing");
        var summary = metadataSummary(metadata);
        var requestBody = buildRequest(summary);
        var approxTokens = (long) TOKENIZER.countTokens(requestBody);

        // Truncate if single request exceeds TPM or context limit
        if (rateLimiter != null) {
            final var maxTokensAllowed = Math.min(rateLimiter.tokensPerMinute(), rateLimiter.maxContextTokens());
            if (approxTokens > maxTokensAllowed) {
                final var originalTokens = approxTokens;
                // Iteratively truncate summary until under limit
                var targetChars = summary.length();
                while (approxTokens > maxTokensAllowed && targetChars > 100) {
                    targetChars = (int) (targetChars * 0.7); // Reduce by 30% each iteration
                    summary = truncateSummary(summary, targetChars);
                    requestBody = buildRequest(summary);
                    approxTokens = TOKENIZER.countTokens(requestBody);
                }
                System.err.printf("AS%d: Truncated request from %d to %d tokens (limit: %d)%n",
                        metadata.asn(), originalTokens, approxTokens, maxTokensAllowed);
            }
        }

        // Wait for rate limit capacity
        if (rateLimiter != null) {
            rateLimiter.waitForCapacity(approxTokens, metadata.asn());
        }

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
                final var delay = extractRetryAfterMs(response);
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

        final var classification = parseResponse(metadata, responseBody);
        if (verbose) {
            System.out.printf("\n=== Parsed Classification for AS%d ===%n", metadata.asn());
            System.out.printf("Name: %s%nOrganization: %s%nCategory: %s%n",
                    classification.name(), classification.organization(), classification.category());
        }
        if (requestLogger != null) {
            requestLogger.logResponse(metadata, response.statusCode(), responseBody, classification, approxTokens);
        }
        if (rateLimiter != null) {
            rateLimiter.recordTokens(approxTokens);
        }
        return new ClassificationResponse(classification, approxTokens, responseBody);
    }

    private static String truncateSummary(String summary, int maxChars) {
        if (summary.length() <= maxChars) return summary;
        // Truncate at last newline before maxChars to preserve field structure
        final var truncated = summary.substring(0, maxChars);
        final var lastNewline = truncated.lastIndexOf('\n');
        return (lastNewline > 0 ? truncated.substring(0, lastNewline) : truncated) + "\n[truncated]";
    }

    private static boolean isRetryableStatus(int status) {
        return status == 429 || status == 500 || status == 502 || status == 503 || status == 504;
    }

    private static long extractRetryAfterMs(HttpResponse<?> response) {
        if (response == null) return DEFAULT_RETRY_DELAY_MS;
        // Try Retry-After header first (standard HTTP header, value in seconds)
        final var retryAfter = response.headers().firstValue("Retry-After").orElse(null);
        if (retryAfter != null) {
            try {
                return (long) (Double.parseDouble(retryAfter) * 1000) + 500; // add 500ms buffer
            } catch (NumberFormatException ignored) {}
        }
        // Try OpenAI-specific rate limit reset headers (milliseconds until reset)
        final var resetTokens = response.headers().firstValue("x-ratelimit-reset-tokens").orElse(null);
        if (resetTokens != null) {
            final var ms = parseDurationToMs(resetTokens);
            if (ms > 0) return ms + 500;
        }
        final var resetRequests = response.headers().firstValue("x-ratelimit-reset-requests").orElse(null);
        if (resetRequests != null) {
            final var ms = parseDurationToMs(resetRequests);
            if (ms > 0) return ms + 500;
        }
        return DEFAULT_RETRY_DELAY_MS;
    }

    private static long parseDurationToMs(String duration) {
        // Parse durations like "1s", "500ms", "1m30s", "2m"
        if (duration == null || duration.isBlank()) return 0;
        long totalMs = 0;
        final var pattern = java.util.regex.Pattern.compile("(\\d+(?:\\.\\d+)?)(ms|s|m|h)");
        final var matcher = pattern.matcher(duration.toLowerCase());
        while (matcher.find()) {
            final var value = Double.parseDouble(matcher.group(1));
            totalMs += switch (matcher.group(2)) {
                case "ms" -> (long) value;
                case "s" -> (long) (value * 1000);
                case "m" -> (long) (value * 60_000);
                case "h" -> (long) (value * 3_600_000);
                default -> 0;
            };
        }
        return totalMs;
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
        final var content = root.path("choices").path(0).path("message").path("content").asText();
        if (content == null || content.isBlank()) {
            throw new IOException("Empty OpenAI response");
        }
        final var category = normalizeCategory(content);
        final var name = metadata.name() != null && !metadata.name().isBlank() ? metadata.name() : "Unknown";
        final var organization = metadata.entityForClassification().orElse("Unknown");
        return new FinalClassification(metadata.asn(), name, organization, category);
    }

    private String normalizeCategory(String content) {
        final var trimmed = content == null ? "" : content.trim();
        // Check if response is just the category name
        for (final var cat : Taxonomy.categories()) {
            if (cat.equalsIgnoreCase(trimmed)) return cat;
        }
        // Look for "ANSWER: CategoryName" pattern
        final var answerPattern = java.util.regex.Pattern.compile(
                "ANSWER:\\s*(\\w+)", java.util.regex.Pattern.CASE_INSENSITIVE);
        final var matcher = answerPattern.matcher(trimmed);
        if (matcher.find()) {
            final var extracted = matcher.group(1);
            for (final var cat : Taxonomy.categories()) {
                if (cat.equalsIgnoreCase(extracted)) return cat;
            }
        }
        // Fallback: find category with the last occurrence in the text
        String lastMatch = null;
        int lastPosition = -1;
        for (final var cat : Taxonomy.categories()) {
            final var wordPattern = java.util.regex.Pattern.compile(
                    "\\b" + cat + "\\b", java.util.regex.Pattern.CASE_INSENSITIVE);
            final var wordMatcher = wordPattern.matcher(trimmed);
            while (wordMatcher.find()) {
                if (wordMatcher.start() > lastPosition) {
                    lastPosition = wordMatcher.start();
                    lastMatch = cat;
                }
            }
        }
        return lastMatch != null ? lastMatch : "Unknown";
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
