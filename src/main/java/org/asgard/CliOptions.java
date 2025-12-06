package org.asgard;

import java.net.URI;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

public record CliOptions(List<URI> registrySources, URI rdapBaseUri, URI openAiBaseUri, String openAiModel,
                         String apiKey, long limit, Path outputFile, Duration rdapTimeout,
                         Duration openAiTimeout, Path stateDir, List<Long> reprocessAsns) {

    public static CliOptions parse(String[] args) {
        final var parsed = parseArgs(args);
        final var registry = sources(parsed.get("registry-sources"), System.getenv("ASGARD_REGISTRY_SOURCES"));
        final var rdapBase = URI.create(parsed.getOrDefault("rdap-base", System.getenv().getOrDefault("RDAP_BASE_URL",
                "https://rdap.org/")));
        final var openAiBase = URI.create(parsed.getOrDefault("openai-base", System.getenv().getOrDefault("OPENAI_BASE_URL",
                "https://api.openai.com/v1/")));
        final var model = parsed.getOrDefault("model", "gpt-5-nano");
        final var apiKey = parsed.getOrDefault("api-key", System.getenv("OPENAI_API_KEY"));
        final var limit = parseLong(parsed.get("limit"), 50);
        final var output = parsed.containsKey("output") ? Path.of(parsed.get("output")) : null;
        final var rdapTimeout = Duration.ofSeconds(parseLong(parsed.get("rdap-timeout-seconds"), 10));
        final var openAiTimeout = Duration.ofSeconds(parseLong(parsed.get("openai-timeout-seconds"), 30));
        final var stateDir = Path.of(parsed.getOrDefault("state-dir",
                System.getenv().getOrDefault("ASGARD_STATE_DIR",
                        System.getProperty("user.home") + "/.asgard")));
        final var reprocessAsns = parseAsnList(parsed.get("reprocess"));
        return new CliOptions(registry, rdapBase, openAiBase, model, apiKey, limit, output, rdapTimeout,
                openAiTimeout, stateDir, reprocessAsns);
    }

    private static Map<String, String> parseArgs(String[] args) {
        final var values = new java.util.HashMap<String, String>();
        Arrays.stream(args)
                .filter(arg -> arg.startsWith("--"))
                .map(arg -> arg.substring(2))
                .forEach(token -> {
                    final var parts = token.split("=", 2);
                    if (parts.length == 2) values.put(parts[0], parts[1]);
                });
        return values;
    }

    private static List<URI> sources(String argValue, String envValue) {
        final var values = argValue != null ? argValue : envValue;
        if (values == null || values.isBlank()) return AsnRegistryClient.defaultSources();
        final var result = new ArrayList<URI>();
        for (final var token : values.split(",")) {
            result.add(URI.create(token.trim()));
        }
        return result;
    }

    private static long parseLong(String candidate, long fallback) {
        if (candidate == null || candidate.isBlank()) return fallback;
        try {
            return Long.parseLong(candidate.trim());
        } catch (NumberFormatException ex) {
            return fallback;
        }
    }

    private static List<Long> parseAsnList(String value) {
        if (value == null || value.isBlank()) return List.of();
        final var result = new ArrayList<Long>();
        for (final var token : value.split(",")) {
            final var trimmed = token.trim();
            if (trimmed.isEmpty()) continue;
            try {
                result.add(Long.parseLong(trimmed));
            } catch (NumberFormatException ex) {
                System.err.println("Warning: Invalid ASN in reprocess list: " + trimmed);
            }
        }
        return List.copyOf(result);
    }
}
