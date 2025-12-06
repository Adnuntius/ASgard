package org.asgard;

import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.net.http.HttpClient;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.*;

import static java.nio.file.StandardOpenOption.*;

public final class AsnClassifierApp {
    private static final String HEADER = "asn\tname\torganization\tcategory";

    private AsnClassifierApp() {
    }

    public static void main(String[] args) throws Exception {
        final var options = CliOptions.parse(args);
        if (options.apiKey() == null || options.apiKey().isBlank()) {
            System.err.println("Missing OpenAI API key. Set OPENAI_API_KEY or pass --api-key=<token>.");
            System.exit(1);
        }
        final var mapper = ObjectMapperFactory.create();
        final var httpClient = HttpClient.newBuilder()
                .connectTimeout(Duration.ofSeconds(10))
                .followRedirects(HttpClient.Redirect.NORMAL)
                .build();

        final var state = new StatePaths(options.stateDir());
        state.ensureDirectories();
        final var config = new ConfigManager(state.configFile()).loadOrCreate();

        final var modelToUse = config.model();
        System.out.printf("Model: %s | RDAP: %s | OpenAI: %s | State: %s%n",
                modelToUse, options.rdapBaseUri(), options.openAiBaseUri(), state.baseDir());

        final var registryClient = new AsnRegistryClient(httpClient, options.registrySources(), options.rdapTimeout());
        final var allocationCache = new AllocationCache(registryClient, mapper, state.allocationsDir());
        final var rdapClient = new RdapClient(httpClient, mapper, options.rdapBaseUri(), options.rdapTimeout());
        final var isReprocessing = !options.reprocessAsns().isEmpty();
        final var classifier = new OpenAiClassifier(httpClient, mapper, options.openAiBaseUri(), modelToUse,
                options.apiKey(), options.openAiTimeout(), isReprocessing);

        final var allocations = allocationCache.loadAllocations(config).stream()
                .sorted(Comparator.comparingLong(AsnAllocation::startAsn))
                .toList();
        if (allocations.isEmpty()) throw new IllegalStateException("No ASN allocations downloaded");
        System.out.printf("Fetched %,d allocation blocks (cache TTL %d hours)%n",
                allocations.size(), config.registryTtl().toHours());

        final var outputFile = state.outputFile(options.outputFile());
        if (outputFile.getParent() != null) Files.createDirectories(outputFile.getParent());
        normalizeOutput(outputFile, mapper);

        if (isReprocessing) {
            System.out.printf("Reprocessing %d ASN(s): %s%n",
                    options.reprocessAsns().size(), options.reprocessAsns());
            removeAsnsFromOutput(outputFile, mapper, options.reprocessAsns());
            try (var writer = Files.newBufferedWriter(outputFile, StandardCharsets.UTF_8, APPEND, CREATE)) {
                reprocessSpecificAsns(options.reprocessAsns(), rdapClient, classifier, writer);
            }
            System.out.println("Classification complete -> " + outputFile);
            return;
        }

        final var processedLoad = loadProcessed(outputFile, mapper);
        final var processed = processedLoad.processed();
        if (!processed.isEmpty()) System.out.printf("Skipping %,d ASNs already classified%n", processed.size());
        if (!processedLoad.unknowns().isEmpty()) {
            logUnknowns(processedLoad.unknowns().size(), processedLoad.unknownSamples(), "entity/category");
            rewriteKnownEntries(outputFile, mapper, processed);
        }

        final var startAt = firstMissing(allocations, processed);
        if (startAt < 0) {
            System.out.println("Nothing new to classify.");
            return;
        }
        final var totalToProcess = countPending(allocations, processed, options.limit(), startAt);
        if (totalToProcess == 0) {
            System.out.println("Nothing new to classify.");
            return;
        }
        System.out.printf("Preparing to classify %,d ASNs starting at AS%d%n", totalToProcess, startAt);

        try (var writer = Files.newBufferedWriter(outputFile, StandardCharsets.UTF_8, APPEND, CREATE)) {
            runPipeline(allocations, processed, options.limit(), rdapClient, classifier, writer, totalToProcess, startAt);
        }
        System.out.println("Classification complete -> " + outputFile);
    }

    private static void runPipeline(
            java.util.List<AsnAllocation> allocations,
            Set<Long> processed,
            long limit,
            RdapClient rdapClient,
            OpenAiClassifier classifier,
            java.io.BufferedWriter writer,
            long totalToProcess,
            long startAt) throws IOException {
        final var summaryEvery = Math.max(1, totalToProcess / 1000);
        long processedCount = 0;
        long tokenApprox = 0;
        final Iterator<Long> iterator = pendingIterator(allocations, processed, limit, startAt);
        while (iterator.hasNext()) {
            final var asn = iterator.next();
            final var metadata = rdapClient.lookup(asn);
            FinalClassification output;
            if (metadata.isEmpty()) {
                System.err.printf("No RDAP record for AS%d (base %s, fallback rdap.org)%n",
                        asn, rdapClient.baseUri());
                output = new FinalClassification(asn, "Unknown", "Unknown", "Unknown");
            } else {
                try {
                    final var response = classifier.classifyWithUsage(metadata.get());
                    tokenApprox += response.approximatePromptTokens();
                    output = response.classification();
                } catch (InterruptedException ex) {
                    Thread.currentThread().interrupt();
                    throw new IOException("Interrupted while classifying AS" + asn, ex);
                } catch (Exception ex) {
                    System.err.printf("Classification failed for AS%d: %s%n", asn, ex.getMessage());
                    throw ex;
                }
            }
            if (isUnknown(output.name()) || isUnknown(output.organization()) || isUnknown(output.category())) {
                System.err.printf("Skipping write for AS%d due to Unknown fields%n", asn);
                continue;
            }
            writer.write(toSeparatedLine(output));
            writer.newLine();
            processed.add(asn);
            processedCount++;
            if (processedCount % summaryEvery == 0 || processedCount == totalToProcess) {
                final var percent = (processedCount * 100.0) / totalToProcess;
                System.out.printf("Progress: %.2f%% (%d/%d) | ~%d total tokens sent%n", percent, processedCount, totalToProcess, tokenApprox);
            }
        }
    }

    private static long countPending(java.util.List<AsnAllocation> allocations, Set<Long> processed, long limit,
                                     long startAt) {
        final var remainingLimit = limit > 0 ? limit : Long.MAX_VALUE;
        final var rest = iteratePending(allocations, processed, remainingLimit, false, null, startAt);
        return rest;
    }

    private static Iterator<Long> pendingIterator(java.util.List<AsnAllocation> allocations,
                                                  Set<Long> processed,
                                                  long limit,
                                                  long startAt) {
        return new Iterator<>() {
            private final Iterator<AsnAllocation> allocationIterator = allocations.iterator();
            private AsnAllocation current;
            private long cursor;
            private long remaining = limit > 0 ? limit : Long.MAX_VALUE;
            private Long nextValue;
            private boolean positioned;

            @Override
            public boolean hasNext() {
                if (remaining == 0) return false;
                if (nextValue != null) return true;
                if (!positioned) {
                    seekToStart();
                    positioned = true;
                }
                while (remaining > 0) {
                    if (current == null) {
                        if (!allocationIterator.hasNext()) return false;
                        current = allocationIterator.next();
                        cursor = current.startAsn();
                    }
                    if (cursor > current.endAsn()) {
                        current = null;
                        continue;
                    }
                    var candidate = cursor++;
                    if (processed.contains(candidate)) continue;
                    nextValue = candidate;
                    return true;
                }
                return false;
            }

            @Override
            public Long next() {
                if (!hasNext()) throw new java.util.NoSuchElementException();
                var value = nextValue;
                nextValue = null;
                remaining--;
                return value;
            }

            private void seekToStart() {
                while (allocationIterator.hasNext()) {
                    current = allocationIterator.next();
                    if (current.endAsn() < startAt) continue;
                    cursor = Math.max(current.startAsn(), startAt);
                    return;
                }
            }
        };
    }

    private static long iteratePending(java.util.List<AsnAllocation> allocations,
                                       Set<Long> processed,
                                       long limit,
                                       boolean collect,
                                       java.util.ArrayDeque<Long> queue,
                                       long startAt) {
        long remaining = limit > 0 ? limit : Long.MAX_VALUE;
        long counted = 0;
        boolean started = false;
        for (var allocation : allocations) {
            if (!started && allocation.endAsn() < startAt) continue;
            final var start = !started ? Math.max(allocation.startAsn(), startAt) : allocation.startAsn();
            started = true;
            for (long asn = start; asn <= allocation.endAsn() && remaining > 0; asn++) {
                if (processed.contains(asn)) continue;
                counted++;
                if (collect) queue.add(asn);
                remaining--;
                if (remaining == 0) break;
            }
        }
        return counted;
    }

    private static ProcessedLoad loadProcessed(Path outputFile, ObjectMapper mapper) {
        if (!Files.exists(outputFile)) {
            return new ProcessedLoad(new HashSet<>(), new java.util.LinkedHashSet<>(), List.of());
        }
        final var processed = new HashSet<Long>();
        final var unknownSet = new java.util.LinkedHashSet<Long>();
        try (var reader = Files.newBufferedReader(outputFile, StandardCharsets.UTF_8)) {
            var firstLine = true;
            String line;
            while ((line = reader.readLine()) != null) {
                if (line.isBlank()) continue;
                if (firstLine && isHeader(line)) {
                    firstLine = false;
                    continue;
                }
                firstLine = false;
                final var classification = parseClassification(line, mapper);
                if (classification == null) continue;
                final var asn = classification.asn();
                if (asn <= 0) continue;
                if (isUnknown(classification.name()) || isUnknown(classification.organization())
                        || isUnknown(classification.category())) {
                    unknownSet.add(asn);
                } else {
                    processed.add(asn);
                }
            }
        } catch (IOException ex) {
            System.err.println("Failed to read prior output; reprocessing all ASNs. " + ex.getMessage());
            processed.clear();
        }
        return new ProcessedLoad(processed, unknownSet, sample(unknownSet));
    }

    private static void logUnknowns(long count, List<Long> samples, String label) {
        if (count <= 0) return;
        final var details = samples.isEmpty() ? "" : " (e.g., " + String.join(", ",
                samples.stream().map(String::valueOf).toList())
                + (count > samples.size() ? ", ..." : "") + ")";
        System.out.printf("Will retry %,d ASNs with %s Unknown%s%n", count, label, details);
    }

    private static FinalClassification parseClassification(String line, ObjectMapper mapper) {
        final var trimmed = line.trim();
        if (trimmed.isEmpty() || isHeader(trimmed)) return null;
        if (trimmed.startsWith("{")) {
            try {
                final var node = mapper.readTree(trimmed);
                final var asn = node.path("asn").asLong(-1);
                final var name = node.path("name").asText(null);
                final var organization = node.path("organization").asText(null);
                final var category = node.path("category").asText(null);
                final var entity = node.path("entity").asText(null);
                final var actualName = name != null ? name : entity;
                final var actualOrg = organization != null ? organization : entity;
                return new FinalClassification(asn, actualName, actualOrg, category);
            } catch (IOException ignored) {
                return null;
            }
        }
        final var delimiter = trimmed.contains("\t") ? '\t' : ',';
        final var parts = parseSeparated(trimmed, delimiter);
        if (parts.size() == 4) {
            try {
                return new FinalClassification(Long.parseLong(parts.get(0)), parts.get(1), parts.get(2), parts.get(3));
            } catch (NumberFormatException ex) {
                return null;
            }
        } else if (parts.size() == 3) {
            try {
                return new FinalClassification(Long.parseLong(parts.get(0)), parts.get(1), parts.get(1), parts.get(2));
            } catch (NumberFormatException ex) {
                return null;
            }
        }
        return null;
    }

    private static List<String> parseSeparated(String line, char delimiter) {
        final var parts = new ArrayList<String>(3);
        final var current = new StringBuilder();
        var escaping = false;
        for (var i = 0; i < line.length(); i++) {
            final var ch = line.charAt(i);
            if (escaping) {
                current.append(ch == 't' ? '\t' : ch);
                escaping = false;
            } else if (ch == '\\') {
                escaping = true;
            } else if (ch == delimiter) {
                parts.add(current.toString());
                current.setLength(0);
            } else {
                current.append(ch);
            }
        }
        parts.add(current.toString());
        return parts;
    }

    private static String toSeparatedLine(FinalClassification classification) {
        final var name = escapeSeparated(classification.name());
        final var organization = escapeSeparated(classification.organization());
        final var category = escapeSeparated(classification.category());
        return classification.asn() + "\t" + name + "\t" + organization + "\t" + category;
    }

    private static String escapeSeparated(String value) {
        if (value == null) return "";
        final var cleaned = value.replace("\r", " ").replace("\n", " ");
        return cleaned.replace("\\", "\\\\").replace("\t", "\\t");
    }

    private static boolean isHeader(String line) {
        final var trimmed = line.trim();
        return HEADER.equalsIgnoreCase(trimmed)
                || "asn,name,organization,category".equalsIgnoreCase(trimmed)
                || "asn\tentity\tcategory".equalsIgnoreCase(trimmed)
                || "asn,entity,category".equalsIgnoreCase(trimmed);
    }

    private static java.util.ArrayDeque<Long> forcedUnknownQueue(ProcessedLoad load) {
        final var combined = new java.util.ArrayDeque<Long>();
        load.unknowns().stream().sorted().forEach(combined::add);
        return combined;
    }

    private static void normalizeOutput(Path outputFile, ObjectMapper mapper) throws IOException {
        if (!Files.exists(outputFile) || Files.size(outputFile) == 0) {
            Files.writeString(outputFile, HEADER + System.lineSeparator(), StandardCharsets.UTF_8, CREATE,
                    TRUNCATE_EXISTING);
            return;
        }
        String firstNonBlank = null;
        try (var reader = Files.newBufferedReader(outputFile, StandardCharsets.UTF_8)) {
            String line;
            while ((line = reader.readLine()) != null) {
                if (line.isBlank()) continue;
                firstNonBlank = line;
                break;
            }
        }
        if (firstNonBlank != null && isHeader(firstNonBlank)) return;

        final var tempFile = Files.createTempFile(outputFile.getParent(), "asn-classifications", ".tsv");
        try (var reader = Files.newBufferedReader(outputFile, StandardCharsets.UTF_8);
             var writer = Files.newBufferedWriter(tempFile, StandardCharsets.UTF_8, TRUNCATE_EXISTING)) {
            writer.write(HEADER);
            writer.newLine();
            String line;
            while ((line = reader.readLine()) != null) {
                final var classification = parseClassification(line, mapper);
                if (classification == null) continue;
                writer.write(toSeparatedLine(classification));
                writer.newLine();
            }
        }
        Files.move(tempFile, outputFile, java.nio.file.StandardCopyOption.REPLACE_EXISTING);
    }

    private record ProcessedLoad(Set<Long> processed,
                                 java.util.Set<Long> unknowns,
                                 List<Long> unknownSamples) {
    }

    private static List<Long> sample(java.util.LinkedHashSet<Long> values) {
        final List<Long> sample = new ArrayList<>(Math.min(values.size(), 20));
        var i = 0;
        for (final var value : values) {
            if (i++ >= 20) break;
            sample.add(value);
        }
        return List.copyOf(sample);
    }

    private static long firstMissing(java.util.List<AsnAllocation> allocations, Set<Long> processed) {
        for (var allocation : allocations) {
            for (long asn = allocation.startAsn(); asn <= allocation.endAsn(); asn++) {
                if (!processed.contains(asn)) return asn;
            }
        }
        return -1;
    }

    private static void rewriteKnownEntries(Path outputFile,
                                            ObjectMapper mapper,
                                            Set<Long> processed) throws IOException {
        final var tempFile = Files.createTempFile(outputFile.getParent(), "asn-classifications-known", ".tsv");
        try (var reader = Files.newBufferedReader(outputFile, StandardCharsets.UTF_8);
             var writer = Files.newBufferedWriter(tempFile, StandardCharsets.UTF_8, TRUNCATE_EXISTING)) {
            writer.write(HEADER);
            writer.newLine();
            String line;
            while ((line = reader.readLine()) != null) {
                if (line.isBlank() || isHeader(line)) continue;
                final var classification = parseClassification(line, mapper);
                if (classification == null) continue;
                if (!processed.contains(classification.asn())) continue;
                writer.write(toSeparatedLine(classification));
                writer.newLine();
            }
        }
        Files.move(tempFile, outputFile, java.nio.file.StandardCopyOption.REPLACE_EXISTING);
    }

    private static boolean isUnknown(String value) {
        if (value == null) return true;
        final var trimmed = value.trim();
        return trimmed.isBlank() || "unknown".equalsIgnoreCase(trimmed);
    }

    private static void removeAsnsFromOutput(Path outputFile, ObjectMapper mapper, List<Long> asnsToRemove)
            throws IOException {
        if (!Files.exists(outputFile)) return;
        final var asnSet = new HashSet<>(asnsToRemove);
        final var tempFile = Files.createTempFile(outputFile.getParent(), "asn-classifications-reprocess", ".tsv");
        try (var reader = Files.newBufferedReader(outputFile, StandardCharsets.UTF_8);
             var writer = Files.newBufferedWriter(tempFile, StandardCharsets.UTF_8, TRUNCATE_EXISTING)) {
            writer.write(HEADER);
            writer.newLine();
            String line;
            while ((line = reader.readLine()) != null) {
                if (line.isBlank() || isHeader(line)) continue;
                final var classification = parseClassification(line, mapper);
                if (classification == null) continue;
                if (asnSet.contains(classification.asn())) continue;
                writer.write(toSeparatedLine(classification));
                writer.newLine();
            }
        }
        Files.move(tempFile, outputFile, java.nio.file.StandardCopyOption.REPLACE_EXISTING);
    }

    private static void reprocessSpecificAsns(List<Long> asns, RdapClient rdapClient, OpenAiClassifier classifier,
                                              java.io.BufferedWriter writer) throws IOException {
        for (final var asn : asns) {
            final var metadata = rdapClient.lookup(asn);
            FinalClassification output;
            if (metadata.isEmpty()) {
                System.err.printf("No RDAP record for AS%d (base %s, fallback rdap.org)%n",
                        asn, rdapClient.baseUri());
                output = new FinalClassification(asn, "Unknown", "Unknown", "Unknown");
            } else {
                try {
                    final var response = classifier.classifyWithUsage(metadata.get());
                    output = response.classification();
                } catch (InterruptedException ex) {
                    Thread.currentThread().interrupt();
                    throw new IOException("Interrupted while classifying AS" + asn, ex);
                } catch (Exception ex) {
                    System.err.printf("Classification failed for AS%d: %s%n", asn, ex.getMessage());
                    throw ex;
                }
            }
            if (isUnknown(output.name()) || isUnknown(output.organization()) || isUnknown(output.category())) {
                System.err.printf("Skipping write for AS%d due to Unknown fields%n", asn);
                continue;
            }
            writer.write(toSeparatedLine(output));
            writer.newLine();
        }
    }
}
