package org.asgard.registry;

import org.asgard.AsnMetadata;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.*;
import java.util.zip.GZIPInputStream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

public final class AsnBulkDump {
    private AsnBulkDump() {
    }

    public static Map<Long, List<DelegatedRecord>> parseDelegatedExtended(Path file) throws IOException {
        final Map<Long, List<DelegatedRecord>> byAsn = new HashMap<>();
        try (var lines = Files.lines(file, StandardCharsets.UTF_8)) {
            lines.filter(line -> !line.startsWith("#") && !line.isBlank())
                    .forEach(line -> parseDelegatedLine(line).ifPresent(record -> {
                        for (long asn = record.start(); asn < record.start() + record.count(); asn++) {
                            byAsn.computeIfAbsent(asn, k -> new ArrayList<>()).add(record);
                        }
                    }));
        }
        return Map.copyOf(byAsn);
    }

    private static Optional<DelegatedRecord> parseDelegatedLine(String line) {
        final var parts = line.split("\\|");
        if (parts.length < 7) return Optional.empty();
        if (!"asn".equalsIgnoreCase(parts[2])) return Optional.empty();
        final long start = parseLong(parts[3], -1);
        final long count = parseLong(parts[4], 0);
        if (start < 0 || count <= 0) return Optional.empty();
        final var registry = parts[0];
        final var country = parts[1];
        final var date = parts[5];
        final var status = parts[6];
        return Optional.of(new DelegatedRecord(start, count, registry, country, status, date, line));
    }

    public static List<RpslObject> iterRpslObjectsFromGz(Path gzFile) throws IOException {
        final var results = new ArrayList<RpslObject>();
        try (var in = new GZIPInputStream(Files.newInputStream(gzFile));
             var reader = new BufferedReader(new InputStreamReader(in, StandardCharsets.UTF_8))) {
            String line;
            final var raw = new StringBuilder();
            Map<String, List<String>> attrs = new LinkedHashMap<>();
            String lastKey = null;
            while ((line = reader.readLine()) != null) {
                if (line.isBlank()) {
                    finalizeObject(results, raw, attrs);
                    raw.setLength(0);
                    attrs = new LinkedHashMap<>();
                    lastKey = null;
                    continue;
                }
                raw.append(line).append('\n');
                if (line.startsWith(" ") || line.startsWith("\t")) {
                    if (lastKey != null) {
                        final var values = attrs.get(lastKey);
                        final var lastIndex = values.size() - 1;
                        final var merged = values.get(lastIndex) + " " + line.trim();
                        values.set(lastIndex, merged);
                    }
                    continue;
                }
                final var idx = line.indexOf(':');
                if (idx < 0) continue;
                final var key = line.substring(0, idx).trim().toLowerCase();
                final var value = line.substring(idx + 1).trim();
                attrs.computeIfAbsent(key, k -> new ArrayList<>()).add(value);
                lastKey = key;
            }
            finalizeObject(results, raw, attrs);
        }
        return List.copyOf(results);
    }

    private static void finalizeObject(List<RpslObject> results, StringBuilder raw, Map<String, List<String>> attrs) {
        if (raw.isEmpty() || attrs.isEmpty()) return;
        final var type = attrs.keySet().iterator().next();
        results.add(new RpslObject(type, Map.copyOf(attrs), raw.toString()));
    }

    public static ArinBulkData parseArinBulk(Path file) throws IOException {
        final Map<Long, Map<String, String>> asns = new HashMap<>();
        final Map<String, Map<String, String>> orgs = new HashMap<>();
        final var fileName = file.getFileName().toString().toLowerCase();
        try {
            if (fileName.endsWith(".zip")) {
                try (var zin = new ZipInputStream(Files.newInputStream(file))) {
                    ZipEntry entry;
                    while ((entry = zin.getNextEntry()) != null) {
                        if (!entry.getName().toLowerCase().endsWith(".xml")) continue;
                        System.out.printf("Parsing %s (streaming)...%n", entry.getName());
                        parseArinXmlStream(new NonClosingInputStream(zin), asns, orgs);
                    }
                }
            } else {
                System.out.printf("Parsing %s (streaming)...%n", file.getFileName());
                try (var in = Files.newInputStream(file)) {
                    parseArinXmlStream(in, asns, orgs);
                }
            }
        } catch (Exception ex) {
            throw new IOException("Failed to parse ARIN bulk file " + file, ex);
        }
        System.out.printf("Parsed %,d ASNs and %,d orgs from ARIN bulk data%n", asns.size(), orgs.size());
        return new ArinBulkData(Map.copyOf(asns), Map.copyOf(orgs));
    }

    private static void parseArinXmlStream(java.io.InputStream in,
                                           Map<Long, Map<String, String>> asns,
                                           Map<String, Map<String, String>> orgs) throws Exception {
        final var factory = javax.xml.stream.XMLInputFactory.newInstance();
        factory.setProperty(javax.xml.stream.XMLInputFactory.IS_SUPPORTING_EXTERNAL_ENTITIES, false);
        factory.setProperty(javax.xml.stream.XMLInputFactory.SUPPORT_DTD, false);
        final var reader = factory.createXMLStreamReader(in, StandardCharsets.UTF_8.name());
        String currentElement = null;
        Map<String, String> currentAttrs = null;
        String currentTag = null;
        final var textBuffer = new StringBuilder();

        while (reader.hasNext()) {
            final int event = reader.next();
            switch (event) {
                case javax.xml.stream.XMLStreamConstants.START_ELEMENT -> {
                    final var localName = reader.getLocalName();
                    if ("asn".equals(localName) || "org".equals(localName)) {
                        currentElement = localName;
                        currentAttrs = new HashMap<>();
                    } else if (currentElement != null) {
                        currentTag = localName;
                        textBuffer.setLength(0);
                    }
                }
                case javax.xml.stream.XMLStreamConstants.CHARACTERS, javax.xml.stream.XMLStreamConstants.CDATA -> {
                    if (currentTag != null) textBuffer.append(reader.getText());
                }
                case javax.xml.stream.XMLStreamConstants.END_ELEMENT -> {
                    final var localName = reader.getLocalName();
                    if (currentTag != null && currentTag.equals(localName) && currentAttrs != null) {
                        final var text = textBuffer.toString().trim();
                        if (!text.isEmpty()) currentAttrs.put(localName, text);
                        currentTag = null;
                    } else if ("asn".equals(localName) && currentAttrs != null) {
                        final long startAsn = parseLong(currentAttrs.get("startAsNumber"), -1);
                        if (startAsn > 0) {
                            final long endAsn = parseLong(currentAttrs.get("endAsNumber"), startAsn);
                            final var orgHandle = currentAttrs.get("orgHandle");
                            // Skip IANA reserved blocks (not real allocations)
                            if (!"IANA".equals(orgHandle)) {
                                final Map<String, String> values = new HashMap<>();
                                if (orgHandle != null) values.put("orghandle", orgHandle);
                                if (currentAttrs.containsKey("name")) values.put("name", currentAttrs.get("name"));
                                final var immutableValues = Map.copyOf(values);
                                final long rangeSize = endAsn - startAsn + 1;
                                // Limit range expansion to avoid OOM on huge reserved blocks
                                if (rangeSize <= 1000) {
                                    for (long asn = startAsn; asn <= endAsn; asn++) {
                                        asns.put(asn, immutableValues);
                                    }
                                } else {
                                    // For large ranges, only store the first ASN as a marker
                                    asns.put(startAsn, immutableValues);
                                }
                            }
                        }
                        currentElement = null;
                        currentAttrs = null;
                    } else if ("org".equals(localName) && currentAttrs != null) {
                        final var handle = currentAttrs.get("handle");
                        if (handle != null) {
                            final Map<String, String> values = new HashMap<>();
                            values.put("handle", handle);
                            if (currentAttrs.containsKey("name")) values.put("name", currentAttrs.get("name"));
                            orgs.put(handle, Map.copyOf(values));
                        }
                        currentElement = null;
                        currentAttrs = null;
                    }
                }
            }
        }
        // Don't close reader - it would close the underlying ZipInputStream
    }

    public static List<AsnMetadata> assembleMetadata(Map<Long, List<DelegatedRecord>> delegated,
                                                     Map<Long, RpslObject> rpslByAsn,
                                                     ArinBulkData arinData) {
        final var results = new ArrayList<AsnMetadata>();
        final var allAsns = new java.util.TreeSet<Long>();
        allAsns.addAll(delegated.keySet());
        allAsns.addAll(rpslByAsn.keySet());
        allAsns.addAll(arinData.asns().keySet());
        for (var asn : allAsns) {
            results.add(buildMetadata(asn,
                    delegated.getOrDefault(asn, List.of()).isEmpty() ? null : delegated.get(asn).get(0),
                    rpslByAsn.get(asn),
                    arinData));
        }
        return results;
    }

    private static AsnMetadata buildMetadata(long asn,
                                             DelegatedRecord delegated,
                                             RpslObject rpsl,
                                             ArinBulkData arinData) {
        final var arinAsnData = arinData.asns().getOrDefault(asn, Map.of());
        final var name = firstNonBlank(attrFirst(rpsl, "as-name"), attrFirst(rpsl, "descr"),
                arinAsnData.get("name"), "Unknown");
        final var country = delegated != null ? delegated.country() : null;
        final var registry = delegated != null ? delegated.registry() : null;
        final var status = delegated != null ? delegated.status() : null;
        final var statuses = status == null || status.isBlank() ? List.<String>of() : List.of(status);
        final var allocationDate = delegated != null ? delegated.allocationDate() : null;
        final var orgHandle = firstNonBlank(attrFirst(rpsl, "org"), attrFirst(rpsl, "org-name"),
                attrFirst(rpsl, "org-hdl"), arinAsnData.get("orghandle"));
        final var orgName = orgHandle == null ? null : arinData.orgs().getOrDefault(orgHandle, Map.of()).get("name");
        final var organization = firstNonBlank(orgName, orgHandle, name, "Unknown");
        final var entity = new AsnMetadata.Entity(orgHandle, organization, organization, null, null,
                List.of(), List.of(), List.of(), List.of(), List.of());
        final var remarks = new ArrayList<String>();
        if (delegated != null && delegated.raw() != null) remarks.add("delegated: " + delegated.raw());
        if (rpsl != null && rpsl.raw() != null) remarks.add("rpsl: " + rpsl.raw().replace('\n', ' '));
        return new AsnMetadata(asn,
                attrFirst(rpsl, "aut-num"),
                asn,
                asn,
                name,
                country,
                registry,
                null,
                statuses,
                allocationDate,
                null,
                entity,
                List.of(),
                remarks,
                Instant.now());
    }

    private static String attrFirst(RpslObject obj, String key) {
        if (obj == null) return null;
        final var values = obj.attrs().get(key.toLowerCase());
        if (values == null || values.isEmpty()) return null;
        return values.get(0);
    }

    private static String firstNonBlank(String... values) {
        for (var value : values) {
            if (value != null && !value.isBlank()) return value.trim();
        }
        return null;
    }

    private static long parseLong(String value, long fallback) {
        try {
            return Long.parseLong(value);
        } catch (Exception ex) {
            return fallback;
        }
    }

    private static class NonClosingInputStream extends java.io.FilterInputStream {
        NonClosingInputStream(java.io.InputStream in) { super(in); }
        @Override public void close() { /* don't close */ }
    }

    public record DelegatedRecord(long start, long count, String registry, String country, String status,
                                  String allocationDate, String raw) {
    }

    public record RpslObject(String objType, Map<String, List<String>> attrs, String raw) {
    }

    public record ArinBulkData(Map<Long, Map<String, String>> asns, Map<String, Map<String, String>> orgs) {
        public static ArinBulkData empty() {
            return new ArinBulkData(Map.of(), Map.of());
        }
    }
}
