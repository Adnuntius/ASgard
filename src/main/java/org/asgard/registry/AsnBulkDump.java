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
        final Map<String, ArinOrg> orgs = new HashMap<>();
        final var fileName = file.getFileName().toString().toLowerCase();
        try {
            if (fileName.endsWith(".zip")) {
                try (var zin = new ZipInputStream(Files.newInputStream(file))) {
                    ZipEntry entry;
                    while ((entry = zin.getNextEntry()) != null) {
                        if (!entry.getName().toLowerCase().endsWith(".xml")) continue;
                        System.out.printf("Parsing %s (streaming)...%n", entry.getName());
                        parseArinXmlStream(new NonClosingInputStream(zin), asns, orgs, null);
                    }
                }
            } else {
                System.out.printf("Parsing %s (streaming)...%n", file.getFileName());
                try (var in = Files.newInputStream(file)) {
                    parseArinXmlStream(in, asns, orgs, null);
                }
            }
        } catch (Exception ex) {
            throw new IOException("Failed to parse ARIN bulk file " + file, ex);
        }
        System.out.printf("Parsed %,d ASNs and %,d orgs from ARIN bulk data%n", asns.size(), orgs.size());
        return new ArinBulkData(Map.copyOf(asns), Map.copyOf(orgs));
    }

    /**
     * Three-pass parsing: parse ASNs to collect orgHandles, parse orgs to collect pocHandles,
     * then parse POCs to get email domains for classification.
     */
    public static ArinBulkData parseArinBulkThreePass(Path asnsFile, Path orgsFile, Path pocsFile) throws IOException {
        final Map<Long, Map<String, String>> asns = new HashMap<>();
        final Set<String> referencedOrgHandles = new HashSet<>();

        // Pass 1: Parse ASNs and collect orgHandles
        System.out.printf("Pass 1: Parsing %s for ASNs and org references...%n", asnsFile.getFileName());
        try (var in = Files.newInputStream(asnsFile)) {
            parseArinAsnsCollectingOrgHandles(in, asns, referencedOrgHandles);
        } catch (Exception ex) {
            throw new IOException("Failed to parse ARIN ASNs file " + asnsFile, ex);
        }
        System.out.printf("Parsed %,d ASNs referencing %,d unique orgs%n", asns.size(), referencedOrgHandles.size());

        // Pass 2: Parse orgs, filtering to only referenced ones, collect POC handles
        final Map<String, ArinOrg> orgs = new HashMap<>();
        final Set<String> referencedPocHandles = new HashSet<>();
        System.out.printf("Pass 2: Parsing %s for referenced orgs...%n", orgsFile.getFileName());
        try (var in = Files.newInputStream(orgsFile)) {
            parseArinOrgsFiltered(in, orgs, referencedOrgHandles, referencedPocHandles);
        } catch (Exception ex) {
            throw new IOException("Failed to parse ARIN orgs file " + orgsFile, ex);
        }
        System.out.printf("Loaded %,d orgs referencing %,d unique POCs%n", orgs.size(), referencedPocHandles.size());

        // Pass 3: Parse POCs, filtering to only referenced ones, extract emails
        final Map<String, List<String>> emailsByPoc = new HashMap<>();
        System.out.printf("Pass 3: Parsing %s for referenced POC emails...%n", pocsFile.getFileName());
        try (var in = Files.newInputStream(pocsFile)) {
            parseArinPocsFiltered(in, emailsByPoc, referencedPocHandles);
        } catch (Exception ex) {
            throw new IOException("Failed to parse ARIN POCs file " + pocsFile, ex);
        }
        System.out.printf("Loaded emails for %,d POCs%n", emailsByPoc.size());

        // Build org-to-pocHandles mapping to attach email domains to orgs
        final Map<String, Set<String>> orgToPocs = buildOrgToPocMapping(orgsFile, referencedOrgHandles);

        // Attach email domains to each org
        final Map<String, ArinOrg> orgsWithEmails = new HashMap<>();
        for (var entry : orgs.entrySet()) {
            final var orgHandle = entry.getKey();
            final var org = entry.getValue();
            final var pocHandles = orgToPocs.getOrDefault(orgHandle, Set.of());
            final var domains = new HashSet<String>();
            for (var pocHandle : pocHandles) {
                final var emails = emailsByPoc.get(pocHandle);
                if (emails != null) {
                    for (var email : emails) {
                        final var domain = extractDomain(email);
                        if (domain != null) domains.add(domain.toLowerCase());
                    }
                }
            }
            orgsWithEmails.put(orgHandle, org.withEmailDomains(List.copyOf(domains)));
        }

        return new ArinBulkData(Map.copyOf(asns), Map.copyOf(orgsWithEmails));
    }

    private static Map<String, Set<String>> buildOrgToPocMapping(Path orgsFile, Set<String> allowedOrgHandles) throws IOException {
        final Map<String, Set<String>> result = new HashMap<>();
        try (var in = Files.newInputStream(orgsFile)) {
            final var factory = javax.xml.stream.XMLInputFactory.newInstance();
            factory.setProperty(javax.xml.stream.XMLInputFactory.IS_SUPPORTING_EXTERNAL_ENTITIES, false);
            factory.setProperty(javax.xml.stream.XMLInputFactory.SUPPORT_DTD, false);
            final var reader = factory.createXMLStreamReader(in, StandardCharsets.UTF_8.name());
            String currentElement = null;
            String currentHandle = null;
            Set<String> currentPocs = null;
            String currentTag = null;
            final var textBuffer = new StringBuilder();

            while (reader.hasNext()) {
                final int event = reader.next();
                switch (event) {
                    case javax.xml.stream.XMLStreamConstants.START_ELEMENT -> {
                        final var localName = reader.getLocalName();
                        if ("org".equals(localName)) {
                            currentElement = localName;
                            currentHandle = null;
                            currentPocs = new HashSet<>();
                        } else if (currentElement != null) {
                            currentTag = localName;
                            textBuffer.setLength(0);
                            // pocLinkRef has handle as XML attribute
                            if ("pocLinkRef".equals(localName) && currentPocs != null) {
                                final var handleAttr = reader.getAttributeValue(null, "handle");
                                if (handleAttr != null && !handleAttr.isBlank()) {
                                    currentPocs.add(handleAttr);
                                }
                            }
                        }
                    }
                    case javax.xml.stream.XMLStreamConstants.CHARACTERS, javax.xml.stream.XMLStreamConstants.CDATA -> {
                        if (currentTag != null) textBuffer.append(reader.getText());
                    }
                    case javax.xml.stream.XMLStreamConstants.END_ELEMENT -> {
                        final var localName = reader.getLocalName();
                        if (currentTag != null && currentTag.equals(localName)) {
                            final var text = textBuffer.toString().trim();
                            if (!text.isEmpty()) {
                                if ("handle".equals(localName)) currentHandle = text;
                            }
                            currentTag = null;
                        } else if ("org".equals(localName)) {
                            if (currentHandle != null && allowedOrgHandles.contains(currentHandle) && currentPocs != null) {
                                result.put(currentHandle, currentPocs);
                            }
                            currentElement = null;
                            currentHandle = null;
                            currentPocs = null;
                        }
                    }
                }
            }
        } catch (Exception ex) {
            throw new IOException("Failed to build org-to-POC mapping", ex);
        }
        return result;
    }

    private static String extractDomain(String email) {
        if (email == null) return null;
        final int at = email.lastIndexOf('@');
        return at > 0 && at < email.length() - 1 ? email.substring(at + 1) : null;
    }

    private static void parseArinAsnsCollectingOrgHandles(java.io.InputStream in,
                                                          Map<Long, Map<String, String>> asns,
                                                          Set<String> orgHandles) throws Exception {
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
                    if ("asn".equals(localName)) {
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
                            if (!"IANA".equals(orgHandle)) {
                                final Map<String, String> values = new HashMap<>();
                                if (orgHandle != null) {
                                    values.put("orghandle", orgHandle);
                                    orgHandles.add(orgHandle);
                                }
                                if (currentAttrs.containsKey("name")) values.put("name", currentAttrs.get("name"));
                                final var immutableValues = Map.copyOf(values);
                                final long rangeSize = endAsn - startAsn + 1;
                                if (rangeSize <= 1000) {
                                    for (long asn = startAsn; asn <= endAsn; asn++) {
                                        asns.put(asn, immutableValues);
                                    }
                                } else {
                                    asns.put(startAsn, immutableValues);
                                }
                            }
                        }
                        currentElement = null;
                        currentAttrs = null;
                    }
                }
            }
        }
    }

    private static void parseArinOrgsFiltered(java.io.InputStream in,
                                              Map<String, ArinOrg> orgs,
                                              Set<String> allowedHandles,
                                              Set<String> pocHandlesOut) throws Exception {
        final var factory = javax.xml.stream.XMLInputFactory.newInstance();
        factory.setProperty(javax.xml.stream.XMLInputFactory.IS_SUPPORTING_EXTERNAL_ENTITIES, false);
        factory.setProperty(javax.xml.stream.XMLInputFactory.SUPPORT_DTD, false);
        final var reader = factory.createXMLStreamReader(in, StandardCharsets.UTF_8.name());
        String currentElement = null;
        Map<String, String> currentAttrs = null;
        List<String> currentPocHandles = null;
        String currentTag = null;
        final var textBuffer = new StringBuilder();
        int skipped = 0;

        while (reader.hasNext()) {
            final int event = reader.next();
            switch (event) {
                case javax.xml.stream.XMLStreamConstants.START_ELEMENT -> {
                    final var localName = reader.getLocalName();
                    if ("org".equals(localName)) {
                        currentElement = localName;
                        currentAttrs = new HashMap<>();
                        currentPocHandles = new ArrayList<>();
                    } else if (currentElement != null) {
                        currentTag = localName;
                        textBuffer.setLength(0);
                        // pocLinkRef has handle as XML attribute: <pocLinkRef handle="POC-HANDLE" ...>
                        if ("pocLinkRef".equals(localName) && currentPocHandles != null) {
                            final var handleAttr = reader.getAttributeValue(null, "handle");
                            if (handleAttr != null && !handleAttr.isBlank()) {
                                currentPocHandles.add(handleAttr);
                            }
                        }
                    }
                }
                case javax.xml.stream.XMLStreamConstants.CHARACTERS, javax.xml.stream.XMLStreamConstants.CDATA -> {
                    if (currentTag != null) textBuffer.append(reader.getText());
                }
                case javax.xml.stream.XMLStreamConstants.END_ELEMENT -> {
                    final var localName = reader.getLocalName();
                    if (currentTag != null && currentTag.equals(localName) && currentAttrs != null) {
                        final var text = textBuffer.toString().trim();
                        if (!text.isEmpty()) {
                            currentAttrs.put(localName, text);
                        }
                        currentTag = null;
                    } else if ("org".equals(localName) && currentAttrs != null) {
                        final var handle = currentAttrs.get("handle");
                        if (handle != null && allowedHandles.contains(handle)) {
                            final var org = new ArinOrg(
                                    currentAttrs.get("name"),
                                    buildAddress(currentAttrs),
                                    currentAttrs.get("city"),
                                    currentAttrs.get("iso3166-2"),
                                    currentAttrs.get("iso3166-1"),
                                    currentAttrs.get("postalCode"),
                                    List.of());
                            orgs.put(handle, org);
                            if (currentPocHandles != null) {
                                pocHandlesOut.addAll(currentPocHandles);
                            }
                        } else {
                            skipped++;
                        }
                        currentElement = null;
                        currentAttrs = null;
                        currentPocHandles = null;
                    }
                }
            }
        }
        if (skipped > 0) {
            System.out.printf("Skipped %,d unreferenced orgs%n", skipped);
        }
    }

    private static String buildAddress(Map<String, String> attrs) {
        final var parts = new ArrayList<String>();
        for (int i = 1; i <= 6; i++) {
            final var line = attrs.get("streetLine" + i);
            if (line != null && !line.isBlank()) parts.add(line.trim());
        }
        return parts.isEmpty() ? null : String.join(", ", parts);
    }

    private static void parseArinPocsFiltered(java.io.InputStream in,
                                              Map<String, List<String>> emailsByPocHandle,
                                              Set<String> allowedHandles) throws Exception {
        final var factory = javax.xml.stream.XMLInputFactory.newInstance();
        factory.setProperty(javax.xml.stream.XMLInputFactory.IS_SUPPORTING_EXTERNAL_ENTITIES, false);
        factory.setProperty(javax.xml.stream.XMLInputFactory.SUPPORT_DTD, false);
        final var reader = factory.createXMLStreamReader(in, StandardCharsets.UTF_8.name());
        String currentElement = null;
        String currentHandle = null;
        List<String> currentEmails = null;
        String currentTag = null;
        final var textBuffer = new StringBuilder();
        int skipped = 0;

        while (reader.hasNext()) {
            final int event = reader.next();
            switch (event) {
                case javax.xml.stream.XMLStreamConstants.START_ELEMENT -> {
                    final var localName = reader.getLocalName();
                    if ("poc".equals(localName)) {
                        currentElement = localName;
                        currentHandle = null;
                        currentEmails = new ArrayList<>();
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
                    if (currentTag != null && currentTag.equals(localName)) {
                        final var text = textBuffer.toString().trim();
                        if (!text.isEmpty()) {
                            if ("handle".equals(localName)) currentHandle = text;
                            else if ("email".equals(localName) && currentEmails != null) currentEmails.add(text);
                        }
                        currentTag = null;
                    } else if ("poc".equals(localName)) {
                        if (currentHandle != null && allowedHandles.contains(currentHandle) && currentEmails != null && !currentEmails.isEmpty()) {
                            emailsByPocHandle.put(currentHandle, List.copyOf(currentEmails));
                        } else if (currentHandle != null && !allowedHandles.contains(currentHandle)) {
                            skipped++;
                        }
                        currentElement = null;
                        currentHandle = null;
                        currentEmails = null;
                    }
                }
            }
        }
        if (skipped > 0) {
            System.out.printf("Skipped %,d unreferenced POCs%n", skipped);
        }
    }

    private static void parseArinXmlStream(java.io.InputStream in,
                                           Map<Long, Map<String, String>> asns,
                                           Map<String, ArinOrg> orgs,
                                           Set<String> orgFilter) throws Exception {
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
                            final var shouldInclude = orgFilter == null || orgFilter.contains(handle);
                            if (shouldInclude && currentAttrs.containsKey("name")) {
                                final var org = new ArinOrg(
                                        currentAttrs.get("name"),
                                        buildAddress(currentAttrs),
                                        currentAttrs.get("city"),
                                        currentAttrs.get("iso3166-2"),
                                        currentAttrs.get("iso3166-1"),
                                        currentAttrs.get("postalCode"),
                                        List.of());
                                orgs.put(handle, org);
                            }
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
        final var arinOrg = orgHandle == null ? null : arinData.orgs().get(orgHandle);
        final var orgName = arinOrg != null ? arinOrg.name() : null;
        final var organization = firstNonBlank(orgName, orgHandle, name, "Unknown");
        // Build address from ARIN org data
        final var address = arinOrg != null ? buildFullAddress(arinOrg) : null;
        // Get email domains from POCs
        final var emails = arinOrg != null && arinOrg.emailDomains() != null ? arinOrg.emailDomains() : List.<String>of();
        final var entity = new AsnMetadata.Entity(orgHandle, organization, organization, null, address,
                List.of(), emails, List.of(), List.of(), List.of());
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

    private static String buildFullAddress(ArinOrg org) {
        final var parts = new ArrayList<String>();
        if (org.address() != null && !org.address().isBlank()) parts.add(org.address());
        if (org.city() != null && !org.city().isBlank()) parts.add(org.city());
        if (org.state() != null && !org.state().isBlank()) parts.add(org.state());
        if (org.postalCode() != null && !org.postalCode().isBlank()) parts.add(org.postalCode());
        if (org.country() != null && !org.country().isBlank()) parts.add(org.country());
        return parts.isEmpty() ? null : String.join(", ", parts);
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

    public record ArinBulkData(Map<Long, Map<String, String>> asns, Map<String, ArinOrg> orgs) {
        public static ArinBulkData empty() {
            return new ArinBulkData(Map.of(), Map.of());
        }
    }

    public record ArinOrg(String name, String address, String city, String state, String country,
                          String postalCode, List<String> emailDomains) {
        public static ArinOrg withName(String name) {
            return new ArinOrg(name, null, null, null, null, null, List.of());
        }

        public ArinOrg withEmailDomains(List<String> domains) {
            return new ArinOrg(name, address, city, state, country, postalCode, domains);
        }
    }
}
