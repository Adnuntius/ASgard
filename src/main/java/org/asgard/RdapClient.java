package org.asgard;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.util.*;

public final class RdapClient {
    private static final int MAX_ATTEMPTS = 3;
    private static final Set<Integer> RETRYABLE_STATUSES = Set.of(429, 500, 502, 503, 504, 522, 524, 599);
    private final HttpClient httpClient;
    private final ObjectMapper objectMapper;
    private final URI baseUri;
    private final Duration timeout;
    private final URI fallbackUri;

    public RdapClient(HttpClient httpClient, ObjectMapper objectMapper, URI baseUri, Duration timeout) {
        this(httpClient, objectMapper, baseUri, URI.create("https://rdap.org/"), timeout);
    }

    RdapClient(HttpClient httpClient, ObjectMapper objectMapper, URI baseUri, URI fallbackUri, Duration timeout) {
        this.httpClient = httpClient;
        this.objectMapper = objectMapper;
        this.baseUri = normalize(baseUri);
        this.fallbackUri = normalize(fallbackUri);
        this.timeout = timeout;
    }

    public URI baseUri() {
        return baseUri;
    }

    public Optional<AsnMetadata> lookup(long asn) {
        return tryLookup(baseUri, asn).or(() -> {
            if (!fallbackUri.equals(baseUri)) return tryLookup(fallbackUri, asn);
            return Optional.empty();
        });
    }

    private Optional<AsnMetadata> tryLookup(URI base, long asn) {
        final var uri = base.resolve("autnum/" + asn);
        final var request = HttpRequest.newBuilder(uri)
                .GET()
                .header("Accept", "application/rdap+json, application/json")
                .timeout(timeout)
                .build();
        for (var attempt = 1; attempt <= MAX_ATTEMPTS; attempt++) {
            try {
                final var response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
                final var status = response.statusCode();
                if (status >= 200 && status < 300) {
                    return Optional.of(parse(asn, response.body()));
                }
                if (!shouldRetry(status, attempt)) {
                    System.err.printf("RDAP %s returned status %d%n", uri, status);
                    return Optional.empty();
                }
                pause(attempt);
            } catch (IOException ex) {
                System.err.printf("RDAP %s failed on attempt %d/%d: %s: %s%n", uri, attempt, MAX_ATTEMPTS,
                        ex.getClass().getSimpleName(), ex.getMessage());
                if (attempt == MAX_ATTEMPTS) return Optional.empty();
                pause(attempt);
            } catch (InterruptedException ex) {
                Thread.currentThread().interrupt();
                return Optional.empty();
            }
        }
        return Optional.empty();
    }

    private boolean shouldRetry(int status, int attempt) {
        return attempt < MAX_ATTEMPTS && (status == 408 || status >= 500 || RETRYABLE_STATUSES.contains(status));
    }

    private void pause(int attempt) {
        final var delayMillis = Math.min(1000L, 200L * attempt);
        try {
            Thread.sleep(delayMillis);
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
        }
    }

    private AsnMetadata parse(long asn, String body) throws IOException {
        final var root = objectMapper.readTree(body);
        final var name = text(root, "name");
        final var country = text(root, "country");
        final var type = text(root, "type");
        final var handle = text(root, "handle");
        final var registry = text(root, "port43");
        final var statuses = stringList(root.path("status"));
        final var registrationDate = eventDate(root.path("events"), "registration");
        final var lastChangedDate = eventDate(root.path("events"), "last changed");

        final var entities = parseEntities(root.path("entities"));
        final var registrant = pickRegistrant(entities);
        final var contacts = registrant == null ? entities : entities.stream()
                .filter(entity -> !entity.equals(registrant))
                .toList();
        final var remarks = collectRemarks(root.path("remarks"), 2);

        return new AsnMetadata(asn,
                handle,
                root.path("startAutnum").asLong(asn),
                root.path("endAutnum").asLong(asn),
                name,
                country,
                registry,
                type,
                statuses,
                registrationDate,
                lastChangedDate,
                registrant,
                contacts,
                remarks,
                Instant.now());
    }

    private List<AsnMetadata.Entity> parseEntities(JsonNode entities) {
        if (!entities.isArray()) return List.of();
        final List<AsnMetadata.Entity> results = new ArrayList<>();
        for (final var entity : entities) {
            results.add(parseEntity(entity));
        }
        return List.copyOf(results);
    }

    private AsnMetadata.Entity parseEntity(JsonNode entity) {
        final var roles = stringList(entity.path("roles"));
        final var statuses = stringList(entity.path("status"));
        final var remarks = collectRemarks(entity.path("remarks"), 3);
        final var vcard = entity.path("vcardArray");
        final var handle = text(entity, "handle");
        final var vcardDetails = parseVcard(vcard);
        return new AsnMetadata.Entity(handle,
                vcardDetails.name(),
                vcardDetails.organization(),
                vcardDetails.kind(),
                vcardDetails.address(),
                roles,
                vcardDetails.emails(),
                vcardDetails.phones(),
                remarks,
                statuses);
    }

    private AsnMetadata.Entity pickRegistrant(List<AsnMetadata.Entity> entities) {
        AsnMetadata.Entity first = null;
        for (final var entity : entities) {
            if (first == null) first = entity;
            final var roles = new HashSet<>(entity.roles()).stream()
                    .map(String::toLowerCase)
                    .collect(java.util.stream.Collectors.toSet());
            if (roles.contains("registrant") || roles.contains("registrar")) return entity;
        }
        return first;
    }

    private static String text(JsonNode node, String field) {
        var value = node.path(field).asText(null);
        return value == null || value.isBlank() ? null : value;
    }

    private static List<String> stringList(JsonNode node) {
        if (!node.isArray()) return List.of();
        final List<String> values = new ArrayList<>();
        for (final var element : node) {
            final var value = element.asText(null);
            if (value != null && !value.isBlank()) values.add(value);
        }
        return List.copyOf(values);
    }

    private record Vcard(String name, String organization, String kind, String address,
                         List<String> emails, List<String> phones) {
    }

    private Vcard parseVcard(JsonNode vcard) {
        if (!vcard.isArray() || vcard.size() < 2 || !vcard.get(1).isArray()) {
            return new Vcard(null, null, null, null, List.of(), List.of());
        }
        String name = null;
        String organization = null;
        String kind = null;
        String address = null;
        final List<String> emails = new ArrayList<>();
        final List<String> phones = new ArrayList<>();
        for (final var entry : vcard.get(1)) {
            if (!entry.isArray() || entry.size() < 4) continue;
            final var label = entry.get(0).asText();
            final var value = entry.get(3).asText(null);
            switch (label) {
                case "fn" -> name = value;
                case "org" -> organization = value;
                case "kind" -> kind = value;
                case "email" -> {
                    if (value != null && !value.isBlank()) emails.add(value);
                }
                case "tel" -> {
                    if (value != null && !value.isBlank()) phones.add(value);
                }
                case "adr" -> {
                    final var addrLabel = entry.get(1).path("label").asText(null);
                    address = addrLabel != null && !addrLabel.isBlank() ? addrLabel.replace("\n", ", ") : address;
                }
                default -> {
                }
            }
        }
        return new Vcard(name, organization, kind, address, List.copyOf(emails), List.copyOf(phones));
    }

    private String eventDate(JsonNode events, String action) {
        if (!events.isArray()) return null;
        for (final var event : events) {
            if (action.equalsIgnoreCase(event.path("eventAction").asText(""))) {
                final var raw = event.path("eventDate").asText(null);
                if (raw == null || raw.isBlank()) return null;
                try {
                    return OffsetDateTime.parse(raw).toString();
                } catch (Exception ignored) {
                    return raw;
                }
            }
        }
        return null;
    }

    private List<String> collectRemarks(JsonNode remarks, int limit) {
        if (!remarks.isArray()) return List.of();
        final List<String> cleaned = new ArrayList<>();
        for (final var remark : remarks) {
            if (cleaned.size() >= limit) break;
            final var description = remark.path("description");
            if (!description.isArray()) continue;
            final List<String> parts = new ArrayList<>();
            for (final var line : description) {
                final var text = line.asText(null);
                if (text != null && !text.isBlank()) parts.add(text.trim());
            }
            if (!parts.isEmpty()) {
                final var joined = String.join(" ", parts);
                cleaned.add(trim(joined, 420));
            }
        }
        return List.copyOf(cleaned);
    }

    private String trim(String value, int maxChars) {
        var normalized = value.replaceAll("\\s+", " ").trim();
        if (normalized.length() <= maxChars) return normalized;
        return normalized.substring(0, maxChars) + "...";
    }

    private static URI normalize(URI baseUri) {
        return baseUri.toString().endsWith("/") ? baseUri : URI.create(baseUri.toString() + "/");
    }
}
