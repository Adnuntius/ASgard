package org.asgard;

import java.time.Instant;
import java.util.List;
import java.util.Optional;

public record AsnMetadata(long asn,
                          String handle,
                          long startAutnum,
                          long endAutnum,
                          String name,
                          String country,
                          String registry,
                          String type,
                          List<String> statuses,
                          String registrationDate,
                          String lastChangedDate,
                          Entity registrant,
                          List<Entity> contacts,
                          List<String> remarks,
                          Instant fetchedAt) {

    public record Entity(String handle,
                         String name,
                         String organization,
                         String kind,
                         String address,
                         List<String> roles,
                         List<String> emails,
                         List<String> phones,
                         List<String> remarks,
                         List<String> statuses) {
    }

    public Optional<String> organizationName() {
        return optional(firstNonBlank(registrant == null ? null : registrant.organization(),
                registrant == null ? null : registrant.name()));
    }

    public Optional<String> countryCode() {
        return optional(country);
    }

    public Optional<String> displayName() {
        return optional(firstNonBlank(registrant == null ? null : registrant.name(), name));
    }

    public Optional<String> registryType() {
        return optional(type);
    }

    public Optional<String> entityForClassification() {
        return optional(firstNonBlank(registrant == null ? null : registrant.organization(),
                registrant == null ? null : registrant.name(), name));
    }

    private Optional<String> optional(String value) {
        return Optional.ofNullable(value).filter(it -> !it.isBlank());
    }

    private String firstNonBlank(String... values) {
        for (var value : values) {
            if (value != null && !value.isBlank()) return value;
        }
        return null;
    }

    public static AsnMetadata minimal(long asn, String name, String country, String organization, String type) {
        var entity = new Entity(null, organization, organization, null, null, List.of(), List.of(), List.of(),
                List.of(), List.of());
        return new AsnMetadata(asn, null, asn, asn, name, country, null, type, List.of(), null, null, entity,
                List.of(), List.of(), Instant.now());
    }
}
