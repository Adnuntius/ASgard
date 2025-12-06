package org.asgard;

import java.io.IOException;
import java.io.StringReader;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.stream.LongStream;

public final class AsnRegistryClient {
    private static final DateTimeFormatter DELEGATED_DATE = DateTimeFormatter.BASIC_ISO_DATE;
    private final HttpClient httpClient;
    private final List<URI> sources;
    private final Duration timeout;

    public AsnRegistryClient(HttpClient httpClient, List<URI> sources, Duration timeout) {
        this.httpClient = httpClient;
        this.sources = List.copyOf(sources);
        this.timeout = timeout;
    }

    public static List<URI> defaultSources() {
        return List.of(
                URI.create("https://ftp.ripe.net/pub/stats/arin/delegated-arin-extended-latest"),
                URI.create("https://ftp.ripe.net/pub/stats/apnic/delegated-apnic-extended-latest"),
                URI.create("https://ftp.ripe.net/pub/stats/ripencc/delegated-ripencc-extended-latest"),
                URI.create("https://ftp.ripe.net/pub/stats/lacnic/delegated-lacnic-extended-latest"),
                URI.create("https://ftp.ripe.net/pub/stats/afrinic/delegated-afrinic-extended-latest")
        );
    }

    public List<AsnAllocation> fetchAllocations() throws IOException, InterruptedException {
        var allocations = new ArrayList<AsnAllocation>();
        for (var source : sources) {
            try {
                var body = download(source);
                allocations.addAll(parseAllocations(body));
            } catch (IOException ex) {
                System.err.println("Failed to download " + source + ": " + ex.getMessage());
            } catch (InterruptedException ex) {
                Thread.currentThread().interrupt();
                throw ex;
            }
        }
        return allocations;
    }

    public List<AsnAllocation> parseAllocations(String delegatedFile) {
        var allocations = new ArrayList<AsnAllocation>();
        var reader = new java.io.BufferedReader(new StringReader(delegatedFile));
        reader.lines().forEach(line -> parseLine(line).ifPresent(allocations::add));
        return allocations;
    }

    private String download(URI source) throws IOException, InterruptedException {
        var request = HttpRequest.newBuilder(source)
                .timeout(timeout)
                .header("Accept", "text/plain")
                .GET()
                .build();
        var response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
        if (response.statusCode() >= 200 && response.statusCode() < 300) return response.body();
        throw new IOException("Failed to download registry data from " + source + ": " + response.statusCode());
    }

    private static java.util.Optional<AsnAllocation> parseLine(String line) {
        if (line.isBlank() || line.startsWith("#")) return java.util.Optional.empty();
        var parts = line.split("\\|");
        if (parts.length < 7) return java.util.Optional.empty();
        if (!"asn".equalsIgnoreCase(parts[2])) return java.util.Optional.empty();
        var status = parts[6].toLowerCase(Locale.ROOT);
        if (!Set.of("allocated", "assigned").contains(status)) return java.util.Optional.empty();
        try {
            var start = Long.parseLong(parts[3]);
            var count = Long.parseLong(parts[4]);
            var registry = parts[0];
            var country = parts[1];
            var date = parseDate(parts[5]);
            return java.util.Optional.of(new AsnAllocation(start, count, registry, country, status, date));
        } catch (NumberFormatException ex) {
            return java.util.Optional.empty();
        }
    }

    private static LocalDate parseDate(String raw) {
        try {
            return LocalDate.parse(raw, DELEGATED_DATE);
        } catch (Exception ex) {
            return null;
        }
    }

    public static LongStream expandAllocations(Collection<AsnAllocation> allocations) {
        return allocations.stream().flatMapToLong(allocation -> LongStream.range(allocation.startAsn(),
                allocation.startAsn() + allocation.count()));
    }
}
