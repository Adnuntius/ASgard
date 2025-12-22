package org.asgard.registry;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.asgard.AsnRegistryClient;
import org.asgard.Config;
import org.asgard.ConfigManager;
import org.asgard.registry.AsnBulkDump.ArinBulkData;
import org.asgard.registry.AsnBulkDump.DelegatedRecord;
import org.asgard.registry.AsnBulkDump.RpslObject;

import javax.swing.*;
import java.awt.*;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;

public final class RegistryDatabaseBuilder {
    private final Path databaseFile;
    private final AsnRegistryClient registryClient;
    private final ObjectMapper mapper;
    private final HttpClient httpClient;
    private final ConfigManager configManager;
    private final Config config;
    private final String arinApiKeyOverride;
    private final boolean skipArinBulk;

    public RegistryDatabaseBuilder(Path databaseFile, AsnRegistryClient registryClient,
                                   ObjectMapper mapper, HttpClient httpClient, ConfigManager configManager, Config config,
                                   String arinApiKeyOverride, boolean skipArinBulk) {
        this.databaseFile = databaseFile;
        this.registryClient = registryClient;
        this.mapper = mapper;
        this.httpClient = httpClient;
        this.configManager = configManager;
        this.config = config;
        this.arinApiKeyOverride = arinApiKeyOverride;
        this.skipArinBulk = skipArinBulk;
    }

    public void build() throws IOException, InterruptedException {
        final var parent = databaseFile.getParent();
        if (parent != null) Files.createDirectories(parent);
        final var tempDir = Files.createTempDirectory("registry-cache-build");
        final var tempOutput = tempDir.resolve("registry-cache.ndjson");
        try {
            System.out.println("Starting registry cache build...");
            final var arinData = downloadArinBulk(tempDir);
            final var delegated = downloadDelegatedExtended(tempDir);
            final var rpslByAsn = downloadRpsl(tempDir);
            final var metadata = AsnBulkDump.assembleMetadata(delegated, rpslByAsn, arinData);
            try (var writer = Files.newBufferedWriter(tempOutput, StandardCharsets.UTF_8)) {
                for (var entry : metadata) {
                    writer.write(mapper.writeValueAsString(entry));
                    writer.newLine();
                }
            }
            Files.move(tempOutput, databaseFile, java.nio.file.StandardCopyOption.REPLACE_EXISTING,
                    java.nio.file.StandardCopyOption.ATOMIC_MOVE);
            System.out.printf("Wrote registry cache with %,d entries to %s%n", metadata.size(), databaseFile);
        } finally {
            deleteRecursively(tempDir);
        }
    }

    private Map<Long, List<DelegatedRecord>> downloadDelegatedExtended(Path tempDir) throws IOException, InterruptedException {
        final var aggregated = tempDir.resolve("delegated-extended.txt");
        final Map<String, String> urls = new java.util.LinkedHashMap<>();
        urls.put("arin", "https://ftp.arin.net/pub/stats/arin/delegated-arin-extended-latest");
        urls.put("apnic", "https://ftp.apnic.net/pub/stats/apnic/delegated-apnic-extended-latest");
        urls.put("afrinic", "https://ftp.afrinic.net/pub/stats/afrinic/delegated-afrinic-extended-latest");
        urls.put("lacnic", "https://ftp.lacnic.net/pub/stats/lacnic/delegated-lacnic-extended-latest");
        urls.put("ripe", "https://ftp.ripe.net/pub/stats/ripencc/delegated-ripencc-extended-latest");
        int downloaded = 0;
        try (var writer = Files.newBufferedWriter(aggregated, StandardCharsets.UTF_8)) {
            for (var entry : urls.entrySet()) {
                System.out.printf("Downloading delegated extended from %s...%n", entry.getKey());
                final var temp = downloadToTemp(URI.create(entry.getValue()));
                if (temp.isEmpty()) {
                    throw new IOException("Failed to download delegated extended for " + entry.getKey());
                }
                downloaded++;
                Files.lines(temp.get(), StandardCharsets.UTF_8).forEach(line -> {
                    try {
                        writer.write(line);
                        writer.newLine();
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                });
                Files.deleteIfExists(temp.get());
            }
        }
        if (downloaded != urls.size()) {
            throw new IOException("Delegated extended downloads incomplete (" + downloaded + "/" + urls.size() + ")");
        }
        System.out.printf("Downloaded delegated extended data from %d registries%n", downloaded);
        return AsnBulkDump.parseDelegatedExtended(aggregated);
    }

    private Map<Long, RpslObject> downloadRpsl(Path tempDir) throws IOException, InterruptedException {
        // LACNIC doesn't provide public RPSL database access - requires special bulk whois request
        // LACNIC ASN data comes from delegated-extended file instead
        final var targets = List.of(
                new DownloadTarget("https://ftp.ripe.net/ripe/dbase/split/ripe.db.aut-num.gz", "ripe aut-num"),
                new DownloadTarget("https://ftp.apnic.net/apnic/whois/apnic.db.aut-num.gz", "apnic aut-num"),
                new DownloadTarget("https://ftp.afrinic.net/pub/dbase/afrinic.db.gz", "afrinic aut-num"),
                new DownloadTarget("https://ftp.arin.net/pub/rr/arin.db.gz", "arin aut-num")
        );
        final Map<Long, RpslObject> byAsn = new HashMap<>();
        for (var target : targets) {
            final var temp = downloadToTemp(URI.create(target.url()));
            if (temp.isEmpty()) continue;
            System.out.printf("Downloaded %s into %s%n", target.description(), temp.get());
            try {
                final var objects = AsnBulkDump.iterRpslObjectsFromGz(temp.get());
                for (var obj : objects) {
                    final var autNum = obj.attrs().get("aut-num");
                    if (autNum == null || autNum.isEmpty()) continue;
                    final var value = autNum.get(0).toUpperCase().replace("AS", "");
                    try {
                        final long asn = Long.parseLong(value);
                        byAsn.putIfAbsent(asn, obj);
                    } catch (NumberFormatException ignored) {
                    }
                }
            } finally {
                Files.deleteIfExists(temp.get());
            }
        }
        return Map.copyOf(byAsn);
    }

    private ArinBulkData downloadArinBulk(Path tempDir) throws IOException, InterruptedException {
        if (skipArinBulk) {
            System.out.println("Skipping ARIN bulk download (--skip-arin-bulk)");
            return ArinBulkData.empty();
        }
        final var asnsFile = tempDir.resolve("arin_asns.xml");
        final var orgsFile = tempDir.resolve("arin_orgs.xml");
        final var pocsFile = tempDir.resolve("arin_pocs.xml");
        final var keySource = resolveArinApiKeyWithSource();
        var apiKey = keySource.key();
        var arinData = tryDownloadAndParseArinBulkThreePass(apiKey, asnsFile, orgsFile, pocsFile);

        // If download failed or data is empty, and key came from config, delete it and re-prompt
        if ((arinData == null || arinData.asns().isEmpty()) && "config".equals(keySource.source())) {
            System.err.println("ARIN API key from config failed or returned empty data. Removing and prompting for new one.");
            configManager.save(new Config(config.model(), null));
            apiKey = promptForArinKey();
            if (apiKey != null) {
                persistArinApiKey(apiKey);
                arinData = tryDownloadAndParseArinBulkThreePass(apiKey, asnsFile, orgsFile, pocsFile);
            }
        }

        if (arinData == null || arinData.asns().isEmpty()) {
            throw new IOException("""
                    ARIN bulk download failed or returned empty data.
                    Your API key may not have bulk whois access enabled.
                    Visit https://account.arin.net to request bulk whois access.

                    To proceed without ARIN bulk data (most ARIN ASNs will show 'Unknown'),
                    add --skip-arin-bulk to your command.""");
        }
        return arinData;
    }

    private ArinBulkData tryDownloadAndParseArinBulkThreePass(String apiKey, Path asnsFile, Path orgsFile, Path pocsFile) throws IOException {
        final var asnsTemp = tryDownloadArinFile(apiKey, "asns.xml");
        if (asnsTemp.isEmpty()) return null;
        Files.move(asnsTemp.get(), asnsFile, java.nio.file.StandardCopyOption.REPLACE_EXISTING);
        System.out.printf("Downloaded ARIN ASNs to %s%n", asnsFile);

        final var orgsTemp = tryDownloadArinFile(apiKey, "orgs.xml");
        if (orgsTemp.isEmpty()) {
            throw new IOException("Failed to download ARIN orgs.xml - full org data is required for classification");
        }
        Files.move(orgsTemp.get(), orgsFile, java.nio.file.StandardCopyOption.REPLACE_EXISTING);
        System.out.printf("Downloaded ARIN Orgs to %s%n", orgsFile);

        final var pocsTemp = tryDownloadArinFile(apiKey, "pocs.xml");
        if (pocsTemp.isEmpty()) {
            throw new IOException("Failed to download ARIN pocs.xml - POC email domains are required for classification");
        }
        Files.move(pocsTemp.get(), pocsFile, java.nio.file.StandardCopyOption.REPLACE_EXISTING);
        System.out.printf("Downloaded ARIN POCs to %s%n", pocsFile);

        return AsnBulkDump.parseArinBulkThreePass(asnsFile, orgsFile, pocsFile);
    }

    private Optional<Path> tryDownloadArinFile(String apiKey, String fileName) {
        final var url = "https://accountws.arin.net/public/rest/downloads/bulkwhois/" + fileName + "?apikey="
                + URLEncoder.encode(apiKey, StandardCharsets.UTF_8);
        System.out.printf("Downloading ARIN %s...%n", fileName);
        return downloadToTemp(URI.create(url));
    }

    private record KeySource(String key, String source) {}

    private KeySource resolveArinApiKeyWithSource() throws IOException {
        final var envKey = firstNonBlank(System.getenv("ASGARD_ARIN_API_KEY"), System.getenv("ARIN_API_KEY"));
        if (arinApiKeyOverride != null && !arinApiKeyOverride.isBlank()) {
            final var key = sanitizeKey(arinApiKeyOverride, "argument");
            if (key == null) throw new IOException("Invalid ARIN API key provided via --arin-api-key.");
            return new KeySource(key, "override");
        }
        final var configKey = sanitizeKey(config.arinApiKey(), "config");
        if (configKey != null) {
            return new KeySource(configKey, "config");
        }
        final var envKeyClean = sanitizeKey(envKey, "env");
        if (envKeyClean != null) {
            return new KeySource(envKeyClean, "env");
        }
        final var prompted = promptForArinKey();
        if (prompted == null) throw new IOException("Invalid ARIN API key.");
        persistArinApiKey(prompted);
        return new KeySource(prompted, "prompt");
    }

    private void persistArinApiKey(String apiKey) {
        final var normalized = normalizeKey(apiKey);
        if (normalized == null || normalized.equals(config.arinApiKey())) return;
        configManager.save(new Config(config.model(), normalized));
    }

    private String promptForArinKey() throws IOException {
        final var url = "https://account.arin.net/public/secure/manageapikey.xhtml";
        System.out.println("ARIN bulk data requires an API key. Visit: " + url);
        openBrowser(url);
        final var console = System.console();
        if (console != null) {
            final var entered = console.readLine(
                    "Make sure you have requested bulk whois on your arin account, then enter the ARIN API key: ");
            final var sanitized = sanitizeKey(entered, "console");
            if (sanitized != null) return sanitized;
        }
        if (GraphicsEnvironment.isHeadless()) {
            throw new IOException("ARIN API key required. Set ARIN_API_KEY, ASGARD_ARIN_API_KEY, or pass --arin-api-key.");
        }
        return sanitizeKey(promptWithDialog(url), "dialog");
    }

    private String promptWithDialog(String url) throws IOException {
        final var valueRef = new AtomicReference<String>();
        try {
            SwingUtilities.invokeAndWait(() -> valueRef.set(JOptionPane.showInputDialog(
                    null,
                    "ARIN bulk data requires an API key.\nOpen: " + url + "\nEnter the ARIN API key:",
                    "ARIN API Key",
                    JOptionPane.QUESTION_MESSAGE
            )));
        } catch (Exception ex) {
            throw new IOException("Failed to open API key prompt: " + ex.getMessage(), ex);
        }
        return valueRef.get();
    }

    static String selectArinApiKey(String override, String configValue, String envValue, KeyPrompt prompt)
            throws IOException {
        final var overrideKey = sanitizeKey(override, "argument");
        if (override != null && overrideKey == null) {
            throw new IOException("Invalid ARIN API key provided via --arin-api-key.");
        }
        if (overrideKey != null) return overrideKey;
        final var configKey = sanitizeKey(configValue, "config");
        if (configKey != null) return configKey;
        final var envKey = sanitizeKey(envValue, "env");
        if (envKey != null) return envKey;
        final var prompted = sanitizeKey(prompt.get(), "prompt");
        if (prompted == null) throw new IOException("Invalid ARIN API key.");
        return prompted;
    }

    private static String firstNonBlank(String... values) {
        if (values == null) return null;
        for (final var value : values) {
            if (value != null && !value.isBlank()) return value.trim();
        }
        return null;
    }

    private static String normalizeKey(String value) {
        if (value == null) return null;
        final var trimmed = value.trim();
        return trimmed.isEmpty() ? null : trimmed;
    }

    private static String sanitizeKey(String value, String source) {
        final var normalized = normalizeKey(value);
        if (normalized == null) return null;
        if (normalized.chars().anyMatch(Character::isWhitespace)) {
            System.err.printf("Ignoring ARIN API key from %s because it contains whitespace.%n", source);
            return null;
        }
        return normalized;
    }

    @FunctionalInterface
    interface KeyPrompt {
        String get() throws IOException;
    }

    private static final int DOWNLOAD_MAX_RETRIES = 3;
    private static final long DOWNLOAD_RETRY_DELAY_MS = 5000;

    private Optional<Path> downloadToTemp(URI uri) {
        return downloadToTempWithRetries(uri, DOWNLOAD_MAX_RETRIES);
    }

    private Optional<Path> downloadToTempWithRetries(URI uri, int retriesLeft) {
        // Use longer timeout for bulk downloads (10 minutes for multi-GB files)
        final var request = HttpRequest.newBuilder()
                .uri(uri)
                .timeout(java.time.Duration.ofMinutes(10))
                .GET()
                .build();
        try {
            final var response = httpClient.send(request, HttpResponse.BodyHandlers.ofInputStream());
            final var status = response.statusCode();
            final var location = response.headers().firstValue("Location").orElse(null);
            if (status >= 300 && status < 400) {
                System.err.printf("Download for %s received redirect %d -> %s%n", uri, status, location);
                return Optional.empty();
            }
            if (status != 200) {
                final var preview = new String(response.body().readNBytes(512), StandardCharsets.UTF_8);
                final var contentType = response.headers().firstValue("Content-Type").orElse("unknown");
                System.err.printf("Failed to download %s (status %d, content-type %s). Body preview: %s%n",
                        uri, status, contentType, preview);
                return Optional.empty();
            }
            final var temp = Files.createTempFile("registry-download", ".bin");
            try (var body = response.body(); var out = Files.newOutputStream(temp)) {
                body.transferTo(out);
            }
            return Optional.of(temp);
        } catch (Exception ex) {
            if (retriesLeft > 0) {
                System.err.printf("Download error for %s: %s (retrying in %.0fs, %d retries left)%n",
                        uri, ex.getMessage(), DOWNLOAD_RETRY_DELAY_MS / 1000.0, retriesLeft);
                try {
                    Thread.sleep(DOWNLOAD_RETRY_DELAY_MS);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    return Optional.empty();
                }
                return downloadToTempWithRetries(uri, retriesLeft - 1);
            }
            System.err.printf("Download error for %s: %s (no retries left)%n", uri, ex.getMessage());
            return Optional.empty();
        }
    }

    private record DownloadTarget(String url, String description) {
    }

    private void deleteRecursively(Path dir) {
        if (dir == null || !Files.exists(dir)) return;
        try (var walk = Files.walk(dir)) {
            walk.sorted(java.util.Comparator.reverseOrder()).forEach(path -> {
                try {
                    Files.deleteIfExists(path);
                } catch (IOException ignored) {
                }
            });
        } catch (IOException ignored) {
        }
    }

    private void openBrowser(String url) {
        try {
            if (Boolean.getBoolean("asgard.skipBrowser")) return;
            if (Desktop.isDesktopSupported()) {
                Desktop.getDesktop().browse(new URI(url));
            } else {
                System.out.println("Please open in your browser: " + url);
            }
        } catch (IOException | URISyntaxException ex) {
            System.out.println("Please open in your browser: " + url + " (" + ex.getMessage() + ")");
        }
    }

}
