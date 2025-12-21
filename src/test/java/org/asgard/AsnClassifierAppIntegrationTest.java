package org.asgard;

import okhttp3.mockwebserver.MockResponse;
import okhttp3.mockwebserver.MockWebServer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import javax.swing.*;
import java.awt.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class AsnClassifierAppIntegrationTest {
    private MockWebServer registryServer;
    private MockWebServer openAiServer;

    @BeforeEach
    void setUp() throws Exception {
        registryServer = new MockWebServer();
        openAiServer = new MockWebServer();
        registryServer.start();
        openAiServer.start();
    }

    @AfterEach
    void tearDown() throws Exception {
        registryServer.shutdown();
        openAiServer.shutdown();
    }

    @Test
    void pipelineRunsAgainstMockServicesAndDisplaysWindow() throws Exception {
        Assumptions.assumeFalse(GraphicsEnvironment.isHeadless(), "UI display required");

        registryServer.enqueue(new MockResponse().setBody("""
                arin|US|asn|64512|2|20240101|allocated
                """).addHeader("Content-Type", "text/plain"));
        openAiServer.enqueue(classificationResponse("ISP", "Alpha ISP"));
        openAiServer.enqueue(classificationResponse("ISP", "Beta ISP"));

        final Path stateDir = Files.createTempDirectory("asn-state");
        writeRegistryCache(stateDir, List.of(
                AsnMetadata.minimal(64512, "Alpha ISP", "US", "Alpha ISP", "ISP"),
                AsnMetadata.minimal(64513, "Beta ISP", "US", "Beta ISP", "ISP")
        ));
        final Path output = stateDir.resolve("asn-classify.tsv");
        final var registryUrl = registryServer.url("/delegated.txt").toString();
        final var openAiUrl = openAiServer.url("/v1/").toString();
        Files.writeString(output, """
                {"asn":64512,"entity":"Unknown","category":"Unknown"}
                """, StandardCharsets.UTF_8);

        AsnClassifierApp.main(new String[]{
                "--registry-sources=" + registryUrl,
                "--openai-base=" + openAiUrl,
                "--state-dir=" + stateDir,
                "--api-key=test-token",
                "--arin-api-key=test-arin",
                "--limit=2",
                "--output=" + output
        });

        final var lines = Files.readAllLines(output);
        assertThat(lines).hasSize(3);
        assertThat(lines.get(0)).isEqualTo("asn\tname\torganization\tcategory");
        assertThat(lines).anyMatch(line -> line.contains("Alpha ISP"));
        assertThat(lines).anyMatch(line -> line.contains("Beta ISP"));
        final var mapper = new com.fasterxml.jackson.databind.ObjectMapper();
        final var firstCall = openAiServer.takeRequest();
        final var secondCall = openAiServer.takeRequest();
        assertThat(registrantName(firstCall.getBody().readUtf8(), mapper)).isEqualTo("Alpha ISP");
        assertThat(registrantName(secondCall.getBody().readUtf8(), mapper)).isEqualTo("Beta ISP");

        final var configContents = Files.readString(new StatePaths(stateDir).configFile());
        assertThat(configContents).contains("arinApiKey=test-arin");

        showWindow(lines);
    }

    private String registrantName(String requestBody, com.fasterxml.jackson.databind.ObjectMapper mapper) throws Exception {
        var root = mapper.readTree(requestBody);
        var content = root.path("messages").get(1).path("content").asText();
        for (var line : content.split("\\R")) {
            var trimmed = line.trim();
            if (trimmed.toLowerCase().startsWith("name:")) {
                return trimmed.substring(5).trim();
            }
        }
        return "";
    }

    private MockResponse classificationResponse(String category, String entity) {
        var content = """
                {
                  "choices": [
                    {
                      "message": {
                        "content": "{\\"category\\":\\"%s\\",\\"confidence\\":0.9,\\"entity\\":\\"%s\\",\\"rationale\\":\\"demo\\"}"
                      }
                    }
                  ]
                }
                """.formatted(category, entity);
        return new MockResponse().setResponseCode(200).addHeader("Content-Type", "application/json").setBody(content);
    }

    private void writeRegistryCache(Path stateDir, List<AsnMetadata> entries) throws Exception {
        final var registryFile = new StatePaths(stateDir).registryDatabaseFile();
        Files.createDirectories(registryFile.getParent());
        final var mapper = ObjectMapperFactory.create();
        try (var writer = Files.newBufferedWriter(registryFile, StandardCharsets.UTF_8)) {
            for (var entry : entries) {
                writer.write(mapper.writeValueAsString(entry));
                writer.newLine();
            }
        }
    }

    private void showWindow(List<String> lines) throws Exception {
        SwingUtilities.invokeAndWait(() -> {
            var frame = new JFrame("ASN Classifier Preview");
            frame.add(new JLabel("<html>" + String.join("<br/>", lines) + "</html>"));
            frame.pack();
            frame.setLocationRelativeTo(null);
            frame.setVisible(true);
            new Timer(400, e -> frame.dispose()).start();
        });
        Thread.sleep(450);
    }
}
