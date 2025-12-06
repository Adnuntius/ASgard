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
    private MockWebServer rdapServer;
    private MockWebServer openAiServer;

    @BeforeEach
    void setUp() throws Exception {
        registryServer = new MockWebServer();
        rdapServer = new MockWebServer();
        openAiServer = new MockWebServer();
        registryServer.start();
        rdapServer.start();
        openAiServer.start();
    }

    @AfterEach
    void tearDown() throws Exception {
        registryServer.shutdown();
        rdapServer.shutdown();
        openAiServer.shutdown();
    }

    @Test
    void pipelineRunsAgainstMockServicesAndDisplaysWindow() throws Exception {
        Assumptions.assumeFalse(GraphicsEnvironment.isHeadless(), "UI display required");

        registryServer.enqueue(new MockResponse().setBody("""
                arin|US|asn|64512|2|20240101|allocated
                """).addHeader("Content-Type", "text/plain"));
        rdapServer.enqueue(rdapResponse("Alpha ISP"));
        rdapServer.enqueue(rdapResponse("Beta ISP"));
        openAiServer.enqueue(classificationResponse("ISP", "Alpha ISP"));
        openAiServer.enqueue(classificationResponse("ISP", "Beta ISP"));

        final Path stateDir = Files.createTempDirectory("asn-state");
        final Path output = stateDir.resolve("asn-classify.tsv");
        final var registryUrl = registryServer.url("/delegated.txt").toString();
        final var rdapUrl = rdapServer.url("/").toString();
        final var openAiUrl = openAiServer.url("/v1/").toString();
        Files.writeString(output, """
                {"asn":64512,"entity":"Unknown","category":"Unknown"}
                """, StandardCharsets.UTF_8);

        AsnClassifierApp.main(new String[]{
                "--registry-sources=" + registryUrl,
                "--rdap-base=" + rdapUrl,
                "--openai-base=" + openAiUrl,
                "--state-dir=" + stateDir,
                "--api-key=test-token",
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

    private MockResponse rdapResponse(String name) {
        var body = """
                {
                  "name": "%s",
                  "country": "US",
                  "entities": [
                    {
                      "vcardArray": [
                        "vcard",
                        [
                          ["version", {}, "text", "4.0"],
                          ["fn", {}, "text", "%s"]
                        ]
                      ]
                    }
                  ],
                  "startAutnum": 64512,
                  "endAutnum": 64512,
                  "handle": "AS64512"
                }
                """.formatted(name, name);
        return new MockResponse().setResponseCode(200).addHeader("Content-Type", "application/json").setBody(body);
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
