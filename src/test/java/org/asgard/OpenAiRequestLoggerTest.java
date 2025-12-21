package org.asgard;

import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.time.Instant;

import static org.assertj.core.api.Assertions.assertThat;

class OpenAiRequestLoggerTest {
    @Test
    void logsRequestAndResponseToPerAsnFile() throws Exception {
        final var mapper = ObjectMapperFactory.create();
        final var logDir = Files.createTempDirectory("openai-log");
        final var logger = new OpenAiRequestLogger(logDir, mapper);
        final var metadata = AsnMetadata.minimal(728, "AFCONC-BLOCK2-AS", "US", "Air Force Systems", "VPN");
        final var classification = new FinalClassification(728, "AFCONC-BLOCK2-AS", "Air Force Systems", "Enterprise");

        logger.logRequest(metadata, "summary", "{\"model\":\"gpt\"}");
        logger.logResponse(metadata, 200, "{\"choices\":[]}", classification, 123);

        final var asnLogFile = logDir.resolve("asn-728.json");
        assertThat(asnLogFile).exists();

        final var content = Files.readString(asnLogFile, StandardCharsets.UTF_8);
        final var root = mapper.readTree(content);
        assertThat(root.isArray()).isTrue();
        assertThat(root).hasSize(2);

        final var requestNode = root.get(0);
        assertThat(requestNode.path("event").asText()).isEqualTo("request");
        assertThat(requestNode.path("asn").asLong()).isEqualTo(728);
        assertThat(requestNode.path("metadata").path("asn").asLong()).isEqualTo(728);
        assertThat(requestNode.path("summary").asText()).isEqualTo("summary");
        assertThat(Instant.parse(requestNode.path("timestamp").asText())).isNotNull();

        final var responseNode = root.get(1);
        assertThat(responseNode.path("event").asText()).isEqualTo("response");
        assertThat(responseNode.path("status").asInt()).isEqualTo(200);
        assertThat(responseNode.path("category").asText()).isEqualTo("Enterprise");
        assertThat(responseNode.path("approxPromptTokens").asLong()).isEqualTo(123);
        assertThat(Instant.parse(responseNode.path("timestamp").asText())).isNotNull();
    }
}
