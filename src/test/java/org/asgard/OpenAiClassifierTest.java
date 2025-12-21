package org.asgard;

import com.fasterxml.jackson.databind.ObjectMapper;
import okhttp3.mockwebserver.MockResponse;
import okhttp3.mockwebserver.MockWebServer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.net.http.HttpClient;
import java.time.Duration;

import static org.asgard.OpenAiClassifier.COMPLETION_TOKENS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class OpenAiClassifierTest {
    private MockWebServer server;

    @BeforeEach
    void setUp() throws Exception {
        server = new MockWebServer();
        server.start();
    }

    @AfterEach
    void tearDown() throws Exception {
        server.shutdown();
    }

    @Test
    void classifiesMetadataWithResponseFormat() throws Exception {
        final var responseJson = """
                {
                  "choices": [
                    {
                      "message": {
                        "content": "ISP"
                      }
                    }
                  ]
                }
                """;
        server.enqueue(new MockResponse()
                .setResponseCode(200)
                .addHeader("Content-Type", "application/json")
                .setBody(responseJson));

        final var classifier = new OpenAiClassifier(HttpClient.newHttpClient(), new ObjectMapper(),
                server.url("/v1/").uri(), "gpt-5-nano", "test-key", Duration.ofSeconds(5));
        final var metadata = AsnMetadata.minimal(64512, "Example ISP", "US", "Example ISP", "isp");

        final var result = classifier.classify(metadata);

        assertThat(result.category()).isEqualTo("ISP");
        assertThat(result.name()).isEqualTo("Example ISP");
        assertThat(result.organization()).isEqualTo("Example ISP");
        final var request = server.takeRequest();
        assertThat(request.getPath()).isEqualTo("/v1/chat/completions");
        final var body = request.getBody().readUtf8();
        final var json = new ObjectMapper().readTree(body);
        assertThat(json.path("reasoning_effort").asText()).isEqualTo("minimal");
        assertThat(json.path("max_completion_tokens").asInt()).isEqualTo(COMPLETION_TOKENS);
        final var content = json.path("messages").get(1).path("content").asText();
        assertThat(content).contains("ASN: 64512");
        assertThat(content).contains("Name: Example ISP");
        assertThat(content).contains("Kind: isp");
    }

    @Test
    void fallsBackWhenResponseIsTruncatedAndEmpty() {
        final var truncated = """
                {
                  "choices": [
                    {
                      "finish_reason": "length",
                      "message": {
                        "content": ""
                      }
                    }
                  ]
                }
                """;
        server.enqueue(new MockResponse()
                .setResponseCode(200)
                .addHeader("Content-Type", "application/json")
                .setBody(truncated));

        final var classifier = new OpenAiClassifier(HttpClient.newHttpClient(), new ObjectMapper(),
                server.url("/v1/").uri(), "gpt-5-nano", "test-key", Duration.ofSeconds(5));
        final var metadata = AsnMetadata.minimal(6, "AS6 Corp", "US", "AS6 Corp", "enterprise");

        assertThatThrownBy(() -> classifier.classify(metadata))
                .hasMessageContaining("truncated");
        assertThat(server.getRequestCount()).isEqualTo(1);
    }

    @Test
    void extractsCategoryFromLabeledResponseWithOtherCategoriesInReasoning() throws Exception {
        final var responseJson = """
                {
                  "choices": [
                    {
                      "finish_reason": "stop",
                      "message": {
                        "content": "Category: Infrastructure\\n\\nReasoning: The ASN belongs to MIT, a major educational and research institution with university-wide IT infrastructure and networks. It fits Infrastructure (large networks, IXPs, route-servers) rather than hosting, VPN, ISP, or enterprise per the provided categories."
                      }
                    }
                  ]
                }
                """;
        server.enqueue(new MockResponse()
                .setResponseCode(200)
                .addHeader("Content-Type", "application/json")
                .setBody(responseJson));

        final var classifier = new OpenAiClassifier(HttpClient.newHttpClient(), new ObjectMapper(),
                server.url("/v1/").uri(), "gpt-5-nano", "test-key", Duration.ofSeconds(5));
        final var metadata = AsnMetadata.minimal(3, "MIT-GATEWAYS", "US", "Massachusetts Institute of Technology",
                "allocated");

        final var result = classifier.classify(metadata);

        assertThat(result.category()).isEqualTo("Infrastructure");
        assertThat(result.name()).isEqualTo("MIT-GATEWAYS");
        assertThat(result.organization()).isEqualTo("Massachusetts Institute of Technology");
    }

    @Test
    void extractsCategoryFromClassificationLabelEvenWithVpnInReasoning() throws Exception {
        // Model returned "Classification: Infrastructure" but reasoning mentioned VPN
        final var responseJson = """
                {
                  "choices": [
                    {
                      "finish_reason": "stop",
                      "message": {
                        "content": "Classification: Infrastructure\\n\\nReasoning: The ASN 2, named UDEL-DCN (UNIVER-19-Z) appears to be an early, large-scale network identifier likely used by a major network infrastructure entity. It aligns with Infrastructure category (large carriers, Tier-1s, IXPs, route-servers) rather than end-user VPN/hosting/ISP/enterprise."
                      }
                    }
                  ]
                }
                """;
        server.enqueue(new MockResponse()
                .setResponseCode(200)
                .addHeader("Content-Type", "application/json")
                .setBody(responseJson));

        final var classifier = new OpenAiClassifier(HttpClient.newHttpClient(), new ObjectMapper(),
                server.url("/v1/").uri(), "gpt-5-nano", "test-key", Duration.ofSeconds(5));
        final var metadata = AsnMetadata.minimal(2, "UDEL-DCN", "US", "UNIVER-19-Z", "assigned");

        final var result = classifier.classify(metadata);

        // Should extract "Infrastructure" from "Classification: Infrastructure", NOT "VPN" from reasoning
        assertThat(result.category()).isEqualTo("Infrastructure");
    }
}
