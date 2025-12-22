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
    void throwsWhenResponseIsTruncatedAndEmpty() {
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
                .hasMessageContaining("Empty");
        assertThat(server.getRequestCount()).isEqualTo(1);
    }

    @Test
    void extractsCategoryFromTruncatedResponseWhenCategoryPresent() throws Exception {
        // Model wrote the category before getting cut off
        final var truncated = """
                {
                  "choices": [
                    {
                      "finish_reason": "length",
                      "message": {
                        "content": "Enterprise\\n\\nReasoning: This ASN belongs to a corporate entity that operates..."
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
        final var metadata = AsnMetadata.minimal(100, "CORP-AS", "US", "Some Corp", "assigned");

        final var result = classifier.classify(metadata);

        assertThat(result.category()).isEqualTo("Enterprise");
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

    @Test
    void extractsCategoryFromDashCategoryColonFormat() throws Exception {
        // Model lists the category with its definition: "- Hosting: Datacenters, cloud providers..."
        final var responseJson = """
                {
                  "choices": [
                    {
                      "finish_reason": "stop",
                      "message": {
                        "content": "Based on the information provided, classify ASN 567 as:\\n\\n- Hosting: Datacenters, cloud providers, VPS/bare-metal/colo, CDN\\n\\nReasoning: ASN 567 is registered to \\"LNET\\" in the US with a long-standing assignment (1990). While the exact service scope isn't specified, many early ASNs with such naming (LNET) typically align with hosting or data-center infrastructure. Therefore, the most fitting single category is Hosting."
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
        final var metadata = AsnMetadata.minimal(567, "ASN-LNET-AS", "US", "LNET", "assigned");

        final var result = classifier.classify(metadata);

        // Should extract "Hosting" from "- Hosting:" pattern
        assertThat(result.category()).isEqualTo("Hosting");
    }

    @Test
    void retriesOnTransientErrors() throws Exception {
        // First request fails with 503, second succeeds
        server.enqueue(new MockResponse().setResponseCode(503).setBody("Service Unavailable"));
        server.enqueue(new MockResponse()
                .setResponseCode(200)
                .addHeader("Content-Type", "application/json")
                .setBody("""
                        {"choices": [{"message": {"content": "ISP"}}]}
                        """));

        final var classifier = new OpenAiClassifier(HttpClient.newHttpClient(), new ObjectMapper(),
                server.url("/v1/").uri(), "gpt-5-nano", "test-key", Duration.ofSeconds(5));
        final var metadata = AsnMetadata.minimal(100, "TEST-AS", "US", "Test Org", "assigned");

        final var result = classifier.classify(metadata);

        assertThat(result.category()).isEqualTo("ISP");
        assertThat(server.getRequestCount()).isEqualTo(2);
    }

    @Test
    void retriesWithMoreTokensWhenTruncatedWithNoCategory() throws Exception {
        // First response is truncated with no recognizable category (just reasoning text)
        final var truncatedNoCategory = """
                {
                  "choices": [
                    {
                      "finish_reason": "length",
                      "message": {
                        "content": "This ASN appears to be associated with a telecommunications provider that offers..."
                      }
                    }
                  ]
                }
                """;
        // Second response with more tokens succeeds
        final var successResponse = """
                {
                  "choices": [
                    {
                      "finish_reason": "stop",
                      "message": {
                        "content": "Category: ISP\\n\\nReasoning: This ASN is a telecommunications provider."
                      }
                    }
                  ]
                }
                """;
        server.enqueue(new MockResponse()
                .setResponseCode(200)
                .addHeader("Content-Type", "application/json")
                .setBody(truncatedNoCategory));
        server.enqueue(new MockResponse()
                .setResponseCode(200)
                .addHeader("Content-Type", "application/json")
                .setBody(successResponse));

        final var classifier = new OpenAiClassifier(HttpClient.newHttpClient(), new ObjectMapper(),
                server.url("/v1/").uri(), "gpt-5-nano", "test-key", Duration.ofSeconds(5));
        final var metadata = AsnMetadata.minimal(2541, "TEST-TELCO", "US", "Telco Corp", "assigned");

        final var result = classifier.classify(metadata);

        assertThat(result.category()).isEqualTo("ISP");
        assertThat(server.getRequestCount()).isEqualTo(2);

        // Verify first request used default tokens
        final var firstRequest = server.takeRequest();
        final var firstBody = new ObjectMapper().readTree(firstRequest.getBody().readUtf8());
        assertThat(firstBody.path("max_completion_tokens").asInt()).isEqualTo(COMPLETION_TOKENS);

        // Verify second request used extended tokens
        final var secondRequest = server.takeRequest();
        final var secondBody = new ObjectMapper().readTree(secondRequest.getBody().readUtf8());
        assertThat(secondBody.path("max_completion_tokens").asInt()).isEqualTo(512);
    }
}
