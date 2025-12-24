package org.asgard;

import com.fasterxml.jackson.databind.ObjectMapper;
import okhttp3.mockwebserver.MockResponse;
import okhttp3.mockwebserver.MockWebServer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.net.http.HttpClient;
import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;

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
        assertThat(json.path("max_completion_tokens").asInt()).isEqualTo(512);
        final var content = json.path("messages").get(1).path("content").asText();
        assertThat(content).contains("ASN: 64512");
        assertThat(content).contains("Name: Example ISP");
        assertThat(content).contains("Kind: isp");
    }

    @Test
    void extractsCategoryFromAnswerFormat() throws Exception {
        final var responseJson = """
                {
                  "choices": [
                    {
                      "finish_reason": "stop",
                      "message": {
                        "content": "The ASN belongs to MIT, a major educational and research institution. It fits Infrastructure (large networks, IXPs) rather than hosting, VPN, ISP, or enterprise.\\n\\nANSWER: Infrastructure"
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
    void extractsCategoryFromAnswerEvenWithOtherCategoriesInReasoning() throws Exception {
        // Reasoning mentions VPN/hosting/ISP/enterprise but ANSWER is Infrastructure
        final var responseJson = """
                {
                  "choices": [
                    {
                      "finish_reason": "stop",
                      "message": {
                        "content": "The ASN 2, named UDEL-DCN appears to be an early, large-scale network. It aligns with Infrastructure rather than end-user VPN/hosting/ISP/enterprise.\\n\\nANSWER: Infrastructure"
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

        assertThat(result.category()).isEqualTo("Infrastructure");
    }

    @Test
    void fallbackFindsLastCategoryMentioned() throws Exception {
        // When no ANSWER: label, fallback finds the last category word in text
        final var responseJson = """
                {
                  "choices": [
                    {
                      "finish_reason": "stop",
                      "message": {
                        "content": "This could be Hosting or ISP. After analysis, definitely ISP."
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
        final var metadata = AsnMetadata.minimal(100, "TEST-AS", "US", "Test Org", "assigned");

        final var result = classifier.classify(metadata);

        assertThat(result.category()).isEqualTo("ISP");
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
    void uses512CompletionTokens() throws Exception {
        final var responseJson = """
                {
                  "choices": [
                    {
                      "message": {
                        "content": "ANSWER: ISP"
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
        final var metadata = AsnMetadata.minimal(100, "TEST-AS", "US", "Test Org", "assigned");

        classifier.classify(metadata);

        final var request = server.takeRequest();
        final var body = new ObjectMapper().readTree(request.getBody().readUtf8());
        assertThat(body.path("max_completion_tokens").asInt()).isEqualTo(512);
    }
}
