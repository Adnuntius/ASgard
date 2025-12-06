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

class RdapClientTest {
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
    void extractsHighSignalFieldsForClassification() throws Exception {
        server.enqueue(new MockResponse()
                .setResponseCode(200)
                .addHeader("Content-Type", "application/rdap+json")
                .setBody(sampleRdap()));
        final var client = new RdapClient(HttpClient.newHttpClient(), new ObjectMapper(), server.url("/").uri(),
                Duration.ofSeconds(5));

        final var metadata = client.lookup(1).orElseThrow();

        assertThat(metadata.handle()).isEqualTo("AS1");
        assertThat(metadata.startAutnum()).isEqualTo(1);
        assertThat(metadata.endAutnum()).isEqualTo(1);
        assertThat(metadata.statuses()).contains("active");
        assertThat(metadata.registrationDate()).startsWith("2001-09-20");
        assertThat(metadata.lastChangedDate()).startsWith("2024-06-18");
        assertThat(metadata.registry()).isEqualTo("whois.arin.net");

        final var registrant = metadata.registrant();
        assertThat(registrant.roles()).contains("registrant");
        assertThat(registrant.name()).isEqualTo("Level 3 Parent, LLC");
        assertThat(registrant.address()).contains("Monroe").contains("United States");
        assertThat(registrant.remarks().get(0)).contains("USAGE OF IP SPACE");

        assertThat(metadata.contacts())
                .extracting(AsnMetadata.Entity::roles)
                .anySatisfy(roles -> assertThat(roles).contains("abuse"));
        assertThat(metadata.contacts())
                .flatExtracting(AsnMetadata.Entity::emails)
                .anySatisfy(email -> assertThat(email).contains("lumen.com"));
    }

    @Test
    void retriesTransientFailures() throws Exception {
        server.enqueue(new MockResponse().setResponseCode(504));
        server.enqueue(new MockResponse()
                .setResponseCode(200)
                .addHeader("Content-Type", "application/rdap+json")
                .setBody(sampleRdap()));

        final var client = new RdapClient(HttpClient.newHttpClient(), new ObjectMapper(), server.url("/").uri(),
                Duration.ofSeconds(2));

        final var metadata = client.lookup(1).orElseThrow();

        assertThat(metadata.handle()).isEqualTo("AS1");
        assertThat(server.getRequestCount()).isEqualTo(2);
    }

    private String sampleRdap() {
        return """
                {
                  "handle" : "AS1",
                  "startAutnum" : 1,
                  "endAutnum" : 1,
                  "name" : "LVLT-1",
                  "status" : [ "active" ],
                  "port43" : "whois.arin.net",
                  "events" : [ {
                    "eventAction" : "last changed",
                    "eventDate" : "2024-06-18T15:47:14-04:00"
                  }, {
                    "eventAction" : "registration",
                    "eventDate" : "2001-09-20T00:00:00-04:00"
                  } ],
                  "entities" : [ {
                    "handle" : "LPL-141",
                    "roles" : [ "registrant" ],
                    "vcardArray" : [ "vcard", [ [ "fn", { }, "text", "Level 3 Parent, LLC" ], [ "kind", { }, "text", "org" ], [ "adr", {
                      "label" : "100 CenturyLink Drive\\nMonroe\\nLA\\n71203\\nUnited States"
                    }, "text", [ "", "", "", "", "", "", "" ] ] ] ],
                    "remarks" : [ {
                      "description" : [ "USAGE OF IP SPACE MUST COMPLY WITH OUR ACCEPTABLE USE POLICY:", "https://www.lumen.com/en-us/about/legal/acceptable-use-policy.html", "Non-portable space | looking glass https://lookingglass.centurylink.com/" ]
                    } ]
                  }, {
                    "handle" : "LAC56-ARIN",
                    "roles" : [ "abuse" ],
                    "vcardArray" : [ "vcard", [ [ "fn", { }, "text", "L3 Abuse Contact" ], [ "org", { }, "text", "L3 Abuse Contact" ], [ "kind", { }, "text", "group" ], [ "email", { }, "text", "abuse@level3.com" ], [ "tel", {
                      "type" : [ "work", "voice" ]
                    }, "text", "+1-877-453-8353" ] ] ],
                    "status" : [ "validated" ]
                  }, {
                    "handle" : "APL7-ARIN",
                    "roles" : [ "administrative", "technical" ],
                    "vcardArray" : [ "vcard", [ [ "fn", { }, "text", "ADMIN POC LVLT" ], [ "org", { }, "text", "ADMIN POC LVLT" ], [ "kind", { }, "text", "group" ], [ "email", { }, "text", "ipadmin@lumen.com" ], [ "adr", {
                      "label" : "1025 Eldorado Blvd.\\nBroomfield\\nCO\\n80021\\nUnited States"
                    }, "text", [ "", "", "", "", "", "", "" ] ] ] ]
                  } ]
                }
                """;
    }
}
