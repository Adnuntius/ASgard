package org.asgard;

import org.junit.jupiter.api.Test;

import java.net.http.HttpClient;
import java.time.Duration;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class AsnRegistryClientTest {

    @Test
    void parsesOnlyAllocatedAndAssignedAsns() {
        var client = new AsnRegistryClient(HttpClient.newHttpClient(), List.of(), Duration.ofSeconds(1));
        var sample = """
                ripencc|NL|asn|123|2|20230901|allocated
                apnic|AU|asn|456|1|20230902|assigned
                apnic|AU|asn|789|*|20230903|available
                # comment
                """;

        var allocations = client.parseAllocations(sample);

        assertThat(allocations).hasSize(2);
        assertThat(allocations.get(0).startAsn()).isEqualTo(123);
        assertThat(allocations.get(0).endAsn()).isEqualTo(124);
    }

    @Test
    void expandsAllocations() {
        var allocations = List.of(new AsnAllocation(65000, 3, "arin", "US", "allocated", null));
        var expanded = AsnRegistryClient.expandAllocations(allocations).toArray();
        assertThat(expanded).containsExactly(65000, 65001, 65002);
    }
}
