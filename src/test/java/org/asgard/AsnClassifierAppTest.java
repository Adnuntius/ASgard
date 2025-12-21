package org.asgard;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class AsnClassifierAppTest {
    @Test
    void skipsUnknownsWhenNotAccepted() {
        final var unknown = new FinalClassification(728, "Unknown", "Unknown", "Unknown");
        assertThat(AsnClassifierApp.shouldSkipUnknowns(unknown, false)).isTrue();
    }

    @Test
    void acceptsUnknownsWhenEnabled() {
        final var unknown = new FinalClassification(728, "Unknown", "Unknown", "Unknown");
        assertThat(AsnClassifierApp.shouldSkipUnknowns(unknown, true)).isFalse();
    }
}
