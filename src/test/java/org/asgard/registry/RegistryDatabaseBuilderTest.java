package org.asgard.registry;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class RegistryDatabaseBuilderTest {
    @Test
    void overrideWinsWithoutPrompt() throws Exception {
        final var prompted = new AtomicBoolean(false);

        final var resolved = RegistryDatabaseBuilder.selectArinApiKey(
                "override",
                "config",
                "env",
                () -> {
                    prompted.set(true);
                    return "prompt";
                });

        assertThat(resolved).isEqualTo("override");
        assertThat(prompted.get()).isFalse();
    }

    @Test
    void configBeatsEnvWhenOverrideMissing() throws Exception {
        final var resolved = RegistryDatabaseBuilder.selectArinApiKey(
                null,
                "config",
                "env",
                () -> "prompt");

        assertThat(resolved).isEqualTo("config");
    }

    @Test
    void envBeatsPromptWhenOverrideAndConfigMissing() throws Exception {
        final var prompted = new AtomicBoolean(false);

        final var resolved = RegistryDatabaseBuilder.selectArinApiKey(
                null,
                "",
                "env",
                () -> {
                    prompted.set(true);
                    return "prompt";
                });

        assertThat(resolved).isEqualTo("env");
        assertThat(prompted.get()).isFalse();
    }

    @Test
    void promptUsedWhenNoOtherKeyProvided() throws Exception {
        final var prompted = new AtomicBoolean(false);

        final var resolved = RegistryDatabaseBuilder.selectArinApiKey(
                null,
                null,
                null,
                () -> {
                    prompted.set(true);
                    return "prompted";
                });

        assertThat(resolved).isEqualTo("prompted");
        assertThat(prompted.get()).isTrue();
    }

    @Test
    void blankPromptFails() {
        assertThatThrownBy(() -> RegistryDatabaseBuilder.selectArinApiKey(
                null,
                null,
                null,
                () -> " "))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("Invalid ARIN API key");
    }

    @Test
    void invalidConfigFallsBackToPrompt() throws Exception {
        final var resolved = RegistryDatabaseBuilder.selectArinApiKey(
                null,
                "bad key",
                null,
                () -> "prompted");

        assertThat(resolved).isEqualTo("prompted");
    }

    @Test
    void invalidOverrideFailsFast() {
        assertThatThrownBy(() -> RegistryDatabaseBuilder.selectArinApiKey(
                "bad key",
                "config",
                "env",
                () -> "prompted"))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("--arin-api-key");
    }
}
