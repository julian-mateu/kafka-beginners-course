package com.github.julian_mateu.kafka.twitter2elastic.producer.twitter.secrets;

import io.github.cdimascio.dotenv.Dotenv;
import io.github.cdimascio.dotenv.DotenvEntry;
import org.junit.jupiter.api.Test;

import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Learning tests for the dotenv-java library.
 *
 * @see <a href="https://github.com/cdimascio/dotenv-java">dotenv-java</a>
 */
public class DotEnvTest {

    @Test
    public void envLoads() {
        // Given
        Dotenv dotenv = new FileOnlyDotenv(Dotenv
                .configure()
                .filename("example.env")
                .load()
                .entries(Dotenv.Filter.DECLARED_IN_ENV_FILE)
        );

        // When
        Secrets result = new SecretsLoader(dotenv).loadSecrets();

        // Then
        Secrets expected = Secrets.of(
                "EXAMPLE_API_KEY",
                "EXAMPLE_API_SECRET",
                "EXAMPLE_ACCESS_TOKEN",
                "EXAMPLE_ACCESS_TOKEN_SECRET"
        );
        assertEquals(expected, result);
    }

    /**
     * Helper class to filter only variables defined in the dotenv file and not system env variables.
     */
    static class FileOnlyDotenv implements Dotenv {

        final Set<DotenvEntry> entries;

        FileOnlyDotenv(Set<DotenvEntry> entries) {
            this.entries = entries;
        }

        @Override
        public Set<DotenvEntry> entries() {
            return entries;
        }

        @Override
        public Set<DotenvEntry> entries(Filter filter) {
            return entries;
        }

        @Override
        public String get(String key) {
            return entries.stream()
                    .filter(e -> e.getKey().equals(key))
                    .findFirst()
                    .map(DotenvEntry::getValue)
                    .orElse(null);
        }

        @Override
        public String get(String key, String defaultValue) {
            return entries.stream()
                    .filter(e -> e.getKey().equals(key))
                    .findFirst()
                    .map(DotenvEntry::getValue)
                    .orElse(defaultValue);
        }
    }
}
