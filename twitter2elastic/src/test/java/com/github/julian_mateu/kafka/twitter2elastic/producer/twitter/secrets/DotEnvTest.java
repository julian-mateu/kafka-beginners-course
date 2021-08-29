package com.github.julian_mateu.kafka.twitter2elastic.producer.twitter.secrets;

import io.github.cdimascio.dotenv.Dotenv;
import org.junit.jupiter.api.Test;

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
        Dotenv dotenv = Dotenv
                .configure()
                .filename("example.env")
                .load();

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
}
