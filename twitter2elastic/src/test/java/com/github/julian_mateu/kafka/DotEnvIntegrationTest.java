package com.github.julian_mateu.kafka;

import io.github.cdimascio.dotenv.Dotenv;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Learning tests for the dotenv-java library.
 *
 * @see <a href="https://github.com/cdimascio/dotenv-java">dotenv-java</a>
 */
public class DotEnvIntegrationTest {

    @Test
    public void envLoads() {
        // Given
        Dotenv dotenv = Dotenv
                .configure()
                .filename("example.env")
                .load();

        // When
        String consumerKey = dotenv.get("API_KEY");
        String consumerSecret = dotenv.get("API_SECRET");
        String token = dotenv.get("ACCESS_TOKEN");
        String secret = dotenv.get("ACCESS_TOKEN_SECRET");

        // Then
        assertEquals("EXAMPLE_API_KEY", consumerKey);
        assertEquals("EXAMPLE_API_SECRET", consumerSecret);
        assertEquals("EXAMPLE_ACCESS_TOKEN", token);
        assertEquals("EXAMPLE_ACCESS_TOKEN_SECRET", secret);
    }
}
