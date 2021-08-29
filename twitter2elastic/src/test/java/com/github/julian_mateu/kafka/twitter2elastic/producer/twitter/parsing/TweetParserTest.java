package com.github.julian_mateu.kafka.twitter2elastic.producer.twitter.parsing;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;


class TweetParserTest {

    private TweetParser parser;

    @BeforeEach
    public void setUp() {
        parser = new TweetParser(new ObjectMapper());
    }

    @Test
    public void missingExpectedKeyThrows() {
        // Given
        String payload = "{\"delete\":{\"unknown\":\"value\"}}";

        // When
        IllegalArgumentException thrown = assertThrows(
                IllegalArgumentException.class,
                () -> parser.parseMessage(payload)
        );

        // Then
        assertEquals("Invalid message: " + payload, thrown.getMessage());
    }

    @Test
    public void missingIdThrows() {
        // Given
        String payload = "{\"unknown\":\"value\"}";

        // When
        IllegalArgumentException thrown = assertThrows(
                IllegalArgumentException.class,
                () -> parser.parseMessage(payload)
        );

        // Then
        assertEquals("Invalid message: " + payload, thrown.getMessage());
    }

    @Test
    public void parsesIdForNonDelete() {
        // Given
        String payload = "{\"id_str\":\"123\", \"other\":\"field\"}";

        // When
        Tweet result = parser.parseMessage(payload);

        // Then
        Tweet expected = Tweet.of("123", ImmutableMap.of("id_str", "123", "other", "field"), payload);
        assertEquals(expected, result);
    }

    @Test
    public void parsesIdDelete() {
        // Given
        String payload = "{\"delete\":{\"status\":{\"id_str\":\"123\", \"other\":\"field\"}}}";

        // When
        Tweet result = parser.parseMessage(payload);

        // Then
        Tweet expected = Tweet.of(
                "123",
                ImmutableMap.of(
                        "delete",
                        ImmutableMap.of("status", ImmutableMap.of("id_str", "123", "other", "field"))
                ),
                payload
        );
        assertEquals(expected, result);
    }
}