package com.github.julian_mateu.kafka.twitter2elastic.producer.twitter.parsing;


import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.when;

class TweetParserTestMock {

    @Mock
    private ObjectMapper objectMapper;

    private TweetParser parser;

    @BeforeEach
    public void setUp() {
        MockitoAnnotations.initMocks(this);

        parser = new TweetParser(objectMapper);
    }

    @Test
    public void wrapsJsonProcessingException() throws JsonProcessingException {
        // Given
        String expectedMessage = "someError";
        JsonProcessingException cause = new JsonProcessingException(expectedMessage) {
        };

        when(objectMapper.readValue(
                anyString(),
                any(new TypeReference<Map<String, Object>>() {
                }.getClass()))
        )
                .thenThrow(cause);

        // When
        IllegalArgumentException thrown = assertThrows(
                IllegalArgumentException.class,
                () -> parser.parseMessage("originalMessage")
        );

        // Then
        assertEquals("Invalid message: originalMessage", thrown.getMessage());
    }
}