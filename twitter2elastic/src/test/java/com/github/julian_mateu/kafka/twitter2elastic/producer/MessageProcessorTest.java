package com.github.julian_mateu.kafka.twitter2elastic.producer;

import com.github.julian_mateu.kafka.twitter2elastic.producer.kafka.Producer;
import com.github.julian_mateu.kafka.twitter2elastic.producer.testutils.RecordMetadataFutureMock;
import com.github.julian_mateu.kafka.twitter2elastic.producer.twitter.parsing.Tweet;
import com.github.julian_mateu.kafka.twitter2elastic.producer.twitter.parsing.TweetParser;
import com.google.common.collect.ImmutableMap;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.util.Optional;
import java.util.concurrent.Future;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.*;

class MessageProcessorTest {

    @Mock
    private Producer producer;
    @Mock
    private TweetParser parser;
    @Mock
    private RecordMetadataFutureMock recordMetadataFutureMock;

    private MessageProcessor messageProcessor;

    @BeforeEach
    public void setUp() {
        MockitoAnnotations.initMocks(this);
        when(producer.sendMessage(anyString(), anyString())).thenReturn(recordMetadataFutureMock);
        messageProcessor = new MessageProcessor(producer, parser);
    }

    @AfterEach
    public void tearDown() throws Exception {
        messageProcessor.close();
    }

    @Test
    public void processMessage() {
        // Given
        String payload = "some_payload";
        String id = "id";
        when(parser.parseMessage(anyString()))
                .thenReturn(Tweet.of(id, ImmutableMap.of(), payload));

        // When
        Optional<Future<RecordMetadata>> result = messageProcessor.processMessage(payload);

        // Then
        assertEquals(Optional.of(recordMetadataFutureMock), result);
        verify(producer, times(1)).sendMessage(eq(id), eq(payload));
        verifyNoMoreInteractions(producer);
    }

    @Test
    public void processMessageReturnsEmptyOptionalOnParsingError() {
        // Given
        String payload = "some_payload";
        String id = "id";
        when(parser.parseMessage(anyString()))
                .thenThrow(new IllegalArgumentException());

        // When
        Optional<Future<RecordMetadata>> result = messageProcessor.processMessage(payload);

        // Then
        assertEquals(Optional.empty(), result);
        verifyNoMoreInteractions(producer);
    }

    @Test
    void close() throws Exception {
        // Given

        // When
        messageProcessor.close();

        // Then
        verify(producer, times(1)).close();
    }
}