package com.github.julian_mateu.kafka.twitter2elastic.producer;

import com.github.julian_mateu.kafka.twitter2elastic.producer.kafka.Producer;
import com.github.julian_mateu.kafka.twitter2elastic.producer.twitter.parsing.Tweet;
import com.github.julian_mateu.kafka.twitter2elastic.producer.twitter.parsing.TweetParser;
import com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.*;

class MessageProcessorTest {

    @Mock
    private Producer producer;
    @Mock
    private TweetParser parser;

    private MessageProcessor messageProcessor;

    @BeforeEach
    public void setUp() {
        MockitoAnnotations.initMocks(this);
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
        messageProcessor.processMessage(payload);

        // Then
        verify(producer, times(1)).sendMessage(eq(id), eq(payload));
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