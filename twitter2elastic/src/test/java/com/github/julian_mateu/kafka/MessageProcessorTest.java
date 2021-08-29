package com.github.julian_mateu.kafka;

import com.google.common.collect.ImmutableMap;
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

    @BeforeEach
    public void setUp() {
        MockitoAnnotations.initMocks(this);
    }

    @Test
    public void processMessage() {
        // Given
        String payload = "some_payload";
        String id = "id";
        MessageProcessor messageProcessor = new MessageProcessor(producer, parser);
        when(parser.parseMessage(anyString()))
                .thenReturn(Tweet.of(id, ImmutableMap.of(), payload));

        // When
        messageProcessor.processMessage(payload);

        // Then
        verify(producer, times(1)).sendMessage(eq(id), eq(payload));
        verifyNoMoreInteractions(producer);
    }
}