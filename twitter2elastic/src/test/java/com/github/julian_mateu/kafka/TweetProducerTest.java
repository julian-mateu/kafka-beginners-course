package com.github.julian_mateu.kafka;


import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.util.Optional;

import static org.mockito.Mockito.*;

class TweetProducerTest {
    @Mock
    private TwitterMessageReader reader;
    @Mock
    private MessageProcessor processor;

    @BeforeEach
    public void setUp() {
        MockitoAnnotations.initMocks(this);
    }

    @Test
    public void readAndProduce() {
        // Given
        TweetProducer tweetProducer = new TweetProducer(reader, processor);
        when(reader.readMessage()).thenReturn(Optional.of("something"));

        // when
        tweetProducer.readAndProduce();

        // Then
        verify(processor, times(1)).processMessage(eq("something"));
        verify(reader, times(1)).readMessage();
        verifyNoMoreInteractions(processor, reader);
    }

}