package com.github.julian_mateu.kafka.twitter2elastic.producer;

import com.github.julian_mateu.kafka.twitter2elastic.producer.kafka.Producer;
import com.github.julian_mateu.kafka.twitter2elastic.producer.kafka.ProducerFactory;
import com.github.julian_mateu.kafka.twitter2elastic.producer.twitter.TwitterMessageReaderFactory;
import com.github.julian_mateu.kafka.twitter2elastic.producer.twitter.parsing.TweetParser;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.*;

public class ResourceManagerTest {

    @Mock
    private TwitterMessageReaderFactory twitterMessageReaderFactory;
    @Mock
    private ProducerFactory producerFactory;
    @Mock
    private Producer producer;
    @Mock
    private TweetParser tweetParser;

    private TweetProducer.SimpleResourceManager resourceManager;

    @BeforeEach
    public void setUp() {
        MockitoAnnotations.initMocks(this);
        when(producerFactory.getProducer()).thenReturn(producer);

        resourceManager = new TweetProducer.SimpleResourceManager(
                twitterMessageReaderFactory,
                producerFactory,
                tweetParser
        );
    }

    @Test
    public void getReader() {
        // Given

        // When
        resourceManager.getReader();

        // Then
        verify(twitterMessageReaderFactory, times(1)).getTwitterMessageReader();
        verifyNoMoreInteractions(twitterMessageReaderFactory);
    }

    @Test
    public void getMessageProcessor() {
        // Given

        // When
        MessageProcessor messageProcessor = resourceManager.getMessageProcessor();

        // Then
        assertEquals(new MessageProcessor(producer, tweetParser), messageProcessor);
    }
}
