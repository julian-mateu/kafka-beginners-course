package com.github.julian_mateu.kafka.twitter2elastic.producer;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import static org.mockito.Mockito.*;

class TwitterProducerApplicationTest {

    @Mock
    private TweetProducer tweetProducer;

    @BeforeEach
    public void setUp() {
        MockitoAnnotations.initMocks(this);
    }

    @Test
    public void run() {
        // Given
        TwitterProducerApplication application = new TwitterProducerApplication(tweetProducer);

        // When
        application.run();

        // Then
        verify(tweetProducer, times(1)).run(500);
        verifyNoMoreInteractions(tweetProducer);
    }

    @Test
    public void getInstance() {
        // Given

        // When
        TwitterProducerApplication.getInstance();

        // Then
    }
}