package com.github.julian_mateu.kafka.twitter2elastic.producer;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import static com.github.julian_mateu.kafka.twitter2elastic.producer.TwitterProducerApplication.NUMBER_OF_MESSAGES_TO_WRITE;
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
        verify(tweetProducer, times(1)).run(NUMBER_OF_MESSAGES_TO_WRITE);
        verifyNoMoreInteractions(tweetProducer);
    }
}