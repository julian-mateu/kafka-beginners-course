package com.github.julian_mateu.kafka;

import lombok.NonNull;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.mockito.Mockito.*;


class TwitterProducerApplicationTest {

    @Mock
    private TweetProducer tweetProducer;
    @Mock
    private FutureMock futureMock;

    @BeforeEach
    void setUp() {
        MockitoAnnotations.initMocks(this);
    }

    @Test
    void run() throws ExecutionException, InterruptedException {
        // Given
        TwitterProducerApplication application = new TwitterProducerApplication(tweetProducer);
        when(tweetProducer.readAndProduce())
                .thenReturn(Optional.of(
                        futureMock
                ));

        // When
        application.run();

        // Then
        verify(tweetProducer, times(5)).readAndProduce();
        verifyNoMoreInteractions(tweetProducer);
        verify(futureMock, times(5)).get();
        verifyNoMoreInteractions(futureMock);
    }

    private static class FutureMock implements Future<RecordMetadata> {

        @Override
        public boolean cancel(boolean mayInterruptIfRunning) {
            return false;
        }

        @Override
        public boolean isCancelled() {
            return false;
        }

        @Override
        public boolean isDone() {
            return false;
        }

        @Override
        public RecordMetadata get() {
            return null;
        }

        @Override
        public RecordMetadata get(long timeout, @NonNull TimeUnit unit) {
            return null;
        }
    }
}