package com.github.julian_mateu.kafka.twitter2elastic.producer;

import com.github.julian_mateu.kafka.twitter2elastic.producer.twitter.TwitterMessageReader;
import lombok.NonNull;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.util.Optional;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.mockito.Mockito.*;

class TweetProducerTest {
    @Mock
    private TwitterMessageReader reader;
    @Mock
    private MessageProcessor processor;
    @Mock
    private TweetProducer.SimpleResourceManager resourceManager;
    @Mock
    private FutureMock futureMock;

    @BeforeEach
    public void setUp() {
        MockitoAnnotations.initMocks(this);
        when(resourceManager.getMessageProcessor()).thenReturn(processor);
        when(resourceManager.getReader()).thenReturn(reader);
    }

    @Test
    public void readAndProduce() throws Exception {
        // Given
        TweetProducer tweetProducer = new TweetProducer(resourceManager);
        when(reader.readMessage()).thenReturn(Optional.of("something"));
        when(processor.processMessage(anyString())).thenReturn(futureMock);

        // when
        tweetProducer.run(1);

        // Then
        verify(processor, times(1)).processMessage(eq("something"));
        verify(processor, times(1)).close();
        verify(reader, times(1)).readMessage();
        verify(reader, times(1)).close();
        verifyNoMoreInteractions(processor, reader);
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