package com.github.julian_mateu.kafka.twitter2elastic.producer.twitter;

import com.twitter.hbc.core.StatsReporter;
import com.twitter.hbc.core.event.Event;
import com.twitter.hbc.httpclient.BasicClient;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.util.Optional;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.*;

public class TwitterMessageReaderTest {

    @Mock
    private BlockingQueue<String> queue;
    @Mock
    private BasicClient client;

    private TwitterMessageReader twitterMessageReader;

    @BeforeEach
    public void initMocks() {
        MockitoAnnotations.initMocks(this);

        when(client.getStatsTracker()).thenReturn(mock(StatsReporter.StatsTracker.class));
        when(client.getExitEvent()).thenReturn(mock(Event.class));
        when(client.isDone()).thenReturn(false);

        twitterMessageReader = new TwitterMessageReader(queue, client);
    }

    @Test
    public void throwsIfClientIsDone() {
        // Given
        when(client.isDone()).thenReturn(true);

        // When
        IllegalStateException thrown = assertThrows(IllegalStateException.class, twitterMessageReader::readMessage);

        // Then
        String expectedMessage = "Client connection closed unexpectedly: ";
        assertTrue(thrown.getMessage().contains(expectedMessage),
                "Exception message is unexpected, expected: " + expectedMessage
                        + " . but got: " + thrown.getMessage());
    }

    @Test
    public void wrapsQueueInterruptedException() throws InterruptedException {
        // Given
        String expectedMessage = "Some expected message";
        when(queue.poll(anyInt(), eq(TimeUnit.SECONDS))).thenThrow(new InterruptedException(expectedMessage));

        // When
        IllegalStateException thrown = assertThrows(IllegalStateException.class, twitterMessageReader::readMessage);

        // Then
        assertTrue(thrown.getMessage().contains(expectedMessage),
                "Exception message is unexpected, expected: " + expectedMessage
                        + " . but got: " + thrown.getMessage());
    }

    @Test
    public void returnsEmptyWhenMessageIsNull() throws InterruptedException {
        // Given
        when(queue.poll(anyInt(), eq(TimeUnit.SECONDS))).thenReturn(null);

        // When
        Optional<String> result = twitterMessageReader.readMessage();

        // Then
        assertEquals(Optional.empty(), result);
    }

    @Test
    public void returnsWrappedMessage() throws InterruptedException {
        // Given
        String expectedMessage = "Some Message";
        when(queue.poll(anyInt(), eq(TimeUnit.SECONDS))).thenReturn(expectedMessage);

        // When
        Optional<String> result = twitterMessageReader.readMessage();

        // Then
        assertEquals(Optional.of(expectedMessage), result);
    }

    @Test
    public void closeStopsClient() {
        // Given

        // When
        twitterMessageReader.close();

        // Then
        verify(client, times(1)).stop();
    }
}
