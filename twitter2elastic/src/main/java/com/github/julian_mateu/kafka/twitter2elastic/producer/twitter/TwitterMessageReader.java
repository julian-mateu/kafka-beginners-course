package com.github.julian_mateu.kafka.twitter2elastic.producer.twitter;

import com.twitter.hbc.httpclient.BasicClient;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

import java.util.Optional;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * Uses the HoseBird Client {@link BasicClient} to get messages from the Twitter API.
 */
@RequiredArgsConstructor
@Slf4j
public class TwitterMessageReader implements AutoCloseable {

    @NonNull
    private final BlockingQueue<String> queue;
    @NonNull
    private final BasicClient client;

    /**
     * Closes the {@link BasicClient}.
     */
    @Override
    public void close() {
        client.stop();
        long readMessages = client.getStatsTracker().getNumMessages();
        log.debug("The client read {} messages!\n", readMessages);
    }

    /**
     * Reads a tweet as a JSON String from the Twitter API.
     *
     * @return An {@link Optional} {@link String}
     */
    public Optional<String> readMessage() {
        checkClientIsAvailable();
        return getMessageFromQueue();
    }

    private void checkClientIsAvailable() {
        if (client.isDone()) {
            throw new IllegalStateException(
                    "Client connection closed unexpectedly: " + client.getExitEvent().getMessage());
        }
    }

    @SneakyThrows(InterruptedException.class)
    private Optional<String> getMessageFromQueue() {
        String message = queue.poll(5, TimeUnit.SECONDS);
        log.debug(message == null ? "Did not receive a message in 5 seconds" : message);
        return Optional.ofNullable(message);
    }
}
