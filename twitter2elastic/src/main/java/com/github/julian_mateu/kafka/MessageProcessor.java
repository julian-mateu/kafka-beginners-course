package com.github.julian_mateu.kafka;

import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.concurrent.Future;

/**
 * Processes a Message representing a tweet. It parses a string to a {@link Tweet} object and sends the relevant fields
 * to the {@link Producer}.
 */
@Slf4j
@RequiredArgsConstructor
public class MessageProcessor {

    @NonNull
    private final Producer producer;
    @NonNull
    private final TweetParser parser;

    /**
     * Process a message by parsing it into a {@link Tweet} and sending it as a message to the {@link Producer}.
     *
     * @param message message to parse
     * @return A {@link Future} of a {@link RecordMetadata} instance
     */
    public Future<RecordMetadata> processMessage(@NonNull String message) {
        Tweet tweet = parser.parseMessage(message);
        return producer.sendMessage(tweet.getId(), tweet.getPayloadString());
    }
}
