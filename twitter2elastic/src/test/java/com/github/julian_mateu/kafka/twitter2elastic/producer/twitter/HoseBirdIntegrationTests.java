package com.github.julian_mateu.kafka.twitter2elastic.producer.twitter;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.julian_mateu.kafka.twitter2elastic.producer.twitter.parsing.TweetParser;
import com.github.julian_mateu.kafka.twitter2elastic.producer.twitter.secrets.SecretsLoader;
import com.google.common.collect.ImmutableList;
import io.github.cdimascio.dotenv.Dotenv;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * Integration tests to run against the real Twitter API, and learning tests for the Hosebird Client library.
 *
 * @see <a href="https://github.com/twitter/hbc">Hosebird Client</a>
 */
@Slf4j
public class HoseBirdIntegrationTests {

    private static final String CLIENT_NAME = "integrationTestClient";
    private static final int QUEUE_CAPACITY = 10000;
    private static final ImmutableList<String> SEARCH_TERMS = ImmutableList.of("bitcoin");

    private static final TweetParser PARSER = new TweetParser(new ObjectMapper());
    private static final Dotenv DOTENV = Dotenv
            .configure()
            .load();
    private static final TwitterMessageReaderFactory TWITTER_MESSAGE_READER_FACTORY = new TwitterMessageReaderFactory(
            QUEUE_CAPACITY, CLIENT_NAME, new SecretsLoader(DOTENV), SEARCH_TERMS
    );

    private TwitterMessageReader twitterMessageReader;

    @BeforeEach
    public void setup() {
        twitterMessageReader = TWITTER_MESSAGE_READER_FACTORY.getTwitterMessageReader();
    }

    @AfterEach
    public void cleanup() {
        twitterMessageReader.close();
    }

    @Test
    public void readMessages() {
        // Given

        // When
        int actualMessages = 0;
        for (int messagesRead = 0; messagesRead < 10; messagesRead++) {
            Optional<String> message = twitterMessageReader.readMessage();
            if (message.isPresent()) {
                assertMessageContainsTimestamp(message.get());
                actualMessages++;
            }
        }

        // Then
        assertNotEquals(0, actualMessages);
    }

    private void assertMessageContainsTimestamp(String message) {
        try {
            assertMessageContainsTimestampChecked(message);
        } catch (JsonProcessingException e) {
            e.printStackTrace();
        }
    }

    @SuppressWarnings("unchecked")
    private void assertMessageContainsTimestampChecked(String message) throws JsonProcessingException {
        Map<String, Object> payload = PARSER.parseMessage(message).getPayload();
        if (!payload.containsKey("timestamp_ms")) {
            if (payload.containsKey("delete")) {
                if (!((Map<String, Object>) payload.get("delete")).containsKey("timestamp_ms")) {
                    fail("Payload \"delete\" does not contain a timestamp_ms field: " + payload.get("delete"));
                }
            } else {
                fail("Payload does not contain a timestamp_ms field: " + message);
            }
        }
    }
}
