package com.github.julian_mateu.kafka;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.endpoint.StatusesSampleEndpoint;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.BasicClient;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;
import io.github.cdimascio.dotenv.Dotenv;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * Integration tests to run against the real Twitter API, and learning tests for the Hosebird Client library.
 *
 * @see <a href="https://github.com/twitter/hbc">Hosebird Client</a>
 */
@Slf4j
public class HoseBirdIntegrationTests {

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    private Secrets secrets;
    private BlockingQueue<String> queue;
    private BasicClient client;

    private static Map<String, Object> getPayloadAsMap(String message) throws JsonProcessingException {
        return OBJECT_MAPPER.readValue(message, new TypeReference<Map<String, Object>>() {
        });
    }

    @BeforeEach
    public void setup() {
        loadSecrets();
        createClientAndQueue();
    }

    @AfterEach
    public void cleanup() {
        client.stop();
    }

    private Optional<String> readMessage() throws InterruptedException {
        if (client.isDone()) {
            fail("Client connection closed unexpectedly: " + client.getExitEvent().getMessage());
        }

        String message = queue.poll(5, TimeUnit.SECONDS);
        if (message == null) {
            log.debug("Did not receive a message in 5 seconds");
            return Optional.empty();
        } else {
            log.debug(message);
            return Optional.of(message);
        }
    }

    @Test
    public void readMessages() throws InterruptedException {
        // Given

        // When
        int actualMessages = 0;
        for (int messagesRead = 0; messagesRead < 10; messagesRead++) {
            Optional<String> message = readMessage();
            if (message.isPresent()) {
                assertMessageContainsTimestamp(message.get());
                actualMessages++;
            }
        }

        // Then
        long readMessages = client.getStatsTracker().getNumMessages();
        log.debug("The client read {} messages!\n", readMessages);

        assertNotEquals(0, readMessages);
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
        Map<String, Object> payload = getPayloadAsMap(message);
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

    private void createClientAndQueue() {
        queue = new LinkedBlockingQueue<>(10000);

        StatusesSampleEndpoint endpoint = new StatusesSampleEndpoint();
        endpoint.stallWarnings(false);

        Authentication auth = new OAuth1(
                secrets.getConsumerKey(),
                secrets.getConsumerSecret(),
                secrets.getToken(),
                secrets.getSecret()
        );

        client = new ClientBuilder()
                .name("integrationTestClient")
                .hosts(Constants.STREAM_HOST)
                .endpoint(endpoint)
                .authentication(auth)
                .processor(new StringDelimitedProcessor(queue))
                .build();

        client.connect();
    }

    private void loadSecrets() {
        Dotenv dotenv = Dotenv
                .configure()
                .load();

        secrets = new SecretsLoader(dotenv).loadSecrets();
    }
}
