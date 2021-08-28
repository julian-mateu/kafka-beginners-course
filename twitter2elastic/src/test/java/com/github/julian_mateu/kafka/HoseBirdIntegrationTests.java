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
import lombok.Value;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.fail;

public class HoseBirdIntegrationTests {

    private static final Logger LOGGER = LoggerFactory.getLogger(HoseBirdIntegrationTests.class);
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

    @Test
    public void readMessages() throws InterruptedException, JsonProcessingException {
        for (int messagesRead = 0; messagesRead < 10; messagesRead++) {
            if (client.isDone()) {
                fail("Client connection closed unexpectedly: " + client.getExitEvent().getMessage());
                break;
            }

            String message = queue.poll(5, TimeUnit.SECONDS);
            if (message == null) {
                LOGGER.debug("Did not receive a message in 5 seconds");
            } else {
                LOGGER.debug(message);
                assertMessageContainsTimestamp(message);
            }
        }

        LOGGER.debug("The client read {} messages!\n", client.getStatsTracker().getNumMessages());
    }

    @SuppressWarnings("unchecked")
    private void assertMessageContainsTimestamp(String message) throws JsonProcessingException {
        Map<String, Object> payload = getPayloadAsMap(message);
        if (!payload.containsKey("timestamp_ms")) {
            if (payload.containsKey("delete")) {
                if (!((Map<String, Object>) payload.get("delete")).containsKey("timestamp_ms")) {
                    fail("Payload \"delete\" does not contain a timestamp_field: " + payload.get("delete"));
                }
            } else {
                fail("Payload does not contain a timestamp_field: " + message);
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
                .name("sampleExampleClient")
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

        String consumerKey = dotenv.get("API_KEY");
        String consumerSecret = dotenv.get("API_SECRET");
        String token = dotenv.get("ACCESS_TOKEN");
        String secret = dotenv.get("ACCESS_TOKEN_SECRET");

        secrets = Secrets.of(consumerKey, consumerSecret, token, secret);
    }

    @Value(staticConstructor = "of")
    private static class Secrets {
        String consumerKey;
        String consumerSecret;
        String token;
        String secret;
    }
}
