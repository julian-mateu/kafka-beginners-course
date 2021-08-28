package com.github.julian_mateu.kafka;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Optional;

public class HelloWorld {
    private static final Logger LOGGER = LoggerFactory.getLogger(HelloWorld.class);
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    public static void main(String[] args) {
        readMessages();
    }

    public static void readMessages() {
        int actualMessages = 0;
        try (TwitterReader reader = TwitterReader.getInstance()) {
            for (int messagesRead = 0; messagesRead < 10; messagesRead++) {
                Optional<String> message = reader.readMessage();
                if (message.isPresent()) {
                    LOGGER.debug(message.get());
                    processMessage(message.get());
                    actualMessages++;
                }
            }
        }

        LOGGER.debug("processed {} messages", actualMessages);
    }

    public static void processMessage(String message) {
        Map<String, Object> payload = getPayloadAsMap(message);
        if (payload.containsKey("delete")) {
            LOGGER.info(payload.getOrDefault("delete", "").toString());
        } else {
            LOGGER.info(payload.getOrDefault("timestamp_ms", "").toString());
        }
    }

    public static Map<String, Object> getPayloadAsMap(String message) {
        try {
            return OBJECT_MAPPER.readValue(message, new TypeReference<Map<String, Object>>() {
            });
        } catch (JsonProcessingException exception) {
            throw new IllegalArgumentException(exception);
        }
    }
}
