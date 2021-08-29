package com.github.julian_mateu.kafka.twitter2elastic.producer.twitter.parsing;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;

import java.util.Map;

/**
 * Helper class to parse a json message into a {@link Tweet} object.
 */
@RequiredArgsConstructor
public class TweetParser {

    @NonNull
    private final ObjectMapper objectMapper;

    /**
     * Parses a json string into a {@link Tweet}.
     *
     * @param message a string representing a tweet in JSON format
     * @return A new {@link Tweet} instance
     */
    public Tweet parseMessage(String message) {
        try {
            return getTweet(message);
        } catch (IllegalArgumentException exception) {
            throw new IllegalArgumentException("Invalid message: " + message, exception);
        }
    }

    private Tweet getTweet(String message) {
        Map<String, Object> payload = getPayloadAsMap(message);

        Map<String, Object> payloadContainingId = payload;

        String deleteKey = "delete";
        if (payload.containsKey(deleteKey)) {
            Map<String, Object> deletePayload = getAsMapIfPresent(payload, deleteKey);
            payloadContainingId = getAsMapIfPresent(deletePayload, "status");
        }

        String id = getIdAsString(payloadContainingId);
        return Tweet.of(id, payload, message);
    }

    @SneakyThrows(JsonProcessingException.class)
    private Map<String, Object> getPayloadAsMap(String message) {
        return objectMapper.readValue(message, new TypeReference<Map<String, Object>>() {
        });
    }

    private Map<String, Object> getAsMapIfPresent(Map<String, Object> payload, String key) {
        if (payload.containsKey(key)) {
            return getAsMap(payload, key);
        } else {
            throw new IllegalArgumentException();
        }
    }

    @SuppressWarnings("unchecked")
    private Map<String, Object> getAsMap(Map<String, Object> payload, String key) {
        return (Map<String, Object>) payload.get(key);
    }

    private String getIdAsString(Map<String, Object> payload) {
        String idKey = "id_str";
        if (!payload.containsKey(idKey)) {
            throw new IllegalArgumentException();
        }
        return (String) payload.get(idKey);
    }
}
