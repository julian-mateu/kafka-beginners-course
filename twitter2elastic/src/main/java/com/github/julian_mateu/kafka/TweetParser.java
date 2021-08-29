package com.github.julian_mateu.kafka;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;

import java.util.Map;

@RequiredArgsConstructor
public class TweetParser {

    @NonNull
    private final ObjectMapper objectMapper;

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
        return new Tweet(id, payload);
    }

    private Map<String, Object> getPayloadAsMap(String message) {
        try {
            return objectMapper.readValue(message, new TypeReference<Map<String, Object>>() {
            });
        } catch (JsonProcessingException exception) {
            throw new IllegalArgumentException(exception);
        }
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
