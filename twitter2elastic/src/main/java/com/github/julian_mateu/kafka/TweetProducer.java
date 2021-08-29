package com.github.julian_mateu.kafka;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.Cleanup;
import lombok.extern.slf4j.Slf4j;

import java.util.Map;
import java.util.Optional;

@Slf4j
public class TweetProducer {

    public static void main(String[] args) {
        readMessages();
    }

    public static void readMessages() {
        int actualMessages = 0;

        @Cleanup
        TwitterReader reader = TwitterReaderFactory.get();

        for (int messagesRead = 0; messagesRead < 10; messagesRead++) {
            Optional<String> message = reader.readMessage();
            if (message.isPresent()) {
                log.debug(message.get());
                processMessage(message.get());
                actualMessages++;
            }
        }

        log.debug("processed {} messages", actualMessages);
    }

    public static void processMessage(String message) {
        TweetParser parser = new TweetParser(new ObjectMapper());
        Map<String, Object> payload = parser.parseMessage(message).getPayload();
        if (payload.containsKey("delete")) {
            log.info(payload.getOrDefault("delete", "").toString());
        } else {
            log.info(payload.getOrDefault("timestamp_ms", "").toString());
        }
    }
}
