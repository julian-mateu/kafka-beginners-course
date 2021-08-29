package com.github.julian_mateu.kafka;

import lombok.Value;

import java.util.Map;

/**
 * Container for a tweet.
 */
@Value(staticConstructor = "of")
public class Tweet {
    String id;
    Map<String, Object> payload;
    String payloadString;
}
