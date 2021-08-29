package com.github.julian_mateu.kafka.twitter2elastic.producer.twitter.parsing;

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
