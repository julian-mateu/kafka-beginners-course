package com.github.julian_mateu.kafka.twitter2elastic.producer.twitter.secrets;

import lombok.Value;

/**
 * Container with secrets for the Twitter API.
 */
@Value(staticConstructor = "of")
public class Secrets {
    String consumerKey;
    String consumerSecret;
    String token;
    String secret;
}
