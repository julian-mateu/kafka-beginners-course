package com.github.julian_mateu.kafka.twitter2elastic.producer.twitter.secrets;

import lombok.NonNull;
import lombok.Value;

/**
 * Container with secrets for the Twitter API.
 */
@Value(staticConstructor = "of")
public class Secrets {
    @NonNull String consumerKey;
    @NonNull String consumerSecret;
    @NonNull String token;
    @NonNull String secret;
}
