package com.github.julian_mateu.kafka;

import lombok.Value;

/**
 * Container with secrets for the Twitter API.
 */
@Value(staticConstructor = "of")
class Secrets {
    String consumerKey;
    String consumerSecret;
    String token;
    String secret;
}
