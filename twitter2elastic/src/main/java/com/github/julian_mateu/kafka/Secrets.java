package com.github.julian_mateu.kafka;

import lombok.Value;

@Value(staticConstructor = "of")
class Secrets {
    String consumerKey;
    String consumerSecret;
    String token;
    String secret;
}
