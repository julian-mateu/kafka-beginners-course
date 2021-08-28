package com.github.julian_mateu.kafka;

import io.github.cdimascio.dotenv.Dotenv;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
public class SecretsLoader {

    @NonNull
    private final Dotenv dotenv;

    public Secrets loadSecrets() {
        String consumerKey = dotenv.get("API_KEY");
        String consumerSecret = dotenv.get("API_SECRET");
        String token = dotenv.get("ACCESS_TOKEN");
        String secret = dotenv.get("ACCESS_TOKEN_SECRET");

        return Secrets.of(consumerKey, consumerSecret, token, secret);
    }
}
