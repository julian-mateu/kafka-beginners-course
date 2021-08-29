package com.github.julian_mateu.kafka;

import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.endpoint.StatusesSampleEndpoint;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.BasicClient;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;
import io.github.cdimascio.dotenv.Dotenv;

import java.util.concurrent.LinkedBlockingQueue;

/**
 * Factory to get an instance of {@link TwitterMessageReader}.
 */
public abstract class TwitterMessageReaderFactory {

    private TwitterMessageReaderFactory() {
    }

    /**
     * Builds a {@link TwitterMessageReader}.
     *
     * @return A new {@link TwitterMessageReader} instance
     */
    public static TwitterMessageReader get() {
        LinkedBlockingQueue<String> queue = createQueue();
        Secrets secrets = loadSecrets();
        BasicClient client = TwitterMessageReaderFactory.createClient(queue, secrets);
        return new TwitterMessageReader(queue, client);
    }

    private static LinkedBlockingQueue<String> createQueue() {
        return new LinkedBlockingQueue<>(10000);
    }

    private static Secrets loadSecrets() {
        Dotenv dotenv = Dotenv
                .configure()
                .load();

        return new SecretsLoader(dotenv).loadSecrets();
    }

    private static BasicClient createClient(LinkedBlockingQueue<String> queue, Secrets secrets) {

        StatusesSampleEndpoint endpoint = new StatusesSampleEndpoint();
        endpoint.stallWarnings(false);

        Authentication auth = new OAuth1(
                secrets.getConsumerKey(),
                secrets.getConsumerSecret(),
                secrets.getToken(),
                secrets.getSecret()
        );

        BasicClient client = new ClientBuilder()
                .name("sampleExampleClient")
                .hosts(Constants.STREAM_HOST)
                .endpoint(endpoint)
                .authentication(auth)
                .processor(new StringDelimitedProcessor(queue))
                .build();

        client.connect();

        return client;
    }
}
