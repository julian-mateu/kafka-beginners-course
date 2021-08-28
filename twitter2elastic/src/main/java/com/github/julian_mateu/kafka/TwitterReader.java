package com.github.julian_mateu.kafka;

import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.endpoint.StatusesSampleEndpoint;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.BasicClient;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;
import io.github.cdimascio.dotenv.Dotenv;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

@RequiredArgsConstructor
public class TwitterReader implements AutoCloseable {
    private static final Logger LOGGER = LoggerFactory.getLogger(TwitterReader.class);

    @NonNull
    private final BlockingQueue<String> queue;
    @NonNull
    private final BasicClient client;

    private static LinkedBlockingQueue<String> createQueue() {
        return new LinkedBlockingQueue<>(10000);
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

    private static Secrets loadSecrets() {
        Dotenv dotenv = Dotenv
                .configure()
                .load();

        return new SecretsLoader(dotenv).loadSecrets();
    }

    public static TwitterReader getInstance() {
        Secrets secrets = loadSecrets();
        LinkedBlockingQueue<String> queue = createQueue();
        BasicClient client = createClient(queue, secrets);
        return new TwitterReader(queue, client);
    }

    @Override
    public void close() {
        client.stop();
        long readMessages = client.getStatsTracker().getNumMessages();
        LOGGER.debug("The client read {} messages!\n", readMessages);
    }

    public Optional<String> readMessage() {
        if (client.isDone()) {
            throw new IllegalStateException(
                    "Client connection closed unexpectedly: " + client.getExitEvent().getMessage());
        }

        try {
            String message = queue.poll(5, TimeUnit.SECONDS);
            if (message == null) {
                LOGGER.debug("Did not receive a message in 5 seconds");
                return Optional.empty();
            } else {
                LOGGER.debug(message);
                return Optional.of(message);
            }
        } catch (InterruptedException exception) {
            throw new IllegalStateException(exception);
        }
    }
}
