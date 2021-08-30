package com.github.julian_mateu.kafka.twitter2elastic.producer.twitter;

import com.github.julian_mateu.kafka.twitter2elastic.producer.twitter.secrets.Secrets;
import com.github.julian_mateu.kafka.twitter2elastic.producer.twitter.secrets.SecretsLoader;
import com.google.common.collect.ImmutableList;
import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.BasicClient;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;

import java.util.concurrent.LinkedBlockingQueue;

/**
 * Factory to get an instance of {@link TwitterMessageReader}.
 */
@RequiredArgsConstructor
public class TwitterMessageReaderFactory {

    private final int queueCapacity;
    @NonNull
    private final String clientName;
    @NonNull
    private final SecretsLoader secretsLoader;
    @NonNull
    private final ImmutableList<String> searchTerms;

    private BasicClient createClient(LinkedBlockingQueue<String> queue, Secrets secrets) {

        StatusesFilterEndpoint endpoint = new StatusesFilterEndpoint();
        endpoint.trackTerms(searchTerms);

        endpoint.stallWarnings(false);

        Authentication auth = new OAuth1(
                secrets.getConsumerKey(),
                secrets.getConsumerSecret(),
                secrets.getToken(),
                secrets.getSecret()
        );

        BasicClient client = new ClientBuilder()
                .name(clientName)
                .hosts(Constants.STREAM_HOST)
                .endpoint(endpoint)
                .authentication(auth)
                .processor(new StringDelimitedProcessor(queue))
                .build();

        client.connect();

        return client;
    }

    /**
     * Builds a {@link TwitterMessageReader}.
     *
     * @return A new {@link TwitterMessageReader} instance
     */
    public TwitterMessageReader getTwitterMessageReader() {
        LinkedBlockingQueue<String> queue = new LinkedBlockingQueue<>(queueCapacity);
        Secrets secrets = secretsLoader.loadSecrets();
        BasicClient client = createClient(queue, secrets);
        return new TwitterMessageReader(queue, client);
    }
}
