package com.github.julian_mateu.kafka.twitter2elastic.producer;

import com.github.julian_mateu.kafka.twitter2elastic.producer.kafka.ProducerFactory;
import com.github.julian_mateu.kafka.twitter2elastic.producer.twitter.TwitterMessageReaderFactory;
import com.github.julian_mateu.kafka.twitter2elastic.producer.twitter.secrets.SecretsLoader;
import com.google.inject.Binder;
import com.google.inject.Module;
import io.github.cdimascio.dotenv.Dotenv;

public class TwitterProducerApplicationModule implements Module {

    private static final String BOOTSTRAP_SERVERS = "localhost:9092";
    private static final String TOPIC_NAME = "tweets";
    private static final int QUEUE_CAPACITY = 10000;
    private static final String CLIENT_NAME = "twitterProducerApplication";

    @Override
    public void configure(Binder binder) {
        Dotenv dotenv = Dotenv
                .configure()
                .load();
        binder.bind(Dotenv.class).toInstance(dotenv);

        TwitterMessageReaderFactory twitterMessageReaderFactory =
                new TwitterMessageReaderFactory(QUEUE_CAPACITY, CLIENT_NAME, new SecretsLoader(dotenv));
        binder.bind(TwitterMessageReaderFactory.class).toInstance(twitterMessageReaderFactory);

        ProducerFactory producerFactory = new ProducerFactory(TOPIC_NAME, BOOTSTRAP_SERVERS);
        binder.bind(ProducerFactory.class).toInstance(producerFactory);

        binder.bind(TweetProducer.ResourceManager.class).to(TweetProducer.SimpleResourceManager.class);
    }
}
