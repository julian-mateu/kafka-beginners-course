package com.github.julian_mateu.kafka.twitter2elastic.consumer;

import com.github.julian_mateu.kafka.twitter2elastic.consumer.elastic.ElasticSearchWriterFactory;
import com.github.julian_mateu.kafka.twitter2elastic.consumer.kafka.ConsumerFactory;
import com.google.inject.Binder;
import com.google.inject.Inject;
import com.google.inject.Module;
import com.google.inject.Provides;
import org.apache.kafka.clients.consumer.KafkaConsumer;

public class ElasticSearchConsumerModule implements Module {

    private static final String INDEX_NAME = "tweets";
    private static final String HOSTNAME = "localhost";
    private static final String SCHEME = "http";
    private static final int PORT = 9200;

    private static final String BOOTSTRAP_SERVERS = "localhost:9092";
    private static final String TOPIC_NAME = "tweets";
    private static final String GROUP_ID = "elasticSearchConsumerApplication";

    @Override
    public void configure(Binder binder) {
        ConsumerFactory consumerFactory = new ConsumerFactory(TOPIC_NAME, GROUP_ID, BOOTSTRAP_SERVERS);
        binder.bind(ConsumerFactory.class).toInstance(consumerFactory);

        ElasticSearchWriterFactory elasticSearchWriterFactory = new ElasticSearchWriterFactory(
                INDEX_NAME, HOSTNAME, SCHEME, PORT
        );
        binder.bind(ElasticSearchWriterFactory.class).toInstance(elasticSearchWriterFactory);
    }

    @Provides
    @Inject
    public KafkaConsumer<String, String> getKafkaConsumer(ConsumerFactory consumerFactory) {
        return consumerFactory.getConsumer();
    }
}