package com.github.julian_mateu.kafka.twitter2elastic.consumer;

import com.github.julian_mateu.kafka.twitter2elastic.consumer.elastic.ElasticSearchWriter;
import com.github.julian_mateu.kafka.twitter2elastic.consumer.elastic.ElasticSearchWriterFactory;
import com.google.inject.Guice;
import com.google.inject.Inject;
import com.google.inject.Injector;
import lombok.Cleanup;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.KafkaConsumer;

@Slf4j
@RequiredArgsConstructor(onConstructor = @__({@Inject}))
public class ElasticSearchConsumerApplication {

    private static final int NUMBER_OF_MESSAGES_TO_CONSUME = 1000;

    @NonNull
    private final ElasticSearchWriterFactory elasticSearchWriterFactory;
    @NonNull
    private final KafkaConsumer<String, String> kafkaConsumer;

    public static void main(String[] args) {
        ElasticSearchConsumerApplication.getInstance().run(NUMBER_OF_MESSAGES_TO_CONSUME);
    }

    public static ElasticSearchConsumerApplication getInstance() {
        Injector injector = Guice.createInjector(new ElasticSearchConsumerModule());
        return injector.getInstance(ElasticSearchConsumerApplication.class);
    }

    @SneakyThrows(Exception.class)
    public void run(int numberOfMessagesToConsume) {
        @Cleanup ElasticSearchWriter elasticSearchWriter = elasticSearchWriterFactory.getWriter();
        Consumer consumer = new Consumer(kafkaConsumer, elasticSearchWriter);
        consumer.consumeMessagesAtLeastUpTo(numberOfMessagesToConsume);
    }
}
