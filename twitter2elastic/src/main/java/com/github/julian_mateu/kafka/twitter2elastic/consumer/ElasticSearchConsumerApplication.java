package com.github.julian_mateu.kafka.twitter2elastic.consumer;

import com.github.julian_mateu.kafka.twitter2elastic.consumer.elastic.ElasticSearchWriter;
import com.github.julian_mateu.kafka.twitter2elastic.consumer.elastic.ElasticSearchWriterFactory;
import lombok.Cleanup;
import lombok.SneakyThrows;

public class ElasticSearchConsumerApplication {
    private static final String INTEGRATION_TEST_INDEX = "tweets";
    private static final String HOSTNAME = "localhost";
    private static final String SCHEME = "http";
    private static final int PORT = 9200;

    @SneakyThrows(Exception.class)
    public static void main(String[] args) {
        ElasticSearchWriterFactory factory = new ElasticSearchWriterFactory(
                INTEGRATION_TEST_INDEX, HOSTNAME, SCHEME, PORT
        );

        @Cleanup ElasticSearchWriter elasticSearchWriter = factory.getWriter();

        elasticSearchWriter.submitDocument("id", "payload");
    }
}
