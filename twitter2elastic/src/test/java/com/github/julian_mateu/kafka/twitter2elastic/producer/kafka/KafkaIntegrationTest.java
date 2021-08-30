package com.github.julian_mateu.kafka.twitter2elastic.producer.kafka;

import org.apache.kafka.clients.producer.RecordMetadata;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.concurrent.ExecutionException;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Integration tests to run against a local Kafka cluster, to be used as learning tests.
 *
 * @see <a href="https://docs.confluent.io/home/overview.html">Kafka docs</a>
 */
// TODO: Automate running docker_compose. For now these require the cluster to be already running locally.
public class KafkaIntegrationTest {

    private static final String BOOTSTRAP_SERVERS = "localhost:9092";
    private static final String TOPIC_NAME = "integration_test";
    private static final IntegrationTestProducerFactory PRODUCER_FACTORY = new IntegrationTestProducerFactory(
            TOPIC_NAME, BOOTSTRAP_SERVERS
    );

    private Producer producer;

    @BeforeEach
    public void setUp() {
        producer = PRODUCER_FACTORY.getProducer();
    }

    @Test
    public void producer() throws ExecutionException, InterruptedException {
        // Given

        // When
        RecordMetadata metadata = producer.sendMessage("key", "message").get();

        // Then
        assertEquals(0L, metadata.offset());
    }
}
