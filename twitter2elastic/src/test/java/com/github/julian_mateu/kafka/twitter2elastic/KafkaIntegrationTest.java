package com.github.julian_mateu.kafka.twitter2elastic;

import com.github.julian_mateu.kafka.twitter2elastic.consumer.kafka.IntegrationTestConsumerFactory;
import com.github.julian_mateu.kafka.twitter2elastic.producer.kafka.IntegrationTestProducerFactory;
import com.github.julian_mateu.kafka.twitter2elastic.producer.kafka.Producer;
import lombok.Cleanup;
import lombok.SneakyThrows;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;

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
    private static final String GROUP_ID = "integrationTestConsumer";

    private static final IntegrationTestConsumerFactory CONSUMER_FACTORY =
            new IntegrationTestConsumerFactory(TOPIC_NAME, GROUP_ID, BOOTSTRAP_SERVERS);
    private static final IntegrationTestProducerFactory PRODUCER_FACTORY = new IntegrationTestProducerFactory(
            TOPIC_NAME, BOOTSTRAP_SERVERS
    );
    private static final String KEY = "key";
    private static final String MESSAGE = "message";
    private static final int POLL_DURATION = 2000;

    private KafkaConsumer<String, String> consumer;

    @BeforeEach
    public void setUp() {
        consumer = CONSUMER_FACTORY.getConsumer();
    }

    @Test
    public void produceAndConsume() {
        consumeNothing();
        produce();
        consume();
    }

    @SneakyThrows
    public void produce() {
        // Given
        @Cleanup Producer producer = PRODUCER_FACTORY.getProducer();

        // When
        RecordMetadata metadata = producer.sendMessage(KEY, MESSAGE).get();

        // Then
        assertEquals(0L, metadata.offset());
    }

    public void consume() {
        // Given

        // When
        ConsumerRecords<String, String> consumerRecords = consumer.poll(Duration.ofMillis(POLL_DURATION));

        // Then
        assertEquals(1, consumerRecords.count());
        ConsumerRecord<String, String> consumerRecord = consumerRecords.iterator().next();
        assertEquals(KEY, consumerRecord.key());
        assertEquals(MESSAGE, consumerRecord.value());
    }

    public void consumeNothing() {
        // Given

        // When
        ConsumerRecords<String, String> consumerRecords = consumer.poll(Duration.ofMillis(POLL_DURATION));

        // Then
        assertEquals(0, consumerRecords.count());
    }
}
