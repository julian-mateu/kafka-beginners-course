package com.github.julian_mateu.kafka.twitter2elastic.producer.kafka;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.KafkaAdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.Properties;
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

    private Producer producer;

    @BeforeEach
    public void setUp() throws ExecutionException, InterruptedException {
        AdminClient adminClient = getAdminClient(BOOTSTRAP_SERVERS);
        if (adminClient.listTopics().names().get().contains(TOPIC_NAME)) {
            adminClient.deleteTopics(Collections.singletonList(TOPIC_NAME)).all().get();
        }
        if (!adminClient.listTopics().names().get().contains(TOPIC_NAME)) {
            NewTopic newTopic = new NewTopic(TOPIC_NAME, 1, (short) 1);
            adminClient.createTopics(Collections.singletonList(newTopic)).all().get();
        }

        producer = ProducerFactory.getProducer(BOOTSTRAP_SERVERS, TOPIC_NAME);

    }

    @Test
    public void producer() throws ExecutionException, InterruptedException {
        // Given

        // When
        RecordMetadata metadata = producer.sendMessage("key", "message").get();

        // Then
        assertEquals(0L, metadata.offset());
    }

    private AdminClient getAdminClient(String bootstrapServers) {
        Properties properties = new Properties();
        properties.setProperty(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        return KafkaAdminClient.create(properties);
    }
}
