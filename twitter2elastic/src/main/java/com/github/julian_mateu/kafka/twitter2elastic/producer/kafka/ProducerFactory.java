package com.github.julian_mateu.kafka.twitter2elastic.producer.kafka;

import com.github.julian_mateu.kafka.twitter2elastic.commons.KafkaFactoryHelper;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

/**
 * Factory to create an instance of a {@link Producer}.
 */
@Slf4j
@RequiredArgsConstructor
public class ProducerFactory {

    @NonNull
    protected final String topicName;
    @NonNull
    private final String bootstrapServers;

    private static void callback(@NonNull RecordMetadata metadata, Exception exception) {
        if (exception == null) {
            log.info(String.format(
                            "Received new metadata:\n Topic: %s\n Partition: %s\n Offset: %s\n Timestamp: %s",
                            metadata.topic(), metadata.partition(), metadata.offset(), metadata.timestamp()
                    )
            );
        } else {
            log.error("Error while producing: ", exception);
        }
    }

    /**
     * Builds a new instance of {@link Producer}.
     *
     * @return An instance of {@link Producer}
     */
    public Producer getProducer() {
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        AdminClient adminClient = KafkaFactoryHelper.getAdminClient(bootstrapServers);
        createTopicIfNeeded(adminClient);

        return new Producer(producer, ProducerFactory::callback, topicName);
    }

    protected void createTopicIfNeeded(AdminClient adminClient) {
        KafkaFactoryHelper.createTopicIfNeeded(adminClient, topicName);
    }
}
