package com.github.julian_mateu.kafka.twitter2elastic.producer.kafka;

import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

/**
 * Factory to create an instance of a {@link Producer}.
 */
@Slf4j
public abstract class ProducerFactory {

    private ProducerFactory() {
    }

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
     * @param bootstrapServers the URL of the bootstrap servers
     * @param topicName        the name of the topic to which messages will be produced
     * @return An instance of {@link Producer}
     */
    public static Producer getProducer(@NonNull String bootstrapServers, @NonNull String topicName) {

        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        return new Producer(producer, ProducerFactory::callback, topicName);
    }
}
