package com.github.julian_mateu.kafka;

import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.concurrent.Future;

/**
 * Producer that sends a message and key to a Kafka topic.
 */
@Slf4j
@RequiredArgsConstructor
public class Producer implements AutoCloseable {

    @NonNull
    private final KafkaProducer<String, String> producer;
    @NonNull
    private final Callback callback;
    @NonNull
    private final String topicName;

    @Override
    public void close() {
        producer.flush();
        producer.close();
    }

    /**
     * Sends a message to the Kafka topic.
     *
     * @param key     the key of the message
     * @param message the message to be sent
     * @return A {@link Future} of a {@link RecordMetadata} instance
     */
    public Future<RecordMetadata> sendMessage(@NonNull String key, @NonNull String message) {
        ProducerRecord<String, String> record = new ProducerRecord<>(topicName, key, message);
        return producer.send(record, callback);
    }
}
