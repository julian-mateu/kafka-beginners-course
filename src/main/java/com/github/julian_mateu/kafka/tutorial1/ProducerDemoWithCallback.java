package com.github.julian_mateu.kafka.tutorial1;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerDemoWithCallback {

    private static final Logger LOGGER = LoggerFactory.getLogger(ProducerDemoWithCallback.class);

    private static final Callback CALLBACK = (metadata, exception) -> {
        // Executes every time a record is successfully sent or an exception is thrown
        if (exception == null) {
            // The record was successfully sent
            LOGGER.info(String.format(
                            "Received new metadata:\n Topic: %s\n Partition: %s\n Offset: %s\n Timestamp: %s",
                            metadata.topic(), metadata.partition(), metadata.offset(), metadata.timestamp()
                    )
            );
        } else {
            LOGGER.error("Error while producing: ", exception);
        }
    };

    public static void main(String[] args) {
        String bootstrapServers = "localhost:9092";
        String topicName = "first_topic";
        String message = "hello world";

        // 1. Create producer properties
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // 2. Create the producer
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        for (int i = 0; i < 10; i++) {
            // 3. Create a producer record
            ProducerRecord<String, String> record = new ProducerRecord<>(topicName, message + i);

            // 4. Send the data asynchronously
            producer.send(record, CALLBACK);
        }

        // 5. Flush and close producer
        producer.flush();
        producer.close();
    }
}
