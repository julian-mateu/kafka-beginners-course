package com.github.julian_mateu.kafka.twitter2elastic.producer.kafka;

import com.evanlennick.retry4j.CallExecutorBuilder;
import com.evanlennick.retry4j.Status;
import com.evanlennick.retry4j.config.RetryConfig;
import com.evanlennick.retry4j.config.RetryConfigBuilder;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.KafkaAdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;

/**
 * Factory to create an instance of a {@link Producer}.
 */
@Slf4j
@RequiredArgsConstructor
public class ProducerFactory {

    private static final RetryConfig RETRY_CONFIG = new RetryConfigBuilder()
            .exponentialBackoff5Tries5Sec()
            .build();

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

    @SuppressWarnings("unchecked")
    private static Status<Object> retry(Callable<Object> callable) {
        return new CallExecutorBuilder<>().config(RETRY_CONFIG).build().execute(callable);
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

        AdminClient adminClient = getAdminClient(bootstrapServers);
        createTopicIfNeeded(adminClient);

        return new Producer(producer, ProducerFactory::callback, topicName);
    }

    @SneakyThrows({InterruptedException.class, ExecutionException.class})
    protected void createTopicIfNeeded(AdminClient adminClient) {
        if (!adminClient.listTopics().names().get().contains(topicName)) {
            NewTopic newTopic = new NewTopic(topicName, 1, (short) 1);
            retry(() -> adminClient.createTopics(Collections.singletonList(newTopic)).all().get());
        }
    }

    private AdminClient getAdminClient(String bootstrapServers) {
        Properties properties = new Properties();
        properties.setProperty(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        return KafkaAdminClient.create(properties);
    }
}
