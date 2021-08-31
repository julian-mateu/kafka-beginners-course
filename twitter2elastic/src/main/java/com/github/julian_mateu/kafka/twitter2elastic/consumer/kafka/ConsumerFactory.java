package com.github.julian_mateu.kafka.twitter2elastic.consumer.kafka;

import com.github.julian_mateu.kafka.twitter2elastic.commons.KafkaFactoryHelper;
import com.github.julian_mateu.kafka.twitter2elastic.consumer.Consumer;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.Collections;
import java.util.Properties;

/**
 * Factory to create an instance of a {@link KafkaConsumer}.
 */
@Slf4j
@RequiredArgsConstructor
public class ConsumerFactory {

    @NonNull
    protected final String topicName;
    @NonNull
    protected final String groupId;
    @NonNull
    private final String bootstrapServers;

    /**
     * Builds a new instance of {@link Consumer}.
     *
     * @return An instance of {@link Consumer}
     */
    public KafkaConsumer<String, String> getConsumer() {
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);

        AdminClient adminClient = KafkaFactoryHelper.getAdminClient(bootstrapServers);
        createTopicIfNeeded(adminClient);

        consumer.subscribe(Collections.singleton(topicName));

        return consumer;
    }

    protected void createTopicIfNeeded(AdminClient adminClient) {
        KafkaFactoryHelper.createTopicIfNeeded(adminClient, topicName);
    }
}
