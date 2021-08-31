package com.github.julian_mateu.kafka.twitter2elastic.producer.kafka;

import com.github.julian_mateu.kafka.twitter2elastic.commons.KafkaFactoryHelper;
import lombok.NonNull;
import org.apache.kafka.clients.admin.AdminClient;

public class IntegrationTestProducerFactory extends ProducerFactory {

    public IntegrationTestProducerFactory(@NonNull String topicName, @NonNull String bootstrapServers) {
        super(topicName, bootstrapServers);
    }

    @Override
    protected void createTopicIfNeeded(AdminClient adminClient) {
        KafkaFactoryHelper.deleteTopicIfExists(adminClient, topicName);
        super.createTopicIfNeeded(adminClient);
    }
}
