package com.github.julian_mateu.kafka.twitter2elastic.producer.kafka;

import lombok.NonNull;
import lombok.SneakyThrows;
import org.apache.kafka.clients.admin.AdminClient;

import java.util.Collections;
import java.util.concurrent.ExecutionException;

public class IntegrationTestProducerFactory extends ProducerFactory {

    public IntegrationTestProducerFactory(@NonNull String topicName, @NonNull String bootstrapServers) {
        super(topicName, bootstrapServers);
    }

    @Override
    @SneakyThrows({InterruptedException.class, ExecutionException.class})
    protected void createTopicIfNeeded(AdminClient adminClient) {
        if (adminClient.listTopics().names().get().contains(topicName)) {
            adminClient.deleteTopics(Collections.singletonList(topicName)).all().get();
        }
        super.createTopicIfNeeded(adminClient);
    }
}
