package com.github.julian_mateu.kafka.twitter2elastic.consumer.kafka;

import com.github.julian_mateu.kafka.twitter2elastic.commons.KafkaFactoryHelper;
import lombok.NonNull;
import org.apache.kafka.clients.admin.AdminClient;

public class IntegrationTestConsumerFactory extends ConsumerFactory {
    public IntegrationTestConsumerFactory(@NonNull String topicName,
                                          @NonNull String groupId,
                                          @NonNull String bootstrapServers) {
        super(topicName, groupId, bootstrapServers);
    }

    @Override
    protected void createTopicIfNeeded(AdminClient adminClient) {
        KafkaFactoryHelper.deleteTopicIfExists(adminClient, topicName);
        super.createTopicIfNeeded(adminClient);
    }
}
