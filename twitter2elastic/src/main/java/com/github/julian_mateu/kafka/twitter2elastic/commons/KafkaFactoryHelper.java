package com.github.julian_mateu.kafka.twitter2elastic.commons;

import com.evanlennick.retry4j.CallExecutorBuilder;
import com.evanlennick.retry4j.Status;
import com.evanlennick.retry4j.config.RetryConfig;
import com.evanlennick.retry4j.config.RetryConfigBuilder;
import lombok.SneakyThrows;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.KafkaAdminClient;
import org.apache.kafka.clients.admin.NewTopic;

import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;

public abstract class KafkaFactoryHelper {

    private static final RetryConfig RETRY_CONFIG = new RetryConfigBuilder()
            .exponentialBackoff5Tries5Sec()
            .build();

    private KafkaFactoryHelper() {
    }

    @SuppressWarnings("unchecked")
    public static void retry(Callable<Object> callable) {
        new CallExecutorBuilder<>()
                .config(RETRY_CONFIG)
                .build()
                .execute(callable);
    }

    @SneakyThrows({InterruptedException.class, ExecutionException.class})
    public static void createTopicIfNeeded(AdminClient adminClient, String topicName) {
        if (!adminClient.listTopics().names().get().contains(topicName)) {
            NewTopic newTopic = new NewTopic(topicName, 1, (short) 1);
            retry(() -> adminClient.createTopics(Collections.singletonList(newTopic)).all().get());
        }
    }

    @SneakyThrows({InterruptedException.class, ExecutionException.class})
    public static void deleteTopicIfExists(AdminClient adminClient, String topicName) {
        if (adminClient.listTopics().names().get().contains(topicName)) {
            adminClient.deleteTopics(Collections.singletonList(topicName)).all().get();
        }
    }

    public static AdminClient getAdminClient(String bootstrapServers) {
        Properties properties = new Properties();
        properties.setProperty(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        return KafkaAdminClient.create(properties);
    }
}
