package com.github.julian_mateu.kafka.twitter2elastic.consumer;

import com.github.julian_mateu.kafka.twitter2elastic.consumer.elastic.ElasticSearchWriter;
import com.google.inject.Inject;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.time.Duration;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Consumer that retrieves a message and key from a Kafka topic.
 */
@Slf4j
@RequiredArgsConstructor(onConstructor = @__({@Inject}))
public class Consumer {

    @NonNull
    private final KafkaConsumer<String, String> consumer;
    @NonNull
    private final ElasticSearchWriter elasticSearchWriter;

    /**
     * Sends a batch of messages from the Kafka topic to the {@link ElasticSearchWriter} instance. Processing is at most
     * once, it is assumed that processing is idempotent given that the key is being used when sending data to the
     * {@link ElasticSearchWriter}.
     *
     * @return the number of messages processed in this batch.
     */
    public int consumeMessages() {
        ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
        log.debug(String.format("Received %s records", records.count()));

        List<Map.Entry<String, String>> documentBatch = new ArrayList<>(records.count());

        int processedMessages = 0;
        for (ConsumerRecord<String, String> record : records) {
            log.info(String.format("Key: %s, Value: %s", record.key(), record.value()));
            log.info(String.format("Partition: %s, Offset: %s", record.partition(), record.offset()));
            documentBatch.add(new AbstractMap.SimpleImmutableEntry<>(record.key(), record.value()));
            processedMessages++;
        }

        elasticSearchWriter.submitDocumentBatch(documentBatch);

        consumer.commitSync();
        log.debug("Committed offsets");
        return processedMessages;
    }

    /**
     * Sends a batch of messages from the Kafka topic to the {@link ElasticSearchWriter} instance.
     *
     * @param numberOfMessagesToConsume indicates the minimum number of messages to be consumed. More can be consumed
     *                                  if the last batch contains more messages than the remainder to reach this param.
     * @return the number of messages processed in this batch.
     */
    public int consumeMessagesAtLeastUpTo(int numberOfMessagesToConsume) {
        int numberOfMessagesConsumedSoFar = 0;
        while (numberOfMessagesConsumedSoFar < numberOfMessagesToConsume) {
            numberOfMessagesConsumedSoFar += consumeMessages();
        }

        log.info("Total number of messages consumed = " + numberOfMessagesConsumedSoFar);
        return numberOfMessagesConsumedSoFar;
    }
}
