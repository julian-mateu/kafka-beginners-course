package com.github.julian_mateu.kafka.twitter2elastic.consumer;

import com.github.julian_mateu.kafka.twitter2elastic.consumer.elastic.ElasticSearchWriter;
import com.google.common.collect.ImmutableList;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.util.AbstractMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.*;

class ConsumerTest {

    private static final AbstractMap.SimpleImmutableEntry<String, String> EXPECTED_DOCUMENT =
            new AbstractMap.SimpleImmutableEntry<>("key", "value");
    private static final List<Map.Entry<String, String>> EXPECTED_DOCUMENT_BATCH = ImmutableList.of(
            EXPECTED_DOCUMENT,
            EXPECTED_DOCUMENT,
            EXPECTED_DOCUMENT
    );
    @Mock
    private KafkaConsumer<String, String> kafkaConsumer;
    @Mock
    private ElasticSearchWriter elasticSearchWriter;
    @Mock
    private ConsumerRecords<String, String> mockRecords;
    @Mock
    private ConsumerRecord<String, String> mockRecord;
    private Consumer consumer;

    @BeforeEach
    public void setUp() {
        MockitoAnnotations.initMocks(this);
        when(kafkaConsumer.poll(any())).thenReturn(mockRecords);
        when(mockRecord.key()).thenReturn("key");
        when(mockRecord.value()).thenReturn("value");
        consumer = new Consumer(kafkaConsumer, elasticSearchWriter);
    }

    @Test
    public void consumeMessages() {
        // Given
        List<ConsumerRecord<String, String>> records = ImmutableList.of(mockRecord, mockRecord, mockRecord);
        when(mockRecords.iterator()).thenReturn(records.iterator());

        // When
        int processedMessages = consumer.consumeMessages();

        // Then
        assertEquals(3, processedMessages);
        verify(kafkaConsumer, times(1)).poll(any());
        verify(kafkaConsumer, times(1)).commitSync();
        verifyNoMoreInteractions(kafkaConsumer);
        verify(elasticSearchWriter, times(1)).submitDocumentBatch(eq(EXPECTED_DOCUMENT_BATCH));
        verifyNoMoreInteractions(elasticSearchWriter);
    }

    @Test
    @SuppressWarnings("unchecked")
    public void consumeMessagesAtLeastUpTo() {
        // Given
        List<ConsumerRecord<String, String>> records = ImmutableList.of(mockRecord, mockRecord, mockRecord);
        when(mockRecords.iterator()).thenReturn(records.iterator(), records.iterator());

        // When
        int processedMessages = consumer.consumeMessagesAtLeastUpTo(5);

        // Then
        assertEquals(6, processedMessages);
        verify(kafkaConsumer, times(2)).poll(any());
        verify(kafkaConsumer, times(2)).commitSync();
        verifyNoMoreInteractions(kafkaConsumer);
        verify(elasticSearchWriter, times(2)).submitDocumentBatch(eq(EXPECTED_DOCUMENT_BATCH));
        verifyNoMoreInteractions(elasticSearchWriter);
    }
}
