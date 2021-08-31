package com.github.julian_mateu.kafka.twitter2elastic.producer.kafka;

import com.github.julian_mateu.kafka.twitter2elastic.producer.testutils.RecordMetadataFutureMock;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.util.concurrent.Future;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.*;

class ProducerTest {

    @Mock
    private KafkaProducer<String, String> kafkaProducer;
    @Mock
    private Callback callback;
    @Mock
    private RecordMetadataFutureMock recordMetadataFutureMock;

    private Producer producer;

    @BeforeEach
    public void setUp() {
        MockitoAnnotations.initMocks(this);
        when(kafkaProducer.send(any(), any())).thenReturn(recordMetadataFutureMock);
        producer = new Producer(kafkaProducer, callback, "topicName");
    }

    @Test
    public void sendMessage() {
        // Given

        // When
        Future<RecordMetadata> result = producer.sendMessage("key", "message");

        // Then
        assertEquals(recordMetadataFutureMock, result);
        verify(kafkaProducer, times(1))
                .send(eq(new ProducerRecord<>("topicName", "key", "message")), eq(callback));
        verifyNoMoreInteractions(kafkaProducer);
    }

    @Test
    public void close() {
        // Given

        // When
        producer.close();

        // Then
        verify(kafkaProducer, times(1)).flush();
        verify(kafkaProducer, times(1)).close();
        verifyNoMoreInteractions(kafkaProducer);
    }
}