package com.github.julian_mateu.kafka.twitter2elastic.producer.kafka;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.*;

class ProducerTest {

    @Mock
    private KafkaProducer<String, String> kafkaProducer;
    @Mock
    private Callback callback;

    @BeforeEach
    public void setUp() {
        MockitoAnnotations.initMocks(this);
    }

    @Test
    public void sendMessage() {
        // Given
        Producer producer = new Producer(kafkaProducer, callback, "topicName");

        // When
        producer.sendMessage("key", "message");

        // Then
        verify(kafkaProducer, times(1))
                .send(eq(new ProducerRecord<>("topicName", "key", "message")), eq(callback));
        verifyNoMoreInteractions(kafkaProducer);
    }

    @Test
    public void close() {
        // Given
        Producer producer = new Producer(kafkaProducer, callback, "topicName");

        // When
        producer.close();

        // Then
        verify(kafkaProducer, times(1)).flush();
        verify(kafkaProducer, times(1)).close();
        verifyNoMoreInteractions(kafkaProducer);
    }
}