package com.github.julian_mateu.kafka.twitter2elastic.consumer.elastic;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.RequestOptions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.io.IOException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.*;

class ElasticSearchWriterTest {

    @Mock
    private ElasticClient client;

    private ElasticSearchWriter writer;

    @BeforeEach
    void setUp() {
        MockitoAnnotations.initMocks(this);

        writer = new ElasticSearchWriter(client, "index");
    }

    @Test
    void submitDocument() throws IOException {
        // Given
        when(client.index(any(), any()))
                .thenReturn(mock(IndexResponse.class));

        // When
        writer.submitDocument("id", "payload");

        // Then
        verify(client, times(1))
                .index(any(), eq(RequestOptions.DEFAULT));
        verifyNoMoreInteractions(client);
    }

    @Test
    void submitDocumentSneakyThrowsIOException() throws IOException {
        // Given
        when(client.index(any(), any()))
                .thenThrow(new IOException("someMessage"));

        // When
        IOException thrown = assertThrows(
                IOException.class,
                () -> writer.submitDocument("id", "payload")
        );

        // Then
        assertEquals("someMessage", thrown.getMessage());
    }

    @Test
    void submitDocumentIgnoresElasticsearchException() throws IOException {
        // Given
        when(client.index(any(), any()))
                .thenThrow(new ElasticsearchException("someMessage"));

        // When
        writer.submitDocument("id", "payload");

        // Then
    }

    @Test
    public void close() throws Exception {
        // Given

        // When
        writer.close();

        // Then
        verify(client, times(1)).close();
        verifyNoMoreInteractions(client);
    }
}