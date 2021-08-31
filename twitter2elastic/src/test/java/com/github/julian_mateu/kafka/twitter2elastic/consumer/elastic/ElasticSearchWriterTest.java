package com.github.julian_mateu.kafka.twitter2elastic.consumer.elastic;

import com.google.common.collect.ImmutableList;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.client.RequestOptions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.io.IOException;
import java.util.AbstractMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.*;

class ElasticSearchWriterTest {

    @Mock
    private ElasticClient client;
    @Mock
    private BulkResponse response;

    private ElasticSearchWriter writer;

    private AbstractMap.SimpleImmutableEntry<String, String> testDocument;
    private List<Map.Entry<String, String>> testDocumentBatch;

    @BeforeEach
    void setUp() {
        MockitoAnnotations.initMocks(this);
        testDocument = new AbstractMap.SimpleImmutableEntry<>("key", "value");
        testDocumentBatch = ImmutableList.of(
                testDocument,
                testDocument,
                testDocument
        );
        writer = new ElasticSearchWriter(client, "index");
    }

    @Test
    void submitDocumentBatch() throws IOException {
        // Given
        when(client.bulk(any(), any()))
                .thenReturn(response);

        // When
        writer.submitDocumentBatch(testDocumentBatch);

        // Then
        verify(client, times(1))
                .bulk(any(), eq(RequestOptions.DEFAULT));
        verifyNoMoreInteractions(client);
    }

    @Test
    void submitDocumentBatchIgnoresEmptyBatch() {
        // Given

        // When
        writer.submitDocumentBatch(ImmutableList.of());

        // Then
        verifyNoMoreInteractions(client);
    }

    @Test
    void submitDocumentBatchSneakyThrowsIOException() throws IOException {
        // Given
        when(client.bulk(any(), any()))
                .thenThrow(new IOException("someMessage"));

        // When
        IOException thrown = assertThrows(
                IOException.class,
                () -> writer.submitDocumentBatch(testDocumentBatch)
        );

        // Then
        assertEquals("someMessage", thrown.getMessage());
    }

    @Test
    void submitDocumentIgnoresElasticsearchException() throws IOException {
        // Given
        when(client.bulk(any(), any()))
                .thenThrow(new ElasticsearchException("someMessage"));

        // When
        writer.submitDocumentBatch(testDocumentBatch);

        // Then
    }

    @Test
    void submitDocumentIgnoresFailures() throws IOException {
        // Given
        when(client.bulk(any(), any()))
                .thenReturn(response);
        when(response.hasFailures()).thenReturn(true);

        // When
        writer.submitDocumentBatch(testDocumentBatch);

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