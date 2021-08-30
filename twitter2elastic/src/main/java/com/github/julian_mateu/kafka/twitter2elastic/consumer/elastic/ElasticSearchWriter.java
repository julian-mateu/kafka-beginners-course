package com.github.julian_mateu.kafka.twitter2elastic.consumer.elastic;

import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.common.xcontent.XContentType;

import java.io.IOException;

/**
 * Allows to submit a document to an ElasticSearch Index.
 */
@Slf4j
@RequiredArgsConstructor
public class ElasticSearchWriter implements AutoCloseable {

    @NonNull
    private final ElasticClient client;
    @NonNull
    private final String indexName;

    @SneakyThrows(IOException.class)
    public void submitDocument(@NonNull String documentId, @NonNull String payload) {
        IndexRequest indexRequest = new IndexRequest(indexName);
        indexRequest.id(documentId);
        indexRequest.source(payload, XContentType.JSON);
        try {
            IndexResponse indexResponse = client.index(indexRequest, RequestOptions.DEFAULT);
            log.info(indexResponse.toString());
        } catch (ElasticsearchException exception) {
            log.error("failed to submit to elastic search", exception);
        }
    }

    @Override
    public void close() throws Exception {
        client.close();
    }
}
