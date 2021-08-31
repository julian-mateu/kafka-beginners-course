package com.github.julian_mateu.kafka.twitter2elastic.consumer.elastic;

import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.common.xcontent.XContentType;

import java.io.IOException;
import java.util.List;
import java.util.Map;

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
    public void submitDocumentBatch(@NonNull List<Map.Entry<String, String>> documentBatch) {
        if (documentBatch.isEmpty()) {
            return;
        }

        BulkRequest bulkRequest = new BulkRequest();

        for (Map.Entry<String, String> document : documentBatch) {
            IndexRequest indexRequest = new IndexRequest(indexName);
            indexRequest.id(document.getKey());
            indexRequest.source(document.getValue(), XContentType.JSON);
            bulkRequest.add(indexRequest);
        }

        try {
            BulkResponse bulkResponse = client.bulk(bulkRequest, RequestOptions.DEFAULT);
            log.info(bulkResponse.toString());
            if (bulkResponse.hasFailures()) {
                log.warn("There were failures while submitting to elastic search. "
                        + bulkResponse.buildFailureMessage());
            }
        } catch (ElasticsearchException exception) {
            log.error("failed to submit to elastic search", exception);
        }
    }


    @Override
    public void close() throws Exception {
        client.close();
    }
}
