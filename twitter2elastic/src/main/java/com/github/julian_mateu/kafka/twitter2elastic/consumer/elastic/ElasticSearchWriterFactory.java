package com.github.julian_mateu.kafka.twitter2elastic.consumer.elastic;

import lombok.*;
import org.apache.http.HttpHost;
import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.client.indices.GetIndexRequest;
import org.elasticsearch.client.indices.GetIndexResponse;

import java.io.IOException;
import java.util.Arrays;

/**
 * Creates an instance of an {@link ElasticSearchWriter} with the given configuration.
 */
@RequiredArgsConstructor
public class ElasticSearchWriterFactory {

    @NonNull
    protected final String indexName;
    @NonNull
    private final String hostname;
    @NonNull
    private final String scheme;
    private final int port;

    @Getter(AccessLevel.PACKAGE)
    RestHighLevelClient client;

    /**
     * Builds a new {@link ElasticSearchWriter}.
     *
     * @return A new {@link ElasticSearchWriter} instance
     */
    public ElasticSearchWriter getWriter() {
        return new ElasticSearchWriter(setupClient(), indexName);
    }

    private ElasticSearchClient setupClient() {
        client = new RestHighLevelClient(
                RestClient.builder(
                        new HttpHost(hostname, port, scheme)
                )
        );

        createIndex(client);

        return new ElasticSearchClient(client);
    }

    protected void createIndex(@NonNull RestHighLevelClient client) {
        if (!indexExists(client)) {
            createNewIndex(client);
        }
    }

    @SneakyThrows(IOException.class)
    protected boolean indexExists(@NonNull RestHighLevelClient client) {
        GetIndexRequest getIndexRequest = new GetIndexRequest(indexName);
        try {
            GetIndexResponse getIndexResponse = client.indices().get(getIndexRequest, RequestOptions.DEFAULT);
            return Arrays.asList(getIndexResponse.getIndices()).contains(indexName);
        } catch (ElasticsearchStatusException exception) {
            String expectedMessage = String.format(
                    "Elasticsearch exception [type=index_not_found_exception, reason=no such index [%s]]",
                    indexName
            );
            if (exception.getMessage().equals(expectedMessage)) {
                return false;
            }
            throw exception;
        }
    }

    @SneakyThrows(IOException.class)
    protected void createNewIndex(@NonNull RestHighLevelClient client) {
        AcknowledgedResponse createIndexResponse = client.indices().create(
                new CreateIndexRequest(indexName),
                RequestOptions.DEFAULT
        );
        if (!createIndexResponse.isAcknowledged()) {
            throw new RuntimeException("Failed to create index " + indexName + ". " + createIndexResponse);
        }
    }
}
