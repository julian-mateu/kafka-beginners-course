package com.github.julian_mateu.kafka.twitter2elastic.consumer.elastic;

import lombok.NonNull;
import lombok.SneakyThrows;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.admin.indices.refresh.RefreshRequest;
import org.elasticsearch.action.admin.indices.refresh.RefreshResponse;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;

import java.io.IOException;

/**
 * Inherits from the {@link ElasticSearchWriterFactory} to delete the index if it exists.
 */
public class IntegrationTestElasticSearchWriterFactory extends ElasticSearchWriterFactory {

    public IntegrationTestElasticSearchWriterFactory(@NonNull String indexName,
                                                     @NonNull String hostname,
                                                     @NonNull String scheme,
                                                     int port) {
        super(indexName, hostname, scheme, port);
    }

    @Override
    protected void createIndex(@NonNull RestHighLevelClient client) {
        if (indexExists(client)) {
            deleteIndex(client);
        }
        super.createIndex(client);
        refreshIndex(client);
    }

    @SneakyThrows(IOException.class)
    protected void deleteIndex(@NonNull RestHighLevelClient client) {
        AcknowledgedResponse deleteIndexResponse = client.indices()
                .delete(
                        new DeleteIndexRequest(indexName),
                        RequestOptions.DEFAULT
                );
        if (!deleteIndexResponse.isAcknowledged()) {
            throw new RuntimeException("Failed to delete index " + indexName + ". " + deleteIndexResponse);
        }
    }

    @SneakyThrows(IOException.class)
    protected void refreshIndex(@NonNull RestHighLevelClient client) {
        RefreshRequest refreshRequest = new RefreshRequest(indexName);
        RefreshResponse refreshResponse = client.indices().refresh(refreshRequest, RequestOptions.DEFAULT);
        if (refreshResponse.getFailedShards() != 0) {
            throw new RuntimeException("Failed to refresh index " + indexName + ". " + refreshResponse);
        }
    }
}
