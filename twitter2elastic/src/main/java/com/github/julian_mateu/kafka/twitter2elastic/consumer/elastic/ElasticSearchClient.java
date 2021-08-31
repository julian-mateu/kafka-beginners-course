package com.github.julian_mateu.kafka.twitter2elastic.consumer.elastic;

import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;

import java.io.IOException;

/**
 * Wraps a {@link RestHighLevelClient} by implementing the {@link ElasticClient} interface.
 */
@RequiredArgsConstructor
public class ElasticSearchClient implements ElasticClient {
    @NonNull
    private final RestHighLevelClient client;

    @Override
    public BulkResponse bulk(BulkRequest request, RequestOptions requestOptions) throws IOException {
        return client.bulk(request, requestOptions);
    }

    @Override
    public void close() throws Exception {
        client.close();
    }
}
