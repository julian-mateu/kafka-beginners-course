package com.github.julian_mateu.kafka.twitter2elastic.consumer.elastic;

import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
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
    public IndexResponse index(IndexRequest request, RequestOptions requestOptions) throws IOException {
        return client.index(request, requestOptions);
    }

    @Override
    public void close() throws Exception {
        client.close();
    }
}
