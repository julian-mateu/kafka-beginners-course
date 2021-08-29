package com.github.julian_mateu.kafka.twitter2elastic.consumer.elastic;

import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.RequestOptions;

import java.io.IOException;

/**
 * Interface to wrap an ElasticSearch {@link org.elasticsearch.client.RestHighLevelClient}.
 */
public interface ElasticClient extends AutoCloseable {
    IndexResponse index(IndexRequest request, RequestOptions requestOptions) throws IOException;
}
