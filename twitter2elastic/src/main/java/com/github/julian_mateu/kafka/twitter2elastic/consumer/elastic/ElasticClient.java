package com.github.julian_mateu.kafka.twitter2elastic.consumer.elastic;

import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.client.RequestOptions;

import java.io.IOException;

/**
 * Interface to wrap an ElasticSearch {@link org.elasticsearch.client.RestHighLevelClient}.
 */
public interface ElasticClient extends AutoCloseable {
    BulkResponse bulk(BulkRequest request, RequestOptions requestOptions) throws IOException;
}
