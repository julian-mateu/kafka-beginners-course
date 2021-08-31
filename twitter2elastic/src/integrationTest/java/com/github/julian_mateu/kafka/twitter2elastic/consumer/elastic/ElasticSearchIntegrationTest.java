package com.github.julian_mateu.kafka.twitter2elastic.consumer.elastic;


import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.SneakyThrows;
import org.apache.lucene.search.TotalHits;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.admin.indices.refresh.RefreshRequest;
import org.elasticsearch.action.admin.indices.refresh.RefreshResponse;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.HashMap;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Integration tests to run against a local ElasticSearch single node cluster, to be used as learning tests.
 *
 * @see <a href="https://www.elastic.co/guide/en/elasticsearch/client/java-rest/master/java-rest-high.html">RestHighLevelClient</a>
 */
public class ElasticSearchIntegrationTest {

    private static final String INTEGRATION_TEST_INDEX = "integration_test";
    private static final String HOSTNAME = "localhost";
    private static final String SCHEME = "http";
    private static final int PORT = 9200;

    private static final String TEST_PAYLOAD = "{\"name\":\"integration_test\"}";
    private static final String DOCUMENT_ID = "1";

    private static final IntegrationTestElasticSearchWriterFactory ELASTIC_SEARCH_WRITER_FACTORY =
            new IntegrationTestElasticSearchWriterFactory(
                    INTEGRATION_TEST_INDEX, HOSTNAME, SCHEME, PORT
            );

    private ElasticSearchWriter elasticSearchWriter;
    private RestHighLevelClient client;

    private static void assertFalseOrNull(Boolean value) {
        if (value == null) {
            assertNull(value);
        } else {
            assertFalse(value);
        }
    }

    @BeforeEach
    public void setUp() {
        elasticSearchWriter = ELASTIC_SEARCH_WRITER_FACTORY.getWriter();
        client = ELASTIC_SEARCH_WRITER_FACTORY.getClient();
    }

    @AfterEach
    public void cleanup() throws Exception {
        elasticSearchWriter.close();
    }

    @Test
    public void createAndRetrieveNewDocumentUsingSearch() throws IOException {
        // Given
        String documentId = DOCUMENT_ID;
        String payload = TEST_PAYLOAD;

        // When
        submitNewDocument(documentId, payload);

        // Then
        assertDocumentExists(documentId);
        assertMatchAllSearchReturnsOnlyOneDocument(documentId, payload);
    }

    @SneakyThrows(IOException.class)
    private void assertDocumentExists(String documentId) {
        GetRequest getIndexRequest = new GetRequest(INTEGRATION_TEST_INDEX, documentId);
        GetResponse getIndexResponse = client.get(getIndexRequest, RequestOptions.DEFAULT);
        assertTrue(getIndexResponse.isExists());
    }

    private void assertMatchAllSearchReturnsOnlyOneDocument(String documentId, String payload) throws IOException {
        // Given
        SearchRequest searchRequest = new SearchRequest(INTEGRATION_TEST_INDEX);
        searchRequest.source(SearchSourceBuilder.searchSource().query(QueryBuilders.matchAllQuery()));

        // When
        SearchResponse response = client.search(searchRequest, RequestOptions.DEFAULT);

        // Then
        assertEquals(RestStatus.OK, response.status());
        assertFalseOrNull(response.isTerminatedEarly());
        assertFalse(response.isTimedOut());
        assertEquals(0, response.getFailedShards());

        SearchHits hits = response.getHits();
        assertEquals(1L, hits.getTotalHits().value);
        assertEquals(TotalHits.Relation.EQUAL_TO, hits.getTotalHits().relation);

        SearchHit hit = hits.getAt(0);
        assertEquals(documentId, hit.getId());
        assertEquals(new ObjectMapper().readValue(payload, HashMap.class), hit.getSourceAsMap());
    }

    private void submitNewDocument(String documentId, String payload) throws IOException {
        IndexRequest indexRequest = new IndexRequest(INTEGRATION_TEST_INDEX);
        indexRequest.id(documentId);
        indexRequest.source(payload, XContentType.JSON);
        IndexResponse indexResponse = client.index(indexRequest, RequestOptions.DEFAULT);
        assertEquals(DocWriteResponse.Result.CREATED, indexResponse.getResult());

        RefreshRequest refreshRequest = new RefreshRequest(INTEGRATION_TEST_INDEX);
        RefreshResponse refreshResponse = client.indices().refresh(refreshRequest, RequestOptions.DEFAULT);
        assertEquals(0, refreshResponse.getFailedShards());
    }
}
