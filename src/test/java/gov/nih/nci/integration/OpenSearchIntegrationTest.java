package gov.nih.nci.integration;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import org.apache.http.HttpHost;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.util.EntityUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.opensearch.client.Request;
import org.opensearch.client.Response;
import org.opensearch.client.RestClient;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Simple integration test for OpenSearch connectivity.
 * This lightweight test verifies OpenSearch is accessible without starting the full Spring application.
 * 
 * Test naming follows *IntegrationTest.java pattern for maven-failsafe-plugin.
 */
public class OpenSearchIntegrationTest {

    /** Document id / participant_id in {@code integration_minimal_bulk.ndjson} */
    private static final String FIXTURE_PARTICIPANT_ID = "ccdi-int-p001";

    private RestClient restClient;
    
    @BeforeEach
    public void setup() {
        // Get OpenSearch connection details from environment variables (set in GitHub Actions)
        String esHost = System.getenv().getOrDefault("ES_HOST", "localhost");
        int esPort = Integer.parseInt(System.getenv().getOrDefault("ES_PORT", "9200"));
        String esScheme = System.getenv().getOrDefault("ES_SCHEME", "http");
        
        // Create OpenSearch REST client
        restClient = RestClient.builder(
            new HttpHost(esHost, esPort, esScheme)
        ).build();
    }
    
    @AfterEach
    public void teardown() throws Exception {
        if (restClient != null) {
            restClient.close();
        }
    }
    
    /**
     * Verify OpenSearch is accessible and responding
     */
    @Test
    public void testOpenSearchIsAccessible() throws Exception {
        // Create a simple GET request to the cluster health endpoint
        Request request = new Request("GET", "/_cluster/health");
        
        // Execute the request
        Response response = restClient.performRequest(request);
        
        // Verify we got a successful response
        assertNotNull(response, "Response should not be null");
        assertEquals(200, response.getStatusLine().getStatusCode(), "OpenSearch should return 200 OK");
        
        System.out.println("✓ OpenSearch integration test passed - service is accessible");
    }

    /**
     * After {@code scripts/init-integration-opensearch.sh} (or CI equivalent), CCDI model
     * indices must exist and contain at least one integration fixture document per index
     * (see {@code src/test/resources/opensearch/fixtures/integration_minimal_bulk.ndjson}).
     */
    @Test
    public void testCcdiModelIndicesExist() throws Exception {
        Request request = new Request("GET", "/_cat/indices?h=index&format=text&s=index");
        Response response = restClient.performRequest(request);
        assertEquals(200, response.getStatusLine().getStatusCode());
        String body = EntityUtils.toString(response.getEntity());
        assertNotNull(body);
        assertTrue(body.contains("cohorts"), () -> "Expected 'cohorts' index; _cat/indices:\n" + body);
        assertTrue(body.contains("participants"), () -> "Expected 'participants' index; _cat/indices:\n" + body);
        assertTrue(body.contains("files"), () -> "Expected 'files' index; _cat/indices:\n" + body);
        assertTrue(body.contains("model_nodes"), () -> "Expected 'model_nodes' index; _cat/indices:\n" + body);
    }

    /**
     * Verifies bulk fixtures were indexed (init script runs {@code seed-integration-opensearch-fixtures.sh}).
     */
    @Test
    public void testIntegrationFixtureParticipantIsSearchable() throws Exception {
        String queryJson = "{\"query\":{\"term\":{\"participant_id\":\"" + FIXTURE_PARTICIPANT_ID + "\"}},\"size\":1}";
        Request request = new Request("POST", "/participants/_search");
        request.setEntity(new StringEntity(queryJson, ContentType.APPLICATION_JSON));

        Response response = restClient.performRequest(request);
        assertEquals(200, response.getStatusLine().getStatusCode());
        String responseBody = EntityUtils.toString(response.getEntity());
        JsonObject root = JsonParser.parseString(responseBody).getAsJsonObject();
        JsonObject hits = root.getAsJsonObject("hits");
        long hitCount;
        if (hits.get("total").isJsonObject()) {
            hitCount = hits.getAsJsonObject("total").get("value").getAsLong();
        } else {
            hitCount = hits.get("total").getAsLong();
        }
        assertTrue(hitCount >= 1,
                () -> "Expected fixture participant in participants index; total=" + hitCount + " body=" + responseBody);
    }
}


