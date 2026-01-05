package gov.nih.nci.integration;

import org.apache.http.HttpHost;
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
}


