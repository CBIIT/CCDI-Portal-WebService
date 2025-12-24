package gov.nih.nci.bento_ri.service;

import org.apache.http.HttpHost;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.opensearch.client.Request;
import org.opensearch.client.Response;
import org.opensearch.client.RestClient;

import static org.junit.Assert.*;

/**
 * Integration test for OpenSearch connectivity.
 * This is a lightweight test that verifies OpenSearch is accessible without starting the full Spring application.
 * 
 * Naming convention: *IntegrationTest.java or *IT.java
 * This will be picked up by maven-failsafe-plugin during 'mvn verify'
 */
public class InventoryESServiceIntegrationTest {
    
    private RestClient restClient;
    
    @Before
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
    
    @After
    public void teardown() throws Exception {
        if (restClient != null) {
            restClient.close();
        }
    }
    
    @Test
    public void testOpenSearchConnection() throws Exception {
        // Create a simple GET request to the cluster health endpoint
        Request request = new Request("GET", "/_cluster/health");
        
        // Execute the request
        Response response = restClient.performRequest(request);
        
        // Verify we got a successful response
        assertNotNull("Response should not be null", response);
        assertEquals("OpenSearch should return 200 OK", 200, response.getStatusLine().getStatusCode());
        
        System.out.println("✓ Successfully connected to OpenSearch");
    }
    
    @Test
    public void testOpenSearchClusterInfo() throws Exception {
        // Get cluster information
        Request request = new Request("GET", "/");
        
        // Execute the request
        Response response = restClient.performRequest(request);
        
        // Verify response
        assertNotNull("Response should not be null", response);
        assertEquals("OpenSearch should return 200 OK", 200, response.getStatusLine().getStatusCode());
        
        // Verify we can read the response entity
        assertNotNull("Response entity should not be null", response.getEntity());
        
        System.out.println("✓ Successfully retrieved OpenSearch cluster info");
    }
    
    @Test
    public void testOpenSearchCreateAndDeleteIndex() throws Exception {
        String testIndexName = "test-index-" + System.currentTimeMillis();
        
        try {
            // Create a test index
            Request createRequest = new Request("PUT", "/" + testIndexName);
            Response createResponse = restClient.performRequest(createRequest);
            
            assertEquals("Index creation should return 200", 200, createResponse.getStatusLine().getStatusCode());
            System.out.println("✓ Successfully created test index: " + testIndexName);
            
            // Verify the index exists
            Request getRequest = new Request("GET", "/" + testIndexName);
            Response getResponse = restClient.performRequest(getRequest);
            
            assertEquals("Index should exist", 200, getResponse.getStatusLine().getStatusCode());
            System.out.println("✓ Verified test index exists");
            
        } finally {
            // Clean up: Delete the test index
            try {
                Request deleteRequest = new Request("DELETE", "/" + testIndexName);
                Response deleteResponse = restClient.performRequest(deleteRequest);
                
                assertEquals("Index deletion should return 200", 200, deleteResponse.getStatusLine().getStatusCode());
                System.out.println("✓ Successfully deleted test index");
            } catch (Exception e) {
                // Ignore cleanup errors
                System.err.println("Warning: Failed to delete test index: " + e.getMessage());
            }
        }
    }
}
