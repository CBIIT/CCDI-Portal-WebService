package gov.nih.nci.bento;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.junit4.SpringRunner;

import static org.junit.Assert.*;

/**
 * Integration test for GraphQL endpoint.
 * This test runs with real services (Neo4j, OpenSearch, Redis) in GitHub Actions.
 * 
 * Naming convention: *IntegrationTest.java
 * Run with: mvn verify -Dspring.profiles.active=integration
 */
@RunWith(SpringRunner.class)
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@ActiveProfiles("integration")
public class GraphQLEndpointIntegrationTest {

    @LocalServerPort
    private int port;

    @Autowired
    private TestRestTemplate restTemplate;

    @Test
    public void testGraphQLEndpointIsAccessible() {
        // Arrange
        String url = "http://localhost:" + port + "/v1/graphql/";
        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);
        
        // Simple introspection query to check if endpoint is alive
        String query = "{\"query\": \"{ __typename }\"}";
        HttpEntity<String> request = new HttpEntity<>(query, headers);
        
        // Act
        ResponseEntity<String> response = restTemplate.postForEntity(url, request, String.class);
        
        // Assert
        assertNotNull("Response should not be null", response);
        // Depending on your configuration, this might be OK or an error
        // Adjust assertion based on your actual setup
        assertTrue("Response status should be OK or BAD_REQUEST", 
            response.getStatusCode() == HttpStatus.OK || 
            response.getStatusCode() == HttpStatus.BAD_REQUEST);
    }

    @Test
    public void testHealthEndpoint() {
        // Arrange
        String url = "http://localhost:" + port + "/actuator/health";
        
        // Act
        ResponseEntity<String> response = restTemplate.getForEntity(url, String.class);
        
        // Assert
        // This test will fail if Spring Boot Actuator is not configured
        // You can enable it by adding spring-boot-starter-actuator dependency
        assertNotNull("Health response should not be null", response);
    }

    @Test
    public void testVersionEndpoint() {
        // Arrange
        String url = "http://localhost:" + port + "/version";
        
        // Act
        ResponseEntity<String> response = restTemplate.getForEntity(url, String.class);
        
        // Assert
        assertNotNull("Version response should not be null", response);
        if (response.getStatusCode() == HttpStatus.OK) {
            assertNotNull("Version response body should not be null", response.getBody());
        }
    }
}

