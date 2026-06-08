package gov.nih.nci.bento_ri.service;

import com.google.gson.JsonObject;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.client.Request;
import org.opensearch.client.Response;
import org.opensearch.client.RestClient;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Phase 1: unit tests for {@link InventoryESService} HTTP helpers ({@code send}, {@code getCount},
 * {@code getBucketNames}) with a mocked OpenSearch client (no live cluster).
 */
@ExtendWith(MockitoExtension.class)
class InventoryESServiceHttpMethodsTest {

    private InventoryESService service;
    private RestClient mockClient;

    @BeforeEach
    void setUp() throws Exception {
        service = InventoryESServiceTestSupport.newService();
        mockClient = mock(RestClient.class);
        InventoryESServiceTestSupport.setClientForTest(service, mockClient);
    }

    @AfterEach
    void tearDown() throws Exception {
        InventoryESServiceTestSupport.closeClient(service);
    }

    @Test
    void send_success_returnsParsedJsonObject() throws IOException {
        Response response = InventoryESServiceTestSupport.mockResponseFromFixture(200, "count_response.json");
        when(mockClient.performRequest(any(Request.class))).thenReturn(response);

        JsonObject result = service.send(new Request("GET", "/participants/_count"));

        assertEquals(42, result.get("count").getAsInt());
    }

    @Test
    void send_non200Status_throwsIOException() throws IOException {
        Response response = InventoryESServiceTestSupport.mockJsonResponse(503, "{\"error\":\"unavailable\"}");
        when(mockClient.performRequest(any(Request.class))).thenReturn(response);

        assertThrows(IOException.class, () -> service.send(new Request("GET", "/participants/_search")));
    }

    @Test
    void getCount_postsQueryToCountEndpoint() throws IOException {
        Response response = InventoryESServiceTestSupport.mockResponseFromFixture(200, "count_response.json");
        when(mockClient.performRequest(any(Request.class))).thenReturn(response);

        Map<String, Object> query = Map.of("query", Map.of("match_all", Map.of()));
        int count = service.getCount(query, "participants");

        assertEquals(42, count);

        ArgumentCaptor<Request> requestCaptor = ArgumentCaptor.forClass(Request.class);
        verify(mockClient).performRequest(requestCaptor.capture());
        Request captured = requestCaptor.getValue();
        assertEquals("GET", captured.getMethod());
        assertEquals("/participants/_count", captured.getEndpoint());
        assertTrue(captured.getEntity() != null);
    }

    @Test
    void getBucketNames_buildsQueryAndReturnsBucketKeys() throws IOException {
        Response response = InventoryESServiceTestSupport.mockResponseFromFixture(200, "bucket_names_aggs.json");
        when(mockClient.performRequest(any(Request.class))).thenReturn(response);

        List<String> bucketNames = service.getBucketNames(
                "treatment_type",
                Map.of("race", List.of("Asian")),
                Set.of("age_at_diagnosis"),
                "participant_id",
                "participants",
                "/participants/_search");

        assertEquals(List.of("Chemotherapy", "Radiation"), bucketNames);

        ArgumentCaptor<Request> requestCaptor = ArgumentCaptor.forClass(Request.class);
        verify(mockClient).performRequest(requestCaptor.capture());
        Request captured = requestCaptor.getValue();
        assertEquals("/participants/_search", captured.getEndpoint());
        assertTrue(captured.getEntity() != null);
    }
}
