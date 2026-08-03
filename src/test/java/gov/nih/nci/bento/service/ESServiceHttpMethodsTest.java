package gov.nih.nci.bento.service;

import com.google.gson.JsonObject;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.client.Request;
import org.opensearch.client.Response;
import org.opensearch.client.RestClient;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static gov.nih.nci.bento.service.ESServiceTestSupport.mockJsonResponse;
import static gov.nih.nci.bento.service.ESServiceTestSupport.mockResponseFromFixture;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Phase 3: unit tests for {@link ESService} HTTP helpers with mocked OpenSearch client.
 */
@ExtendWith(MockitoExtension.class)
class ESServiceHttpMethodsTest {

    private ESService service;
    private RestClient mockClient;

    @BeforeEach
    void setUp() throws Exception {
        service = ESServiceTestSupport.newService();
        mockClient = mock(RestClient.class);
        ESServiceTestSupport.setClientForTest(service, mockClient);
    }

    @AfterEach
    void tearDown() throws Exception {
        ESServiceTestSupport.closeClient(service);
    }

    @Test
    void send_success_returnsParsedJson() throws IOException {
        Response response = mockResponseFromFixture(200, "count_response.json");
        when(mockClient.performRequest(any(Request.class))).thenReturn(response);

        JsonObject result = service.send(new Request("GET", "/subjects/_count"));

        assertEquals(42, result.get("count").getAsInt());
    }

    @Test
    void send_non200_throwsIOException() throws IOException {
        Response response = mockJsonResponse(500, "{}");
        when(mockClient.performRequest(any(Request.class))).thenReturn(response);

        assertThrows(IOException.class, () -> service.send(new Request("GET", "/subjects/_search")));
    }

    @Test
    void getFilteredGroupCount_parsesDiagnosesBuckets() throws IOException {
        Response response = mockResponseFromFixture(200, "group_count_aggs.json");
        when(mockClient.performRequest(any(Request.class))).thenReturn(response);

        List<Map<String, Object>> groups = service.getFilteredGroupCount(Map.of(), "/diagnosis/_search", "diagnoses");

        assertEquals(2, groups.size());
        assertEquals("Leukemia", groups.get(0).get("group"));
        assertEquals(10, groups.get(0).get("subjects"));
    }
}
