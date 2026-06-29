package gov.nih.nci.bento_ri.service;

import gov.nih.nci.bento.service.ESService;
import org.apache.http.util.EntityUtils;
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
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static gov.nih.nci.bento_ri.service.InventoryESServiceTestSupport.emptyScrollResponse;
import static gov.nih.nci.bento_ri.service.InventoryESServiceTestSupport.mockJsonResponse;
import static gov.nih.nci.bento_ri.service.InventoryESServiceTestSupport.scrollHitsResponse;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Phase 5: unit tests for inherited scroll pagination on {@link InventoryESService}.
 */
@ExtendWith(MockitoExtension.class)
class InventoryESServiceScrollTest {

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
    void collectPage_withinThreshold_usesInventorySendAndFromSize() throws IOException {
        Response response = InventoryESServiceTestSupport.mockResponseFromFixture(200, "search_hits_page.json");
        when(mockClient.performRequest(any(Request.class))).thenReturn(response);

        Request request = new Request("GET", "/participants/_search");
        Map<String, Object> query = new HashMap<>(Map.of("query", Map.of("match_all", Map.of())));
        String[][] properties = {{"participant_id", "participant_id"}, {"race", "race"}};

        List<Map<String, Object>> page = service.collectPage(request, query, properties, 10, 0);

        assertEquals(2, page.size());
        assertEquals("ccdi-int-p001", page.get(0).get("participant_id"));

        ArgumentCaptor<Request> captor = ArgumentCaptor.forClass(Request.class);
        verify(mockClient).performRequest(captor.capture());
        String body = EntityUtils.toString(captor.getValue().getEntity());
        assertTrue(body.contains("\"size\":10"));
        assertTrue(body.contains("\"from\":0"));
    }

    @Test
    void collectPage_beyondThreshold_usesScrollThroughInventorySend() throws IOException {
        var initialScroll = scrollHitsResponse("inv-scroll-1", 10_000, "participant_id", 0);
        var followUpScroll = scrollHitsResponse("inv-scroll-1", 10, "participant_id", 10_000);
        when(mockClient.performRequest(any(Request.class))).thenAnswer(invocation -> {
            Request request = invocation.getArgument(0);
            if ("DELETE".equals(request.getMethod()) && ESService.SCROLL_ENDPOINT.equals(request.getEndpoint())) {
                return mockJsonResponse(200, "{\"succeeded\":true}");
            }
            if ("POST".equals(request.getMethod()) && ESService.SCROLL_ENDPOINT.equals(request.getEndpoint())) {
                return mockJsonResponse(200, InventoryESServiceTestSupport.GSON.toJson(followUpScroll));
            }
            return mockJsonResponse(200, InventoryESServiceTestSupport.GSON.toJson(initialScroll));
        });

        Request request = new Request("GET", "/participants/_search");
        Map<String, Object> query = new HashMap<>(Map.of("query", Map.of("match_all", Map.of())));

        List<Map<String, Object>> page = service.collectPage(
                request, query, new String[][] {{"participant_id", "participant_id"}}, 10, 10000);

        assertEquals(10, page.size());
        assertEquals("participant_id-10000", page.get(0).get("participant_id"));
        verify(mockClient, atLeastOnce()).performRequest(any(Request.class));
    }

    @Test
    void collectField_scrollsViaInventorySend() throws IOException {
        var firstBatch = scrollHitsResponse("inv-scroll-field", 1, "participant_id", 0);
        var emptyBatch = emptyScrollResponse("inv-scroll-field");
        when(mockClient.performRequest(any(Request.class))).thenAnswer(invocation -> {
            Request request = invocation.getArgument(0);
            if ("DELETE".equals(request.getMethod()) && ESService.SCROLL_ENDPOINT.equals(request.getEndpoint())) {
                return mockJsonResponse(200, "{\"succeeded\":true}");
            }
            if ("POST".equals(request.getMethod()) && ESService.SCROLL_ENDPOINT.equals(request.getEndpoint())) {
                return mockJsonResponse(200, InventoryESServiceTestSupport.GSON.toJson(emptyBatch));
            }
            return mockJsonResponse(200, InventoryESServiceTestSupport.GSON.toJson(firstBatch));
        });

        List<String> values = service.collectField(new Request("GET", "/participants/_search"), "participant_id");

        assertEquals(List.of("participant_id-0"), values);
    }
}
