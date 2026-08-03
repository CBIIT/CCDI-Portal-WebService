package gov.nih.nci.bento.service;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
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

import static gov.nih.nci.bento.service.ESService.SCROLL_ENDPOINT;
import static gov.nih.nci.bento.service.ESServiceTestSupport.emptyScrollResponse;
import static gov.nih.nci.bento.service.ESServiceTestSupport.mockJsonResponse;
import static gov.nih.nci.bento.service.ESServiceTestSupport.mockResponseFromFixture;
import static gov.nih.nci.bento.service.ESServiceTestSupport.scrollHitsResponse;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Phase 5: unit tests for {@link ESService} scroll pagination and field collection.
 */
@ExtendWith(MockitoExtension.class)
class ESServiceScrollTest {

    private static final String[][] PARTICIPANT_PROPERTIES = {
        {"participant_id", "participant_id"},
        {"race", "race"}
    };

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
    void collectPage_withinThreshold_usesFromAndSize() throws IOException {
        Response response = mockResponseFromFixture(200, "search_hits_page.json");
        when(mockClient.performRequest(any(Request.class))).thenReturn(response);

        Request request = new Request("GET", "/participants/_search");
        Map<String, Object> query = new HashMap<>(Map.of("query", Map.of("match_all", Map.of())));

        List<Map<String, Object>> page =
                service.collectPage(request, query, PARTICIPANT_PROPERTIES, 10, 0);

        assertEquals(2, page.size());
        assertEquals("ccdi-int-p001", page.get(0).get("participant_id"));

        ArgumentCaptor<Request> captor = ArgumentCaptor.forClass(Request.class);
        verify(mockClient).performRequest(captor.capture());
        String body = EntityUtils.toString(captor.getValue().getEntity());
        assertTrue(body.contains("\"size\":10"));
        assertTrue(body.contains("\"from\":0"));
    }

    @Test
    void collectPage_beyondThreshold_usesScrollAndReturnsWindow() throws IOException {
        JsonObject initialScroll = scrollHitsResponse("scroll-1", 10_000, "participant_id", 0);
        JsonObject followUpScroll = scrollHitsResponse("scroll-1", 10, "participant_id", 10_000);
        when(mockClient.performRequest(any(Request.class))).thenAnswer(invocation -> {
            Request request = invocation.getArgument(0);
            if ("DELETE".equals(request.getMethod()) && SCROLL_ENDPOINT.equals(request.getEndpoint())) {
                return mockJsonResponse(200, "{\"succeeded\":true}");
            }
            if ("POST".equals(request.getMethod()) && SCROLL_ENDPOINT.equals(request.getEndpoint())) {
                return mockJsonResponse(200, ESServiceTestSupport.GSON.toJson(followUpScroll));
            }
            return mockJsonResponse(200, ESServiceTestSupport.GSON.toJson(initialScroll));
        });

        Request request = new Request("GET", "/participants/_search");
        Map<String, Object> query = new HashMap<>(Map.of("query", Map.of("match_all", Map.of())));

        List<Map<String, Object>> page =
                service.collectPage(request, query, new String[][] {{"participant_id", "participant_id"}}, 10, 10000);

        assertEquals(10, page.size());
        assertEquals("participant_id-10000", page.get(0).get("participant_id"));
        assertEquals("participant_id-10009", page.get(9).get("participant_id"));

        verify(mockClient, atLeastOnce()).performRequest(any(Request.class));
    }

    @Test
    void collectPage_pageSizeAboveMax_throwsIOException() {
        Request request = new Request("GET", "/participants/_search");
        Map<String, Object> query = Map.of("query", Map.of("match_all", Map.of()));

        assertThrows(
                IOException.class,
                () -> service.collectPage(request, query, PARTICIPANT_PROPERTIES, 200_001, 0));
    }

    @Test
    void collectScrollPage_appliesOffsetWithinScrollBatch() throws IOException {
        JsonObject response = ESServiceTestSupport.loadResponseFixture("search_hits_page.json");
        JsonArray hits = response.getAsJsonObject("hits").getAsJsonArray("hits");
        String[][] properties = {{"participant_id", "participant_id"}};

        List<Map<String, Object>> page = service.collectScrollPage(hits, properties, 1, 1);

        assertEquals(1, page.size());
        assertEquals("ccdi-int-p002", page.get(0).get("participant_id"));
    }

    @Test
    void collectField_scrollsUntilEmptyThenClearsContext() throws IOException {
        JsonObject firstBatch = scrollHitsResponse("scroll-field", 2, "participant_id", 100);
        JsonObject emptyBatch = emptyScrollResponse("scroll-field");
        when(mockClient.performRequest(any(Request.class))).thenAnswer(invocation -> {
            Request request = invocation.getArgument(0);
            if ("DELETE".equals(request.getMethod()) && SCROLL_ENDPOINT.equals(request.getEndpoint())) {
                return mockJsonResponse(200, "{\"succeeded\":true}");
            }
            if ("POST".equals(request.getMethod()) && SCROLL_ENDPOINT.equals(request.getEndpoint())) {
                return mockJsonResponse(200, ESServiceTestSupport.GSON.toJson(emptyBatch));
            }
            return mockJsonResponse(200, ESServiceTestSupport.GSON.toJson(firstBatch));
        });

        Request request = new Request("GET", "/participants/_search");
        List<String> values = service.collectField(request, "participant_id");

        assertEquals(List.of("participant_id-100", "participant_id-101"), values);
        verify(mockClient, atLeastOnce()).performRequest(any(Request.class));
    }
}
