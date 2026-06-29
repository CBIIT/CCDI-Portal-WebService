package gov.nih.nci.bento_ri.model;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import gov.nih.nci.bento_ri.service.InventoryESService;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.client.Request;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anySet;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Phase 4: unit tests for participant resolvers on {@link PrivateESDataFetcher}.
 */
@ExtendWith(MockitoExtension.class)
class PrivateESDataFetcherParticipantsTest {

    @Test
    void participantOverview_collectsPageAndSkipsCpiWhenServiceAbsent() throws Exception {
        InventoryESService inventoryESService = mock(InventoryESService.class);
        when(inventoryESService.buildFacetFilterQuery(
                        any(), anySet(), anySet(), anySet(), any(), eq("participants")))
                .thenReturn(new java.util.HashMap<>(Map.of("query", Map.of("match_all", Map.of()))));
        List<Map<String, Object>> page = List.of(
                Map.of("participant_id", "ccdi-int-p001", "study_id", "CCDI-STUDY", "race", "Asian"),
                Map.of("participant_id", "ccdi-int-p002", "study_id", "CCDI-STUDY", "race", "White"));
        when(inventoryESService.collectPage(
                        any(Request.class), any(), any(String[][].class), anyInt(), anyInt()))
                .thenReturn(page);

        PrivateESDataFetcher fetcher = PrivateESDataFetcherTestSupport.newFetcher(inventoryESService);

        @SuppressWarnings("unchecked")
        List<Map<String, Object>> result = (List<Map<String, Object>>) PrivateESDataFetcherTestSupport.invoke(
                fetcher,
                "participantOverview",
                new Class[] {Map.class},
                PrivateESDataFetcherTestSupport.defaultOverviewParams());

        assertEquals(2, result.size());
        assertEquals("ccdi-int-p001", result.get(0).get("participant_id"));
        verify(inventoryESService)
                .collectPage(any(Request.class), any(), any(String[][].class), eq(10), eq(0));
    }

    @Test
    void getParticipantsCount_returnsTotalFromSearchResponse() throws Exception {
        InventoryESService inventoryESService = mock(InventoryESService.class);
        when(inventoryESService.buildFacetFilterQuery(
                        any(), anySet(), anySet(), anySet(), any(), eq("participants")))
                .thenReturn(Map.of("query", Map.of("match_all", Map.of())));
        JsonObject response = PrivateESDataFetcherTestSupport.loadFixture("participant_search_total.json");
        when(inventoryESService.send(any(Request.class))).thenReturn(response);

        PrivateESDataFetcher fetcher = PrivateESDataFetcherTestSupport.newFetcher(inventoryESService);

        Integer count = (Integer) PrivateESDataFetcherTestSupport.invoke(
                fetcher, "getParticipantsCount", new Class[] {});

        assertEquals(42, count);
    }

    @Test
    void searchParticipants_cacheHit_returnsCachedPayloadWithoutEsCalls() throws Exception {
        InventoryESService inventoryESService = mock(InventoryESService.class);
        PrivateESDataFetcher fetcher = PrivateESDataFetcherTestSupport.newFetcher(inventoryESService);
        Map<String, Object> cached = Map.of("numberOfParticipants", 99, "numberOfStudies", 3);
        PrivateESDataFetcherTestSupport.putCacheEntry(fetcher, "all", cached);
        Map<String, Object> params = Map.of("race", List.of(""));

        @SuppressWarnings("unchecked")
        Map<String, Object> result = (Map<String, Object>) PrivateESDataFetcherTestSupport.invoke(
                fetcher, "searchParticipants", new Class[] {Map.class}, params);

        assertEquals(99, result.get("numberOfParticipants"));
        assertEquals(3, result.get("numberOfStudies"));
        verify(inventoryESService, never()).send(any());
    }

    @Test
    void searchParticipants_cacheMiss_returnsAggregatedCounts() throws Exception {
        InventoryESService inventoryESService = mock(InventoryESService.class);
        when(inventoryESService.buildFacetFilterQuery(
                        any(), anySet(), anySet(), anySet(), any(), anyString()))
                .thenReturn(PrivateESDataFetcherTestSupport.mutableQuery());
        JsonObject aggResponse =
                PrivateESDataFetcherTestSupport.loadFixture("search_participants_agg_response.json");
        JsonObject countResponse = PrivateESDataFetcherTestSupport.countResponse(10);
        when(inventoryESService.send(any(Request.class))).thenAnswer(invocation -> {
            Request request = invocation.getArgument(0);
            if (request.getEndpoint().contains("_count")) {
                return countResponse;
            }
            return aggResponse;
        });
        when(inventoryESService.addAggregations(any(), any(String[].class), any(), anyList()))
                .thenAnswer(invocation -> invocation.getArgument(0));
        when(inventoryESService.addRangeAggregations(any(), anyString(), anyList()))
                .thenAnswer(invocation -> invocation.getArgument(0));
        when(inventoryESService.addRangeCountAggregations(any(), anyString(), any()))
                .thenAnswer(invocation -> invocation.getArgument(0));
        when(inventoryESService.addNodeCountAggregations(any(), anyString()))
                .thenAnswer(invocation -> invocation.getArgument(0));
        JsonArray buckets = PrivateESDataFetcherTestSupport.termBuckets("subject_count_term_aggs.json", "race");
        when(inventoryESService.collectTermAggs(any(JsonObject.class), any(String[].class)))
                .thenAnswer(invocation -> Map.of(((String[]) invocation.getArgument(1))[0], buckets));
        when(inventoryESService.collectRangAggs(any(JsonObject.class), anyString()))
                .thenAnswer(invocation -> {
                    JsonObject ranges = new JsonObject();
                    ranges.addProperty("count", 10);
                    ranges.addProperty("min", 0);
                    ranges.addProperty("max", 18);
                    Map<String, JsonObject> result = new HashMap<>();
                    result.put(invocation.getArgument(1), ranges);
                    return result;
                });
        when(inventoryESService.collectRangCountAggs(any(JsonObject.class), anyString()))
                .thenReturn(Map.of("age_at_diagnosis", buckets));
        when(inventoryESService.collectNodeCountAggs(any(JsonObject.class), eq("study_id")))
                .thenReturn(Map.of("study_id", buckets));

        PrivateESDataFetcher fetcher = PrivateESDataFetcherTestSupport.newFetcher(inventoryESService);
        Map<String, Object> params = Map.of("race", List.of(""));

        @SuppressWarnings("unchecked")
        Map<String, Object> result = (Map<String, Object>) PrivateESDataFetcherTestSupport.invoke(
                fetcher, "searchParticipants", new Class[] {Map.class}, params);

        assertEquals(100, result.get("numberOfParticipants"));
        assertEquals(100, result.get("numberOfSamples"));
        assertEquals(10, result.get("numberOfFiles"));
        assertTrue(result.containsKey("filterParticipantCountByRace"));
    }
}
