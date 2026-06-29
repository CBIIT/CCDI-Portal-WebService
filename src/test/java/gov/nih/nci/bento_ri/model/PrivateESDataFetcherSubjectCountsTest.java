package gov.nih.nci.bento_ri.model;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import gov.nih.nci.bento_ri.service.InventoryESService;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.client.Request;

import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anySet;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Phase 4: unit tests for facet/subject count helpers on {@link PrivateESDataFetcher}.
 */
@ExtendWith(MockitoExtension.class)
class PrivateESDataFetcherSubjectCountsTest {

    @Test
    void subjectCountBy_raceTerm_usesCardinalityCounts() throws Exception {
        InventoryESService inventoryESService = mock(InventoryESService.class);
        Map<String, Object> baseQuery = Map.of("query", Map.of("match_all", Map.of()));
        when(inventoryESService.buildFacetFilterQuery(
                        any(), anySet(), anySet(), anySet(), any(), eq("participants")))
                .thenReturn(baseQuery);
        when(inventoryESService.addAggregations(any(), any(String[].class), eq("pid"), anyList()))
                .thenAnswer(invocation -> {
                    Map<String, Object> query = new java.util.HashMap<>(invocation.getArgument(0));
                    query.put("size", 0);
                    return query;
                });
        JsonObject response = PrivateESDataFetcherTestSupport.loadFixture("subject_count_term_aggs.json");
        when(inventoryESService.send(any(Request.class))).thenReturn(response);
        JsonArray buckets = PrivateESDataFetcherTestSupport.termBuckets("subject_count_term_aggs.json", "race");
        when(inventoryESService.collectTermAggs(any(JsonObject.class), eq(new String[] {"race"})))
                .thenReturn(Map.of("race", buckets));

        PrivateESDataFetcher fetcher = PrivateESDataFetcherTestSupport.newFetcher(inventoryESService);
        Map<String, Object> params = Map.of("race", List.of("Asian"));

        @SuppressWarnings("unchecked")
        List<Map<String, Object>> groups = (List<Map<String, Object>>) PrivateESDataFetcherTestSupport.invoke(
                fetcher,
                "subjectCountBy",
                new Class[] {String.class, Map.class, String.class, String.class, String.class},
                "race",
                params,
                "/participants/_search",
                "pid",
                "participants");

        assertEquals(2, groups.size());
        assertEquals("Asian", groups.get(0).get("group"));
        assertEquals(8, groups.get(0).get("subjects"));
    }

    @Test
    void filterSubjectCountBy_excludesCategoryFromFacetFilter() throws Exception {
        InventoryESService inventoryESService = mock(InventoryESService.class);
        when(inventoryESService.buildFacetFilterQuery(
                        any(), anySet(), eq(Set.of("first", "race")), anySet(), any(), eq("participants")))
                .thenReturn(Map.of("query", Map.of("match_all", Map.of())));
        when(inventoryESService.addAggregations(any(), any(String[].class), eq("pid"), anyList()))
                .thenAnswer(invocation -> invocation.getArgument(0));
        JsonObject response = PrivateESDataFetcherTestSupport.loadFixture("subject_count_term_aggs.json");
        when(inventoryESService.send(any(Request.class))).thenReturn(response);
        JsonArray buckets = PrivateESDataFetcherTestSupport.termBuckets("subject_count_term_aggs.json", "race");
        when(inventoryESService.collectTermAggs(any(JsonObject.class), eq(new String[] {"race"})))
                .thenReturn(Map.of("race", buckets));

        PrivateESDataFetcher fetcher = PrivateESDataFetcherTestSupport.newFetcher(inventoryESService);
        Map<String, Object> params = Map.of("race", List.of("Asian"), "first", 10);

        @SuppressWarnings("unchecked")
        List<Map<String, Object>> groups = (List<Map<String, Object>>) PrivateESDataFetcherTestSupport.invoke(
                fetcher,
                "filterSubjectCountBy",
                new Class[] {String.class, Map.class, String.class, String.class, String.class},
                "race",
                params,
                "/participants/_search",
                "pid",
                "participants");

        assertEquals(2, groups.size());
    }

    @Test
    void subjectCountBy_ageAtDiagnosisRange_usesRangeStatsAggregation() throws Exception {
        InventoryESService inventoryESService = mock(InventoryESService.class);
        when(inventoryESService.buildFacetFilterQuery(
                        any(), anySet(), anySet(), anySet(), any(), eq("participants")))
                .thenReturn(Map.of("query", Map.of("match_all", Map.of())));
        when(inventoryESService.addRangeAggregations(any(), eq("age_at_diagnosis"), anyList()))
                .thenAnswer(invocation -> invocation.getArgument(0));
        JsonObject response = PrivateESDataFetcherTestSupport.loadFixture("range_stats_aggs.json");
        when(inventoryESService.send(any(Request.class))).thenReturn(response);
        JsonObject stats = response.getAsJsonObject("aggregations")
                .getAsJsonObject("inner")
                .getAsJsonObject("range_stats");
        when(inventoryESService.collectRangAggs(any(JsonObject.class), eq("age_at_diagnosis")))
                .thenReturn(Map.of("age_at_diagnosis", stats));

        PrivateESDataFetcher fetcher = PrivateESDataFetcherTestSupport.newFetcher(inventoryESService);
        Map<String, Object> params = Map.of("age_at_diagnosis", List.of(0, 18));

        @SuppressWarnings("unchecked")
        List<Map<String, Object>> groups = (List<Map<String, Object>>) PrivateESDataFetcherTestSupport.invoke(
                fetcher,
                "subjectCountBy",
                new Class[] {String.class, Map.class, String.class, String.class, String.class},
                "age_at_diagnosis",
                params,
                "/diagnosis/_search",
                "pid",
                "participants");

        assertEquals(1, groups.size());
        assertEquals(5, groups.get(0).get("lowerBound"));
        assertEquals(65, groups.get(0).get("upperBound"));
        assertEquals(100, groups.get(0).get("subjects"));
    }

    @Test
    void subjectCountByRange_usesRangeCountBuckets() throws Exception {
        InventoryESService inventoryESService = mock(InventoryESService.class);
        when(inventoryESService.buildFacetFilterQuery(
                        any(), anySet(), anySet(), anySet(), any(), eq("participants")))
                .thenReturn(Map.of("query", Map.of("match_all", Map.of())));
        when(inventoryESService.addRangeCountAggregations(any(), eq("age_at_treatment_start"), eq("pid")))
                .thenAnswer(invocation -> invocation.getArgument(0));
        JsonObject response = PrivateESDataFetcherTestSupport.loadFixture("subject_count_range_count_aggs.json");
        when(inventoryESService.send(any(Request.class))).thenReturn(response);
        JsonArray buckets = PrivateESDataFetcherTestSupport.rangeCountBuckets(
                "subject_count_range_count_aggs.json", "age_at_treatment_start");
        when(inventoryESService.collectRangCountAggs(any(JsonObject.class), eq("age_at_treatment_start")))
                .thenReturn(Map.of("age_at_treatment_start", buckets));

        PrivateESDataFetcher fetcher = PrivateESDataFetcherTestSupport.newFetcher(inventoryESService);
        Map<String, Object> params = Map.of("age_at_treatment_start", List.of(0, 18));

        @SuppressWarnings("unchecked")
        List<Map<String, Object>> groups = (List<Map<String, Object>>) PrivateESDataFetcherTestSupport.invoke(
                fetcher,
                "subjectCountByRange",
                new Class[] {String.class, Map.class, String.class, String.class, String.class},
                "age_at_treatment_start",
                params,
                "/treatment/_search",
                "pid",
                "participants");

        assertEquals(2, groups.size());
        assertEquals("0 - 4", groups.get(0).get("group"));
        assertEquals(2, groups.get(0).get("subjects"));
    }
}
