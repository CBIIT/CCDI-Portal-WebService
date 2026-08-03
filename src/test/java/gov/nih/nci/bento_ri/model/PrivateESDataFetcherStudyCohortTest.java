package gov.nih.nci.bento_ri.model;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import gov.nih.nci.bento_ri.service.InventoryESService;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.client.Request;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anySet;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Unit tests for study and cohort resolvers on {@link PrivateESDataFetcher}.
 */
@ExtendWith(MockitoExtension.class)
class PrivateESDataFetcherStudyCohortTest {

    private void stubTermCountChain(InventoryESService inventoryESService) throws Exception {
        when(inventoryESService.buildFacetFilterQuery(
                        any(), anySet(), anySet(), anySet(), anyString(), anyString()))
                .thenReturn(PrivateESDataFetcherTestSupport.mutableQuery());
        when(inventoryESService.addAggregations(any(), any(String[].class), any(), anyList()))
                .thenAnswer(invocation -> invocation.getArgument(0));
        JsonObject response = PrivateESDataFetcherTestSupport.loadFixture("subject_count_term_aggs.json");
        when(inventoryESService.send(any(Request.class))).thenReturn(response);
        JsonArray buckets = PrivateESDataFetcherTestSupport.termBuckets("subject_count_term_aggs.json", "race");
        when(inventoryESService.collectTermAggs(any(JsonObject.class), any(String[].class)))
                .thenAnswer(invocation -> Map.of(((String[]) invocation.getArgument(1))[0], buckets));
    }

    @Test
    void studyDetails_returnsStudyWithFacetCountsAndSupportingData() throws Exception {
        InventoryESService inventoryESService = mock(InventoryESService.class);
        when(inventoryESService.buildFacetFilterQuery(
                        any(), anySet(), anySet(), anySet(), anyString(), anyString()))
                .thenReturn(PrivateESDataFetcherTestSupport.mutableQuery());
        when(inventoryESService.collectPage(
                        any(Request.class), any(), any(String[][].class), anyInt(), anyInt()))
                .thenReturn(List.of(new HashMap<>(Map.of(
                        "study_id", "phs002790",
                        "study_name", "MCI Study",
                        "dbgap_accession", "phs002790",
                        "num_of_participants", 100))));
        stubTermCountChain(inventoryESService);

        PrivateESDataFetcher fetcher = PrivateESDataFetcherTestSupport.newFetcher(inventoryESService);

        @SuppressWarnings("unchecked")
        Map<String, Object> result = (Map<String, Object>) PrivateESDataFetcherTestSupport.invoke(
                fetcher, "studyDetails", new Class[] {Map.class}, Map.of("study_id", "phs002790"));

        assertEquals("MCI Study", result.get("study_name"));
        assertTrue(result.containsKey("diagnoses"));
        @SuppressWarnings("unchecked")
        List<Map<String, Object>> supportingData = (List<Map<String, Object>>) result.get("supporting_data");
        assertEquals(1, supportingData.size());
        assertEquals("IDC", supportingData.get(0).get("data_category"));
    }

    @Test
    void cohortMetadata_groupsParticipantsByDbgapAccessionAndSortsSurvivals() throws Exception {
        InventoryESService inventoryESService = mock(InventoryESService.class);
        when(inventoryESService.buildFacetFilterQuery(
                        any(), anySet(), anySet(), anySet(), anyString(), anyString()))
                .thenReturn(PrivateESDataFetcherTestSupport.mutableQuery());
        List<Map<String, Object>> survivals = new ArrayList<>(List.of(
                Map.of("age_at_last_known_survival_status", 20),
                Map.of("age_at_last_known_survival_status", 5)));
        when(inventoryESService.collectPage(
                        any(Request.class), any(), any(String[][].class), anyInt(), anyInt()))
                .thenReturn(List.of(
                        Map.of(
                                "participant_id", "p1",
                                "dbgap_accession", "phs001",
                                "survivals", survivals),
                        Map.of("participant_id", "p2", "dbgap_accession", "phs001")));

        PrivateESDataFetcher fetcher = PrivateESDataFetcherTestSupport.newFetcher(inventoryESService);

        @SuppressWarnings("unchecked")
        List<Map<String, Object>> result = (List<Map<String, Object>>) PrivateESDataFetcherTestSupport.invoke(
                fetcher,
                "cohortMetadata",
                new Class[] {Map.class},
                PrivateESDataFetcherTestSupport.overviewParams(10, 0, "participant_id", "asc"));

        assertEquals(1, result.size());
        assertEquals("phs001", result.get(0).get("dbgap_accession"));
        @SuppressWarnings("unchecked")
        List<Map<String, Object>> grouped = (List<Map<String, Object>>) result.get(0).get("participants");
        assertEquals(2, grouped.size());
        @SuppressWarnings("unchecked")
        List<Map<String, Object>> sortedSurvivals =
                (List<Map<String, Object>>) grouped.get(0).get("survivals");
        assertEquals(5, sortedSurvivals.get(0).get("age_at_last_known_survival_status"));
    }

    @Test
    void cohortCharts_noChartsParam_returnsEmptyList() throws Exception {
        PrivateESDataFetcher fetcher = PrivateESDataFetcherTestSupport.newFetcher(mock(InventoryESService.class));

        @SuppressWarnings("unchecked")
        List<Map<String, Object>> result = (List<Map<String, Object>>) PrivateESDataFetcherTestSupport.invoke(
                fetcher, "cohortCharts", new Class[] {Map.class}, Map.of("c1", List.of("id1")));

        assertTrue(result.isEmpty());
    }

    @Test
    void cohortCharts_countChart_returnsGroupedCounts() throws Exception {
        InventoryESService inventoryESService = mock(InventoryESService.class);
        stubTermCountChain(inventoryESService);
        when(inventoryESService.getBucketNames(
                        anyString(), any(), anySet(), any(), anyString(), anyString()))
                .thenReturn(List.of("Asian", "White"));

        PrivateESDataFetcher fetcher = PrivateESDataFetcherTestSupport.newFetcher(inventoryESService);
        Map<String, Object> params = Map.of(
                "c1", List.of("id-1"),
                "charts", List.of(Map.of("property", "race", "type", "count")));

        @SuppressWarnings("unchecked")
        List<Map<String, Object>> result = (List<Map<String, Object>>) PrivateESDataFetcherTestSupport.invoke(
                fetcher, "cohortCharts", new Class[] {Map.class}, params);

        assertEquals(1, result.size());
        assertEquals("race", result.get(0).get("property"));
        @SuppressWarnings("unchecked")
        List<Map<String, Object>> cohorts = (List<Map<String, Object>>) result.get(0).get("cohorts");
        assertEquals("c1", cohorts.get(0).get("cohort"));
    }
}
