package gov.nih.nci.bento_ri.model;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import gov.nih.nci.bento_ri.service.InventoryESService;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.client.Request;

import java.util.HashMap;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anySet;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Unit tests for list/manifest resolvers on {@link PrivateESDataFetcher}.
 */
@ExtendWith(MockitoExtension.class)
class PrivateESDataFetcherListsTest {

    private void stubOverviewQuery(InventoryESService inventoryESService) throws Exception {
        when(inventoryESService.buildFacetFilterQuery(
                        any(), anySet(), anySet(), anySet(), anyString(), anyString()))
                .thenReturn(PrivateESDataFetcherTestSupport.mutableQuery());
    }

    @Test
    void studiesListing_collectsPagedStudies() throws Exception {
        InventoryESService inventoryESService = mock(InventoryESService.class);
        stubOverviewQuery(inventoryESService);
        List<Map<String, Object>> page = List.of(
                Map.of("study_id", "phs002790", "study_name", "CCDI Study", "num_of_participants", 100));
        when(inventoryESService.collectPage(
                        any(Request.class), any(), any(String[][].class), anyInt(), anyInt()))
                .thenReturn(page);

        PrivateESDataFetcher fetcher = PrivateESDataFetcherTestSupport.newFetcher(inventoryESService);
        Map<String, Object> params = PrivateESDataFetcherTestSupport.overviewParams(10, 0, "study_id", "asc");

        @SuppressWarnings("unchecked")
        List<Map<String, Object>> result = (List<Map<String, Object>>) PrivateESDataFetcherTestSupport.invoke(
                fetcher, "studiesListing", new Class[] {Map.class}, params);

        assertEquals("phs002790", result.get(0).get("study_id"));
    }

    @Test
    void studyOverview_collectsPagedStudyRecords() throws Exception {
        InventoryESService inventoryESService = mock(InventoryESService.class);
        stubOverviewQuery(inventoryESService);
        when(inventoryESService.addAggregations(any(), any(String[].class)))
                .thenAnswer(invocation -> invocation.getArgument(0));
        when(inventoryESService.send(any(Request.class)))
                .thenReturn(PrivateESDataFetcherTestSupport.loadFixture("subject_count_term_aggs.json"));
        JsonArray buckets = PrivateESDataFetcherTestSupport.termBuckets("subject_count_term_aggs.json", "race");
        when(inventoryESService.collectTermAggs(any(JsonObject.class), any(String[].class)))
                .thenReturn(Map.of("study_id", buckets));
        when(inventoryESService.collectPage(
                        any(Request.class), any(), any(String[][].class), anyInt(), anyInt()))
                .thenReturn(List.of(new HashMap<>(Map.of("study_id", "phs002790", "study_name", "CCDI Study"))));

        PrivateESDataFetcher fetcher = PrivateESDataFetcherTestSupport.newFetcher(inventoryESService);

        @SuppressWarnings("unchecked")
        List<Map<String, Object>> result = (List<Map<String, Object>>) PrivateESDataFetcherTestSupport.invoke(
                fetcher,
                "studyOverview",
                new Class[] {Map.class},
                PrivateESDataFetcherTestSupport.overviewParams(5, 0, "study_id", "asc"));

        assertEquals("CCDI Study", result.get(0).get("study_name"));
    }

    @Test
    void cohortManifest_collectsCohortParticipants() throws Exception {
        InventoryESService inventoryESService = mock(InventoryESService.class);
        stubOverviewQuery(inventoryESService);
        when(inventoryESService.collectPage(
                        any(Request.class), any(), any(String[][].class), anyInt(), anyInt()))
                .thenReturn(List.of(Map.of("participant_id", "p1", "race", "Asian")));

        PrivateESDataFetcher fetcher = PrivateESDataFetcherTestSupport.newFetcher(inventoryESService);

        @SuppressWarnings("unchecked")
        List<Map<String, Object>> result = (List<Map<String, Object>>) PrivateESDataFetcherTestSupport.invoke(
                fetcher,
                "cohortManifest",
                new Class[] {Map.class},
                PrivateESDataFetcherTestSupport.overviewParams(10, 0, "participant_id", "asc"));

        assertEquals("p1", result.get(0).get("participant_id"));
    }

    @Test
    void findParticipantIdsInList_collectsParticipantIds() throws Exception {
        InventoryESService inventoryESService = mock(InventoryESService.class);
        when(inventoryESService.buildListQuery(any(), anySet(), eq(false)))
                .thenReturn(Map.of("query", Map.of("match_all", Map.of())));
        when(inventoryESService.collectPage(
                        any(Request.class), any(), any(String[][].class), anyInt(), anyInt()))
                .thenReturn(List.of(
                        Map.of("participant_id", "p1", "study_id", "study-a"),
                        Map.of("participant_id", "p2", "study_id", "study-b")));

        PrivateESDataFetcher fetcher = PrivateESDataFetcherTestSupport.newFetcher(inventoryESService);

        @SuppressWarnings("unchecked")
        List<Map<String, Object>> result = (List<Map<String, Object>>) PrivateESDataFetcherTestSupport.invoke(
                fetcher, "findParticipantIdsInList", new Class[] {Map.class}, Map.of("id", List.of("p1", "p2")));

        assertEquals(2, result.size());
    }

    @Test
    void filesManifestInList_collectsFileManifestRows() throws Exception {
        InventoryESService inventoryESService = mock(InventoryESService.class);
        when(inventoryESService.buildListQuery(any(), anySet(), eq(false)))
                .thenReturn(PrivateESDataFetcherTestSupport.mutableQuery());
        when(inventoryESService.collectPage(
                        any(Request.class), any(), any(String[][].class), anyInt(), anyInt()))
                .thenReturn(List.of(Map.of(
                        "guid", "guid-1",
                        "file_name", "sample.bam",
                        "participant_id", "p1",
                        "md5sum", "abc123")));

        PrivateESDataFetcher fetcher = PrivateESDataFetcherTestSupport.newFetcher(inventoryESService);
        Map<String, Object> params = Map.of(
                "id", List.of("file-1"),
                "first", 10,
                "offset", 0);

        @SuppressWarnings("unchecked")
        List<Map<String, Object>> result = (List<Map<String, Object>>) PrivateESDataFetcherTestSupport.invoke(
                fetcher, "filesManifestInList", new Class[] {Map.class}, params);

        assertEquals("sample.bam", result.get(0).get("file_name"));
    }

    @Test
    void fileIDsFromList_participantIds_collectsFromOpenSearch() throws Exception {
        InventoryESService inventoryESService = mock(InventoryESService.class);
        when(inventoryESService.buildGetFileIDsQuery(List.of("p1")))
                .thenReturn(Map.of("query", Map.of("terms", Map.of("id", List.of("p1")))));
        when(inventoryESService.send(any(Request.class)))
                .thenReturn(PrivateESDataFetcherTestSupport.loadFixture("file_ids_hits.json"));
        when(inventoryESService.collectFileIDs(any(JsonObject.class)))
                .thenReturn(List.of("ccdi-int-f001", "ccdi-int-f002", "ccdi-int-f003"));

        PrivateESDataFetcher fetcher = PrivateESDataFetcherTestSupport.newFetcher(inventoryESService);
        Map<String, Object> params = PrivateESDataFetcherTestSupport.emptyIdListParams();
        params.put("participant_ids", List.of("p1"));

        @SuppressWarnings("unchecked")
        List<String> result = (List<String>) PrivateESDataFetcherTestSupport.invoke(
                fetcher, "fileIDsFromList", new Class[] {Map.class}, params);

        assertEquals(3, result.size());
    }

    @Test
    void fileIDsFromList_fileIds_returnsInputWhenProvided() throws Exception {
        InventoryESService inventoryESService = mock(InventoryESService.class);
        PrivateESDataFetcher fetcher = PrivateESDataFetcherTestSupport.newFetcher(inventoryESService);
        Map<String, Object> params = PrivateESDataFetcherTestSupport.emptyIdListParams();
        params.put("file_ids", List.of("f1", "f2"));

        @SuppressWarnings("unchecked")
        List<String> result = (List<String>) PrivateESDataFetcherTestSupport.invoke(
                fetcher, "fileIDsFromList", new Class[] {Map.class}, params);

        assertEquals(List.of("f1", "f2"), result);
        verify(inventoryESService, never()).send(any());
    }

    @Test
    void fileIDsFromList_noIds_returnsEmptyList() throws Exception {
        PrivateESDataFetcher fetcher = PrivateESDataFetcherTestSupport.newFetcher(mock(InventoryESService.class));

        @SuppressWarnings("unchecked")
        List<String> result = (List<String>) PrivateESDataFetcherTestSupport.invoke(
                fetcher,
                "fileIDsFromList",
                new Class[] {Map.class},
                PrivateESDataFetcherTestSupport.emptyIdListParams());

        assertTrue(result.isEmpty());
    }

    @Test
    void idsLists_cacheHit_returnsCachedParticipantIds() throws Exception {
        InventoryESService inventoryESService = mock(InventoryESService.class);
        PrivateESDataFetcher fetcher = PrivateESDataFetcherTestSupport.newFetcher(inventoryESService);
        Map<String, Object> cached = new HashMap<>();
        cached.put("participantIds", new String[] {"p1", "p2"});
        PrivateESDataFetcherTestSupport.putCacheEntry(fetcher, "participantIDs", cached);

        @SuppressWarnings("unchecked")
        Map<String, String[]> result = (Map<String, String[]>) PrivateESDataFetcherTestSupport.invoke(
                fetcher, "idsLists", new Class[] {});

        assertEquals(2, result.get("participantIds").length);
        verify(inventoryESService, never())
                .collectPage(any(Request.class), any(), any(String[][].class), anyInt(), anyInt());
    }

    @Test
    void idsLists_cacheMiss_collectsDistinctParticipantIds() throws Exception {
        InventoryESService inventoryESService = mock(InventoryESService.class);
        when(inventoryESService.buildListQuery()).thenReturn(PrivateESDataFetcherTestSupport.mutableQuery());
        when(inventoryESService.collectPage(
                        any(Request.class), any(), any(String[][].class), anyInt(), anyInt()))
                .thenReturn(List.of(
                        Map.of("participantIds", "p1"),
                        Map.of("participantIds", "p2"),
                        Map.of("participantIds", "p1")));

        PrivateESDataFetcher fetcher = PrivateESDataFetcherTestSupport.newFetcher(inventoryESService);

        @SuppressWarnings("unchecked")
        Map<String, String[]> result = (Map<String, String[]>) PrivateESDataFetcherTestSupport.invoke(
                fetcher, "idsLists", new Class[] {});

        assertEquals(2, result.get("participantIds").length);
        assertTrue(Set.of(result.get("participantIds")).containsAll(List.of("p1", "p2")));
    }
}
