package gov.nih.nci.bento_ri.model;

import com.github.benmanes.caffeine.cache.Cache;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import gov.nih.nci.bento_ri.model.FormattedCPIResponse.CPIDataItem;
import gov.nih.nci.bento_ri.service.InventoryESService;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;
import org.opensearch.client.Request;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.IntStream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anySet;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Branch-focused unit tests for low-coverage {@link PrivateESDataFetcher} resolvers.
 */
@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
class PrivateESDataFetcherBranchCoverageTest {

    private InventoryESService stubSearchParticipantsInventory(
            java.util.function.Function<String, JsonArray> bucketsForField) throws Exception {
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
        when(inventoryESService.addCustomAggregations(any(), anyString(), anyString(), anyString()))
                .thenAnswer(invocation -> invocation.getArgument(0));
        when(inventoryESService.collectTermAggs(any(JsonObject.class), any(String[].class)))
                .thenAnswer(invocation -> {
                    String field = ((String[]) invocation.getArgument(1))[0];
                    return Map.of(field, bucketsForField.apply(field));
                });
        when(inventoryESService.collectRangAggs(any(JsonObject.class), anyString()))
                .thenAnswer(invocation -> {
                    JsonObject ranges = new JsonObject();
                    ranges.addProperty("count", 10);
                    ranges.addProperty("min", 0);
                    ranges.addProperty("max", 18);
                    return Map.of(invocation.getArgument(1), ranges);
                });
        when(inventoryESService.collectRangCountAggs(any(JsonObject.class), anyString()))
                .thenAnswer(invocation -> Map.of("age_at_diagnosis", bucketsForField.apply("age_at_diagnosis")));
        when(inventoryESService.collectNodeCountAggs(any(JsonObject.class), eq("study_id")))
                .thenReturn(Map.of("study_id", PrivateESDataFetcherTestSupport.termBucket("phs001", 1)));
        return inventoryESService;
    }

    private JsonArray defaultParticipantBuckets(String field) {
        return PrivateESDataFetcherTestSupport.termBuckets("subject_count_term_aggs.json", "race");
    }

    private JsonArray diagnosisHighCountOnly(String field) {
        if ("diagnosis".equals(field)) {
            return PrivateESDataFetcherTestSupport.termBucket("Precursor cell lymphoblastic lymphoma, NOS", 2500);
        }
        return defaultParticipantBuckets(field);
    }

    @Test
    void searchParticipants_importData_skipsCacheLookup() throws Exception {
        InventoryESService inventoryESService = stubSearchParticipantsInventory(this::defaultParticipantBuckets);
        PrivateESDataFetcher fetcher = PrivateESDataFetcherTestSupport.newFetcher(inventoryESService);
        PrivateESDataFetcherTestSupport.putCacheEntry(
                fetcher, "all", Map.of("numberOfParticipants", 99));

        @SuppressWarnings("unchecked")
        Map<String, Object> result = (Map<String, Object>) PrivateESDataFetcherTestSupport.invoke(
                fetcher,
                "searchParticipants",
                new Class[] {Map.class},
                Map.of("import_data", List.of("CCDI"), "race", List.of("")));

        assertEquals(100, result.get("numberOfParticipants"));
        verify(inventoryESService, atLeastOnce()).send(any(Request.class));
    }

    @Test
    void searchParticipants_withRaceFilter_usesSubjectCountByForWidget() throws Exception {
        InventoryESService inventoryESService = stubSearchParticipantsInventory(this::defaultParticipantBuckets);
        PrivateESDataFetcher fetcher = PrivateESDataFetcherTestSupport.newFetcher(inventoryESService);

        @SuppressWarnings("unchecked")
        Map<String, Object> result = (Map<String, Object>) PrivateESDataFetcherTestSupport.invoke(
                fetcher,
                "searchParticipants",
                new Class[] {Map.class},
                Map.of("race", List.of("Asian")));

        assertNotNull(result.get("participantCountByRace"));
        assertNotNull(result.get("filterParticipantCountByRace"));
    }

    @Test
    void searchParticipants_rangeWidget_populatesDiagnosisAgeWidget() throws Exception {
        InventoryESService inventoryESService = stubSearchParticipantsInventory(this::defaultParticipantBuckets);
        PrivateESDataFetcher fetcher = PrivateESDataFetcherTestSupport.newFetcher(inventoryESService);

        @SuppressWarnings("unchecked")
        Map<String, Object> result = (Map<String, Object>) PrivateESDataFetcherTestSupport.invoke(
                fetcher,
                "searchParticipants",
                new Class[] {Map.class},
                Map.of("race", List.of("")));

        @SuppressWarnings("unchecked")
        List<Map<String, Object>> widget =
                (List<Map<String, Object>>) result.get("participantCountByDiagnosisAge");
        assertNotNull(widget);
        assertFalse(widget.isEmpty());
    }

    @Test
    void searchParticipants_additionalUpdate_refreshesDiagnosisCounts() throws Exception {
        InventoryESService inventoryESService = stubSearchParticipantsInventory(this::diagnosisHighCountOnly);
        when(inventoryESService.collectCustomTerms(any(JsonObject.class), eq("facetAgg")))
                .thenReturn(Map.of("Precursor cell lymphoblastic lymphoma, NOS", 2100));
        PrivateESDataFetcher fetcher = PrivateESDataFetcherTestSupport.newFetcher(inventoryESService);

        PrivateESDataFetcherTestSupport.invoke(
                fetcher, "searchParticipants", new Class[] {Map.class}, Map.of("race", List.of("")));

        verify(inventoryESService, atLeastOnce()).addCustomAggregations(any(), eq("facetAgg"), eq("diagnosis"), anyString());
        verify(inventoryESService, atLeastOnce()).collectCustomTerms(any(JsonObject.class), eq("facetAgg"));
    }

    @Test
    void searchParticipants_cacheMiss_storesResultInCache() throws Exception {
        InventoryESService inventoryESService = stubSearchParticipantsInventory(this::defaultParticipantBuckets);
        PrivateESDataFetcher fetcher = PrivateESDataFetcherTestSupport.newFetcher(inventoryESService);

        PrivateESDataFetcherTestSupport.invoke(
                fetcher, "searchParticipants", new Class[] {Map.class}, Map.of("race", List.of("")));

        Field cacheField = fetcher.getClass().getDeclaredField("caffeineCache");
        cacheField.setAccessible(true);
        @SuppressWarnings("unchecked")
        Cache<String, Object> cache = (Cache<String, Object>) cacheField.get(fetcher);
        assertTrue(cache.asMap().containsKey("all"));
    }

    @Test
    void getFilenames_noFilename_skipsWildcardInjection() throws Exception {
        InventoryESService inventoryESService = mock(InventoryESService.class);
        when(inventoryESService.buildFacetFilterQuery(
                        any(), anySet(), anySet(), anySet(), anyString(), eq("files")))
                .thenReturn(PrivateESDataFetcherTestSupport.filesQueryWithMustOnly());
        when(inventoryESService.send(any(Request.class)))
                .thenReturn(PrivateESDataFetcherTestSupport.filenamesTotalHits(2));
        when(inventoryESService.collectPage(
                        any(Request.class), any(), any(String[][].class), anyInt(), anyInt()))
                .thenReturn(List.of(Map.of("file_name", "a.bam")));

        PrivateESDataFetcher fetcher = PrivateESDataFetcherTestSupport.newFetcher(inventoryESService);
        Map<String, Object> params = Map.of(
                "first", 10,
                "offset", 0,
                "order_by", "file_name",
                "sort_direction", "asc");

        @SuppressWarnings("unchecked")
        Map<String, Object> result = (Map<String, Object>) PrivateESDataFetcherTestSupport.invoke(
                fetcher, "getFilenames", new Class[] {Map.class}, params);

        assertEquals(2, result.get("totalCount"));
        @SuppressWarnings("unchecked")
        List<Map<String, Object>> files = (List<Map<String, Object>>) result.get("files");
        assertEquals("a.bam", files.get(0).get("file_name"));
    }

    @Test
    void getFilenames_withMustOnlyQuery_addsWildcardToMustClause() throws Exception {
        InventoryESService inventoryESService = mock(InventoryESService.class);
        when(inventoryESService.buildFacetFilterQuery(
                        any(), anySet(), anySet(), anySet(), anyString(), eq("files")))
                .thenReturn(PrivateESDataFetcherTestSupport.filesQueryWithMustOnly());
        when(inventoryESService.send(any(Request.class)))
                .thenReturn(PrivateESDataFetcherTestSupport.filenamesTotalHits(1));
        when(inventoryESService.collectPage(
                        any(Request.class), any(), any(String[][].class), anyInt(), anyInt()))
                .thenReturn(List.of());

        PrivateESDataFetcher fetcher = PrivateESDataFetcherTestSupport.newFetcher(inventoryESService);

        PrivateESDataFetcherTestSupport.invoke(
                fetcher,
                "getFilenames",
                new Class[] {Map.class},
                Map.of("filename", "bam", "first", 5, "offset", 0));

        verify(inventoryESService)
                .collectPage(any(Request.class), org.mockito.ArgumentMatchers.argThat(query -> {
                    @SuppressWarnings("unchecked")
                    Map<String, Object> queryMap = (Map<String, Object>) query.get("query");
                    @SuppressWarnings("unchecked")
                    Map<String, Object> bool = (Map<String, Object>) queryMap.get("bool");
                    Object must = bool.get("must");
                    return must instanceof List && ((List<?>) must).size() >= 2;
                }), any(String[][].class), eq(5), eq(0));
    }

    @Test
    void getFilenames_withFacetFilters_addsWildcardToShouldFilterList() throws Exception {
        InventoryESService inventoryESService = mock(InventoryESService.class);
        when(inventoryESService.buildFacetFilterQuery(
                        any(), anySet(), anySet(), anySet(), anyString(), eq("files")))
                .thenReturn(PrivateESDataFetcherTestSupport.filesQueryWithShouldFilter());
        when(inventoryESService.send(any(Request.class)))
                .thenReturn(PrivateESDataFetcherTestSupport.filenamesTotalHits(4));
        when(inventoryESService.collectPage(
                        any(Request.class), any(), any(String[][].class), anyInt(), anyInt()))
                .thenReturn(List.of());

        PrivateESDataFetcher fetcher = PrivateESDataFetcherTestSupport.newFetcher(inventoryESService);

        PrivateESDataFetcherTestSupport.invoke(
                fetcher,
                "getFilenames",
                new Class[] {Map.class},
                Map.of("filename", "vcf", "race", List.of("Asian"), "first", 10, "offset", 0));

        verify(inventoryESService)
                .collectPage(any(Request.class), org.mockito.ArgumentMatchers.argThat(query -> {
                    @SuppressWarnings("unchecked")
                    Map<String, Object> queryMap = (Map<String, Object>) query.get("query");
                    @SuppressWarnings("unchecked")
                    Map<String, Object> bool = (Map<String, Object>) queryMap.get("bool");
                    @SuppressWarnings("unchecked")
                    List<Object> should = (List<Object>) bool.get("should");
                    @SuppressWarnings("unchecked")
                    Map<String, Object> innerBool = (Map<String, Object>) ((Map<String, Object>) should.get(0)).get("bool");
                    @SuppressWarnings("unchecked")
                    List<Object> filters = (List<Object>) innerBool.get("filter");
                    return filters != null && !filters.isEmpty();
                }), any(String[][].class), anyInt(), anyInt());
    }

    @Test
    void getFilenames_nullTotalHits_defaultsTotalCountToZero() throws Exception {
        InventoryESService inventoryESService = mock(InventoryESService.class);
        when(inventoryESService.buildFacetFilterQuery(
                        any(), anySet(), anySet(), anySet(), anyString(), anyString()))
                .thenReturn(PrivateESDataFetcherTestSupport.mutableQuery());
        when(inventoryESService.send(any(Request.class))).thenReturn(new JsonObject());
        when(inventoryESService.collectPage(
                        any(Request.class), any(), any(String[][].class), anyInt(), anyInt()))
                .thenReturn(List.of());

        PrivateESDataFetcher fetcher = PrivateESDataFetcherTestSupport.newFetcher(inventoryESService);

        @SuppressWarnings("unchecked")
        Map<String, Object> result = (Map<String, Object>) PrivateESDataFetcherTestSupport.invoke(
                fetcher, "getFilenames", new Class[] {Map.class}, Map.of("first", 10, "offset", 0));

        assertEquals(0, result.get("totalCount"));
    }

    @Test
    void studyDetails_unknownStudy_returnsEmptySupportingData() throws Exception {
        InventoryESService inventoryESService = mock(InventoryESService.class);
        when(inventoryESService.buildFacetFilterQuery(
                        any(), anySet(), anySet(), anySet(), anyString(), anyString()))
                .thenReturn(PrivateESDataFetcherTestSupport.mutableQuery());
        when(inventoryESService.collectPage(
                        any(Request.class), any(), any(String[][].class), anyInt(), anyInt()))
                .thenReturn(List.of(new HashMap<>(Map.of("study_id", "phs999999", "study_name", "Unknown"))));
        when(inventoryESService.addAggregations(any(), any(String[].class), any(), anyList()))
                .thenAnswer(invocation -> invocation.getArgument(0));
        when(inventoryESService.send(any(Request.class)))
                .thenReturn(PrivateESDataFetcherTestSupport.loadFixture("subject_count_term_aggs.json"));
        when(inventoryESService.collectTermAggs(any(JsonObject.class), any(String[].class)))
                .thenAnswer(invocation -> {
                    String field = ((String[]) invocation.getArgument(1))[0];
                    return Map.of(field, PrivateESDataFetcherTestSupport.termBuckets("subject_count_term_aggs.json", "race"));
                });

        PrivateESDataFetcher fetcher = PrivateESDataFetcherTestSupport.newFetcher(inventoryESService);

        @SuppressWarnings("unchecked")
        Map<String, Object> result = (Map<String, Object>) PrivateESDataFetcherTestSupport.invoke(
                fetcher, "studyDetails", new Class[] {Map.class}, Map.of("study_id", "phs999999"));

        @SuppressWarnings("unchecked")
        List<Map<String, Object>> supportingData = (List<Map<String, Object>>) result.get("supporting_data");
        assertTrue(supportingData.isEmpty());
    }

    @Test
    void studyDetails_dataCategoryAboveThreshold_triggersCustomAggregationRefresh() throws Exception {
        InventoryESService inventoryESService = mock(InventoryESService.class);
        when(inventoryESService.buildFacetFilterQuery(
                        any(), anySet(), anySet(), anySet(), anyString(), anyString()))
                .thenReturn(PrivateESDataFetcherTestSupport.mutableQuery());
        when(inventoryESService.collectPage(
                        any(Request.class), any(), any(String[][].class), anyInt(), anyInt()))
                .thenReturn(List.of(new HashMap<>(Map.of("study_id", "phs002790", "study_name", "MCI"))));
        when(inventoryESService.addAggregations(any(), any(String[].class), any(), anyList()))
                .thenAnswer(invocation -> invocation.getArgument(0));
        when(inventoryESService.addCustomAggregations(any(), anyString(), anyString(), anyString()))
                .thenAnswer(invocation -> invocation.getArgument(0));
        when(inventoryESService.collectCustomTerms(any(JsonObject.class), eq("facetAgg")))
                .thenReturn(Map.of("Clinical", 1200));
        when(inventoryESService.send(any(Request.class)))
                .thenReturn(PrivateESDataFetcherTestSupport.loadFixture("subject_count_term_aggs.json"));
        when(inventoryESService.collectTermAggs(any(JsonObject.class), any(String[].class)))
                .thenAnswer(invocation -> {
                    String field = ((String[]) invocation.getArgument(1))[0];
                    if ("data_category".equals(field)) {
                        return Map.of(field, PrivateESDataFetcherTestSupport.termBucket("Clinical", 1600));
                    }
                    return Map.of(field, PrivateESDataFetcherTestSupport.termBuckets("subject_count_term_aggs.json", "race"));
                });

        PrivateESDataFetcher fetcher = PrivateESDataFetcherTestSupport.newFetcher(inventoryESService);

        @SuppressWarnings("unchecked")
        Map<String, Object> result = (Map<String, Object>) PrivateESDataFetcherTestSupport.invoke(
                fetcher, "studyDetails", new Class[] {Map.class}, Map.of("study_id", "phs002790"));

        verify(inventoryESService).addCustomAggregations(any(), eq("facetAgg"), eq("data_category"), anyString());
        @SuppressWarnings("unchecked")
        List<Map<String, Object>> categories = (List<Map<String, Object>>) result.get("data_categories");
        assertEquals(1200, categories.stream()
                .filter(entry -> "Clinical".equals(entry.get("group")))
                .findFirst()
                .orElseThrow()
                .get("subjects"));
    }

    @Test
    void cohortCharts_noCohortKeys_returnsEmptyList() throws Exception {
        PrivateESDataFetcher fetcher = PrivateESDataFetcherTestSupport.newFetcher(mock(InventoryESService.class));

        @SuppressWarnings("unchecked")
        List<Map<String, Object>> result = (List<Map<String, Object>>) PrivateESDataFetcherTestSupport.invoke(
                fetcher,
                "cohortCharts",
                new Class[] {Map.class},
                Map.of("charts", List.of(Map.of("property", "race", "type", "count"))));

        assertTrue(result.isEmpty());
    }

    @Test
    void cohortCharts_emptyCohortLists_returnsEmptyList() throws Exception {
        PrivateESDataFetcher fetcher = PrivateESDataFetcherTestSupport.newFetcher(mock(InventoryESService.class));
        Map<String, Object> params = Map.of(
                "c1", List.of(),
                "charts", List.of(Map.of("property", "race", "type", "count")));

        @SuppressWarnings("unchecked")
        List<Map<String, Object>> result = (List<Map<String, Object>>) PrivateESDataFetcherTestSupport.invoke(
                fetcher, "cohortCharts", new Class[] {Map.class}, params);

        assertTrue(result.isEmpty());
    }

    @Test
    void cohortCharts_unknownProperty_skipsChartEntry() throws Exception {
        PrivateESDataFetcher fetcher = PrivateESDataFetcherTestSupport.newFetcher(mock(InventoryESService.class));
        Map<String, Object> params = Map.of(
                "c1", List.of("id-1"),
                "charts", List.of(Map.of("property", "not_a_real_field", "type", "count")));

        @SuppressWarnings("unchecked")
        List<Map<String, Object>> result = (List<Map<String, Object>>) PrivateESDataFetcherTestSupport.invoke(
                fetcher, "cohortCharts", new Class[] {Map.class}, params);

        assertTrue(result.isEmpty());
    }

    @Test
    void cohortCharts_percentageType_computesPercentages() throws Exception {
        InventoryESService inventoryESService = mock(InventoryESService.class);
        when(inventoryESService.buildFacetFilterQuery(any(), anySet(), anySet(), anySet(), anyString(), anyString()))
                .thenReturn(PrivateESDataFetcherTestSupport.mutableQuery());
        when(inventoryESService.getBucketNames(
                        anyString(), any(), anySet(), any(), anyString(), anyString()))
                .thenReturn(List.of("Asian", "White"));
        when(inventoryESService.getCount(any(), eq("participants"))).thenReturn(100);
        when(inventoryESService.addAggregations(any(), any(String[].class), any(), anyList()))
                .thenAnswer(invocation -> invocation.getArgument(0));
        when(inventoryESService.send(any(Request.class)))
                .thenReturn(PrivateESDataFetcherTestSupport.loadFixture("subject_count_term_aggs.json"));
        when(inventoryESService.collectTermAggs(any(JsonObject.class), any(String[].class)))
                .thenAnswer(invocation -> Map.of("race", PrivateESDataFetcherTestSupport.termBucket("Asian", 25)));

        PrivateESDataFetcher fetcher = PrivateESDataFetcherTestSupport.newFetcher(inventoryESService);
        Map<String, Object> params = Map.of(
                "c1", List.of("id-1"),
                "charts", List.of(Map.of("property", "race", "type", "percentage")));

        @SuppressWarnings("unchecked")
        List<Map<String, Object>> result = (List<Map<String, Object>>) PrivateESDataFetcherTestSupport.invoke(
                fetcher, "cohortCharts", new Class[] {Map.class}, params);

        @SuppressWarnings("unchecked")
        List<Map<String, Object>> cohorts = (List<Map<String, Object>>) result.get(0).get("cohorts");
        @SuppressWarnings("unchecked")
        List<Map<String, Object>> groups = (List<Map<String, Object>>) cohorts.get(0).get("participantsByGroup");
        double asianPct = groups.stream()
                .filter(g -> "Asian".equals(g.get("group")))
                .mapToDouble(g -> (Double) g.get("subjects"))
                .findFirst()
                .orElseThrow();
        assertEquals(25.0, asianPct, 0.01);
    }

    @Test
    void cohortCharts_manyBuckets_addsOtherFewAndOtherMany() throws Exception {
        InventoryESService inventoryESService = mock(InventoryESService.class);
        when(inventoryESService.buildFacetFilterQuery(any(), anySet(), anySet(), anySet(), anyString(), anyString()))
                .thenReturn(PrivateESDataFetcherTestSupport.mutableQuery());
        List<String> manyBuckets =
                IntStream.rangeClosed(1, 25).mapToObj(i -> "bucket-" + i).toList();
        when(inventoryESService.getBucketNames(
                        anyString(), any(), anySet(), any(), anyString(), anyString()))
                .thenReturn(manyBuckets);
        when(inventoryESService.addAggregations(any(), any(String[].class), any(), anyList()))
                .thenAnswer(invocation -> invocation.getArgument(0));
        when(inventoryESService.send(any(Request.class)))
                .thenReturn(PrivateESDataFetcherTestSupport.loadFixture("subject_count_term_aggs.json"));
        when(inventoryESService.collectTermAggs(any(JsonObject.class), any(String[].class)))
                .thenAnswer(invocation -> {
                    JsonArray buckets = new JsonArray();
                    for (String bucket : manyBuckets) {
                        buckets.addAll(PrivateESDataFetcherTestSupport.termBucket(bucket, 2));
                    }
                    return Map.of("race", buckets);
                });

        PrivateESDataFetcher fetcher = PrivateESDataFetcherTestSupport.newFetcher(inventoryESService);
        Map<String, Object> params = Map.of(
                "c1", List.of("id-1"),
                "c2", List.of("id-2"),
                "charts", List.of(Map.of("property", "race", "type", "count")));

        @SuppressWarnings("unchecked")
        List<Map<String, Object>> result = (List<Map<String, Object>>) PrivateESDataFetcherTestSupport.invoke(
                fetcher, "cohortCharts", new Class[] {Map.class}, params);

        @SuppressWarnings("unchecked")
        List<Map<String, Object>> cohorts = (List<Map<String, Object>>) result.get(0).get("cohorts");
        assertEquals(2, cohorts.size());
        @SuppressWarnings("unchecked")
        List<Map<String, Object>> groups = (List<Map<String, Object>>) cohorts.get(0).get("participantsByGroup");
        assertTrue(groups.stream().anyMatch(g -> "OtherFew".equals(g.get("group"))));
        assertTrue(groups.stream().anyMatch(g -> "OtherMany".equals(g.get("group"))));
    }

    @Test
    void fileIDsFromList_diagnosisIds_queriesDiagnosisEndpoint() throws Exception {
        InventoryESService inventoryESService = mock(InventoryESService.class);
        when(inventoryESService.buildGetFileIDsQuery(List.of("d1")))
                .thenReturn(Map.of("query", Map.of("terms", Map.of("id", List.of("d1")))));
        when(inventoryESService.send(any(Request.class)))
                .thenReturn(PrivateESDataFetcherTestSupport.loadFixture("file_ids_hits.json"));
        when(inventoryESService.collectFileIDs(any(JsonObject.class))).thenReturn(List.of("file-d1"));

        PrivateESDataFetcher fetcher = PrivateESDataFetcherTestSupport.newFetcher(inventoryESService);
        Map<String, Object> params = PrivateESDataFetcherTestSupport.emptyIdListParams();
        params.put("diagnosis_ids", List.of("d1"));

        @SuppressWarnings("unchecked")
        List<String> result = (List<String>) PrivateESDataFetcherTestSupport.invoke(
                fetcher, "fileIDsFromList", new Class[] {Map.class}, params);

        assertEquals(List.of("file-d1"), result);
        verify(inventoryESService).send(org.mockito.ArgumentMatchers.argThat(
                (Request request) -> request.getEndpoint().contains("diagnosis")));
    }

    @Test
    void fileIDsFromList_studyIds_queriesStudiesEndpoint() throws Exception {
        InventoryESService inventoryESService = mock(InventoryESService.class);
        when(inventoryESService.buildGetFileIDsQuery(List.of("st1")))
                .thenReturn(Map.of("query", Map.of("terms", Map.of("id", List.of("st1")))));
        when(inventoryESService.send(any(Request.class)))
                .thenReturn(PrivateESDataFetcherTestSupport.loadFixture("file_ids_hits.json"));
        when(inventoryESService.collectFileIDs(any(JsonObject.class))).thenReturn(List.of("file-st1"));

        PrivateESDataFetcher fetcher = PrivateESDataFetcherTestSupport.newFetcher(inventoryESService);
        Map<String, Object> params = PrivateESDataFetcherTestSupport.emptyIdListParams();
        params.put("study_ids", List.of("st1"));

        @SuppressWarnings("unchecked")
        List<String> result = (List<String>) PrivateESDataFetcherTestSupport.invoke(
                fetcher, "fileIDsFromList", new Class[] {Map.class}, params);

        assertEquals(List.of("file-st1"), result);
    }

    @Test
    void fileIDsFromList_sampleIds_queriesSamplesEndpoint() throws Exception {
        InventoryESService inventoryESService = mock(InventoryESService.class);
        when(inventoryESService.buildGetFileIDsQuery(List.of("s1")))
                .thenReturn(Map.of("query", Map.of("terms", Map.of("id", List.of("s1")))));
        when(inventoryESService.send(any(Request.class)))
                .thenReturn(PrivateESDataFetcherTestSupport.loadFixture("file_ids_hits.json"));
        when(inventoryESService.collectFileIDs(any(JsonObject.class))).thenReturn(List.of("file-s1"));

        PrivateESDataFetcher fetcher = PrivateESDataFetcherTestSupport.newFetcher(inventoryESService);
        Map<String, Object> params = PrivateESDataFetcherTestSupport.emptyIdListParams();
        params.put("sample_ids", List.of("s1"));

        @SuppressWarnings("unchecked")
        List<String> result = (List<String>) PrivateESDataFetcherTestSupport.invoke(
                fetcher, "fileIDsFromList", new Class[] {Map.class}, params);

        assertEquals(List.of("file-s1"), result);
    }

    @Test
    void numberOfStudies_returnsDistinctStudyCount() throws Exception {
        InventoryESService inventoryESService = mock(InventoryESService.class);
        when(inventoryESService.buildFacetFilterQuery(
                        any(), anySet(), anySet(), anySet(), any(), eq("files_overall")))
                .thenReturn(PrivateESDataFetcherTestSupport.mutableQuery());
        when(inventoryESService.addNodeCountAggregations(any(), eq("study_id")))
                .thenAnswer(invocation -> invocation.getArgument(0));
        when(inventoryESService.send(any(Request.class)))
                .thenReturn(PrivateESDataFetcherTestSupport.loadFixture("node_count_aggs.json"));
        when(inventoryESService.collectNodeCountAggs(any(JsonObject.class), eq("study_id")))
                .thenReturn(Map.of(
                        "study_id",
                        PrivateESDataFetcherTestSupport.termBuckets("node_count_aggs.json", "race")));

        PrivateESDataFetcher fetcher = PrivateESDataFetcherTestSupport.newFetcher(inventoryESService);

        Integer count = (Integer) PrivateESDataFetcherTestSupport.invoke(
                fetcher, "numberOfStudies", new Class[] {Map.class}, Map.of("race", List.of("")));

        assertEquals(2, count);
    }

    @Test
    void getPropertyConfig_coversDiagnosisSurvivalAndSampleProperties() throws Exception {
        PrivateESDataFetcher fetcher = PrivateESDataFetcherTestSupport.newFetcher(null);

        @SuppressWarnings("unchecked")
        Map<String, String> diagnosis = (Map<String, String>) PrivateESDataFetcherTestSupport.invoke(
                fetcher, "getPropertyConfig", new Class[] {String.class}, "diagnosis");
        @SuppressWarnings("unchecked")
        Map<String, String> survival = (Map<String, String>) PrivateESDataFetcherTestSupport.invoke(
                fetcher, "getPropertyConfig", new Class[] {String.class}, "last_known_survival_status");
        @SuppressWarnings("unchecked")
        Map<String, String> sample = (Map<String, String>) PrivateESDataFetcherTestSupport.invoke(
                fetcher, "getPropertyConfig", new Class[] {String.class}, "sample_tumor_status");
        @SuppressWarnings("unchecked")
        Map<String, String> study = (Map<String, String>) PrivateESDataFetcherTestSupport.invoke(
                fetcher, "getPropertyConfig", new Class[] {String.class}, "dbgap_accession");

        assertEquals("diagnosis", diagnosis.get("index"));
        assertEquals("pid", diagnosis.get("cardinalityAggName"));
        assertEquals("survivals", survival.get("index"));
        assertEquals("samples", sample.get("index"));
        assertEquals("", study.get("cardinalityAggName"));
    }

    @Test
    void cohortMetadata_sortsNullSurvivalAgesAndGroupsMultipleAccessions() throws Exception {
        InventoryESService inventoryESService = mock(InventoryESService.class);
        when(inventoryESService.buildFacetFilterQuery(
                        any(), anySet(), anySet(), anySet(), anyString(), anyString()))
                .thenReturn(PrivateESDataFetcherTestSupport.mutableQuery());
        List<Map<String, Object>> survivalsWithNull = new ArrayList<>();
        Map<String, Object> nullAge = new HashMap<>();
        nullAge.put("age_at_last_known_survival_status", null);
        survivalsWithNull.add(nullAge);
        survivalsWithNull.add(Map.of("age_at_last_known_survival_status", 10));
        when(inventoryESService.collectPage(
                        any(Request.class), any(), any(String[][].class), anyInt(), anyInt()))
                .thenReturn(List.of(
                        Map.of(
                                "participant_id", "p1",
                                "dbgap_accession", "phs001",
                                "survivals", survivalsWithNull),
                        Map.of("participant_id", "p2", "dbgap_accession", "phs002")));

        PrivateESDataFetcher fetcher = PrivateESDataFetcherTestSupport.newFetcher(inventoryESService);

        @SuppressWarnings("unchecked")
        List<Map<String, Object>> result = (List<Map<String, Object>>) PrivateESDataFetcherTestSupport.invoke(
                fetcher,
                "cohortMetadata",
                new Class[] {Map.class},
                PrivateESDataFetcherTestSupport.overviewParams(10, 0, "participant_id", "asc"));

        assertEquals(2, result.size());
        Map<String, Object> phs001Group = result.stream()
                .filter(entry -> "phs001".equals(entry.get("dbgap_accession")))
                .findFirst()
                .orElseThrow();
        @SuppressWarnings("unchecked")
        List<Map<String, Object>> sortedSurvivals = (List<Map<String, Object>>)
                ((List<Map<String, Object>>) phs001Group.get("participants")).get(0).get("survivals");
        assertEquals(10, sortedSurvivals.get(0).get("age_at_last_known_survival_status"));
    }

    @Test
    void hasCpiData_returnsFalseForEmptyCpiList() throws Exception {
        PrivateESDataFetcher fetcher = PrivateESDataFetcherTestSupport.newFetcher(null);
        FormattedCPIResponse empty = new FormattedCPIResponse("p1", "study", List.of());

        Boolean hasData = (Boolean) PrivateESDataFetcherTestSupport.invoke(
                fetcher, "hasCpiData", new Class[] {FormattedCPIResponse.class}, empty);

        assertFalse(hasData);
    }

    @Test
    void enrichCPIDataWithParticipantInfo_skipsWhenNoCpiRecords() throws Exception {
        InventoryESService inventoryESService = mock(InventoryESService.class);
        PrivateESDataFetcher fetcher = PrivateESDataFetcherTestSupport.newFetcher(inventoryESService);
        FormattedCPIResponse empty = new FormattedCPIResponse("p1", "study", List.of());

        PrivateESDataFetcherTestSupport.invoke(
                fetcher, "enrichCPIDataWithParticipantInfo", new Class[] {List.class}, List.of(empty));

        verify(inventoryESService, never()).send(any());
    }

    @Test
    void convertToMap_convertsCpiDataItemViaGson() throws Exception {
        PrivateESDataFetcher fetcher = PrivateESDataFetcherTestSupport.newFetcher(null);
        CPIDataItem item = new CPIDataItem("assoc-1", "STUDY-1", "desc", "Research", "s3://bucket");

        @SuppressWarnings("unchecked")
        Map<String, Object> map = (Map<String, Object>) PrivateESDataFetcherTestSupport.invoke(
                fetcher, "convertToMap", new Class[] {Object.class}, item);

        assertEquals("assoc-1", map.get("associated_id"));
        assertEquals("STUDY-1", map.get("repository_of_synonym_id"));
    }

    @Test
    void executeBatchQuery_emptyMap_returnsEmptyList() throws Exception {
        PrivateESDataFetcher fetcher = PrivateESDataFetcherTestSupport.newFetcher(mock(InventoryESService.class));

        @SuppressWarnings("unchecked")
        List<Map<String, Object>> result = (List<Map<String, Object>>) PrivateESDataFetcherTestSupport.invoke(
                fetcher, "executeBatchQuery", new Class[] {Map.class}, Map.of());

        assertTrue(result.isEmpty());
    }

    @Test
    void updateParticipantListWithEnrichedCPIData_noOpsOnEmptyInputs() throws Exception {
        PrivateESDataFetcher fetcher = PrivateESDataFetcherTestSupport.newFetcher(null);

        PrivateESDataFetcherTestSupport.invoke(
                fetcher,
                "updateParticipantListWithEnrichedCPIData",
                new Class[] {List.class, List.class},
                List.of(),
                List.of());
    }
}
