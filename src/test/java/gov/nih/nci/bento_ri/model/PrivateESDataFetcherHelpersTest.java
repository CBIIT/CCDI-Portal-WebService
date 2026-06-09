package gov.nih.nci.bento_ri.model;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import gov.nih.nci.bento_ri.model.FormattedCPIResponse.CPIDataItem;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Phase 4: unit tests for pure helper methods on {@link PrivateESDataFetcher}.
 */
class PrivateESDataFetcherHelpersTest {

    @Test
    void paginate_returnsSliceWithinBounds() throws Exception {
        PrivateESDataFetcher fetcher = PrivateESDataFetcherTestSupport.newFetcher(null);
        List<String> items = List.of("a", "b", "c", "d", "e");

        @SuppressWarnings("unchecked")
        List<Object> page = (List<Object>) PrivateESDataFetcherTestSupport.invoke(
                fetcher, "paginate", new Class[] {List.class, int.class, int.class}, items, 2, 1);

        assertEquals(List.of("b", "c"), page);
    }

    @Test
    void paginate_offsetBeyondSize_returnsEmpty() throws Exception {
        PrivateESDataFetcher fetcher = PrivateESDataFetcherTestSupport.newFetcher(null);
        List<String> items = List.of("a", "b");

        @SuppressWarnings("unchecked")
        List<Object> page = (List<Object>) PrivateESDataFetcherTestSupport.invoke(
                fetcher, "paginate", new Class[] {List.class, int.class, int.class}, items, 5, 5);

        assertTrue(page.isEmpty());
    }

    @Test
    void extractIDs_mapsParticipantAndStudyFields() throws Exception {
        PrivateESDataFetcher fetcher = PrivateESDataFetcherTestSupport.newFetcher(null);
        List<Map<String, Object>> participants = List.of(
                Map.of("participant_id", "p1", "study_id", "study-a"),
                Map.of("participant_id", "p2", "study_id", "study-b"));

        @SuppressWarnings("unchecked")
        List<ParticipantRequest> ids = (List<ParticipantRequest>) PrivateESDataFetcherTestSupport.invoke(
                fetcher, "extractIDs", new Class[] {List.class}, participants);

        assertEquals(2, ids.size());
        assertEquals("p1", ids.get(0).getParticipantId());
        assertEquals("study-a", ids.get(0).getStudyId());
    }

    @Test
    void extractStringValue_handlesScalarAndListValues() throws Exception {
        PrivateESDataFetcher fetcher = PrivateESDataFetcherTestSupport.newFetcher(null);
        Map<String, Object> map = new HashMap<>();
        map.put("scalar", "value");
        map.put("list", List.of("first", "second"));
        map.put("empty", null);

        assertEquals(
                "value",
                PrivateESDataFetcherTestSupport.invoke(
                        fetcher, "extractStringValue", new Class[] {Map.class, String.class}, map, "scalar"));
        assertEquals(
                "first",
                PrivateESDataFetcherTestSupport.invoke(
                        fetcher, "extractStringValue", new Class[] {Map.class, String.class}, map, "list"));
        assertNull(PrivateESDataFetcherTestSupport.invoke(
                fetcher, "extractStringValue", new Class[] {Map.class, String.class}, map, "empty"));
    }

    @Test
    void convertToMap_roundTripsCpiDataItemViaGson() throws Exception {
        PrivateESDataFetcher fetcher = PrivateESDataFetcherTestSupport.newFetcher(null);
        CPIDataItem item = new CPIDataItem("assoc-1", "CCDI-STUDY", "desc", "Research", "s3://bucket");

        @SuppressWarnings("unchecked")
        Map<String, Object> map = (Map<String, Object>) PrivateESDataFetcherTestSupport.invoke(
                fetcher, "convertToMap", new Class[] {Object.class}, item);

        assertEquals("assoc-1", map.get("associated_id"));
        assertEquals("CCDI-STUDY", map.get("repository_of_synonym_id"));
    }

    @Test
    void buildStudyToParticipantsMap_groupsByRepository() throws Exception {
        PrivateESDataFetcher fetcher = PrivateESDataFetcherTestSupport.newFetcher(null);
        List<FormattedCPIResponse> records = List.of(new FormattedCPIResponse(
                "p1",
                "CCDI-STUDY",
                List.of(
                        new CPIDataItem("assoc-1", "CCDI-STUDY", "d1", "c1", "loc1"),
                        new CPIDataItem("assoc-2", "OTHER-STUDY", "d2", "c2", "loc2"))));

        @SuppressWarnings("unchecked")
        Map<String, Set<String>> grouped = (Map<String, Set<String>>) PrivateESDataFetcherTestSupport.invoke(
                fetcher, "buildStudyToParticipantsMap", new Class[] {List.class}, records);

        assertEquals(Set.of("assoc-1"), grouped.get("CCDI-STUDY"));
        assertEquals(Set.of("assoc-2"), grouped.get("OTHER-STUDY"));
    }

    @Test
    void mapSortOrder_mapsKnownFieldAndDefaultsInvalidDirection() throws Exception {
        PrivateESDataFetcher fetcher = PrivateESDataFetcherTestSupport.newFetcher(null);
        Map<String, String> mapping = Map.of("participant_id", "participant_id.keyword");

        @SuppressWarnings("unchecked")
        Map<String, String> sort = (Map<String, String>) PrivateESDataFetcherTestSupport.invoke(
                fetcher,
                "mapSortOrder",
                new Class[] {String.class, String.class, String.class, Map.class},
                "participant_id",
                "desc",
                "participant_id",
                mapping);

        assertEquals(Map.of("participant_id.keyword", "desc"), sort);

        @SuppressWarnings("unchecked")
        Map<String, String> defaultSort = (Map<String, String>) PrivateESDataFetcherTestSupport.invoke(
                fetcher,
                "mapSortOrder",
                new Class[] {String.class, String.class, String.class, Map.class},
                "unknown_field",
                "sideways",
                "participant_id",
                mapping);

        assertEquals(Map.of("participant_id", "asc"), defaultSort);
    }

    @Test
    void getGroupCountHelper_usesCardinalityWhenPresent() throws Exception {
        PrivateESDataFetcher fetcher = PrivateESDataFetcherTestSupport.newFetcher(null);
        JsonArray buckets = PrivateESDataFetcherTestSupport.termBuckets("subject_count_term_aggs.json", "race");

        @SuppressWarnings("unchecked")
        List<Map<String, Object>> groups = (List<Map<String, Object>>) PrivateESDataFetcherTestSupport.invoke(
                fetcher, "getGroupCountHelper", new Class[] {JsonArray.class, String.class}, buckets, "pid");

        assertEquals(2, groups.size());
        assertEquals("Asian", groups.get(0).get("group"));
        assertEquals(8, groups.get(0).get("subjects"));
    }

    @Test
    void getRangeGroupCountHelper_zeroCount_returnsZeroBounds() throws Exception {
        PrivateESDataFetcher fetcher = PrivateESDataFetcherTestSupport.newFetcher(null);
        JsonObject ranges = new JsonObject();
        ranges.addProperty("count", 0);
        ranges.addProperty("min", 0);
        ranges.addProperty("max", 0);

        @SuppressWarnings("unchecked")
        List<Map<String, Object>> groups = (List<Map<String, Object>>) PrivateESDataFetcherTestSupport.invoke(
                fetcher, "getRangeGroupCountHelper", new Class[] {JsonObject.class}, ranges);

        assertEquals(1, groups.size());
        assertEquals(0, groups.get(0).get("lowerBound"));
        assertEquals(0, groups.get(0).get("upperBound"));
        assertEquals(0, groups.get(0).get("subjects"));
    }

    @Test
    void getRangeGroupCountHelper_withValues_returnsMinMaxAndCount() throws Exception {
        PrivateESDataFetcher fetcher = PrivateESDataFetcherTestSupport.newFetcher(null);
        JsonObject ranges = new JsonObject();
        ranges.addProperty("count", 25);
        ranges.addProperty("min", 5);
        ranges.addProperty("max", 65);

        @SuppressWarnings("unchecked")
        List<Map<String, Object>> groups = (List<Map<String, Object>>) PrivateESDataFetcherTestSupport.invoke(
                fetcher, "getRangeGroupCountHelper", new Class[] {JsonObject.class}, ranges);

        assertEquals(5, groups.get(0).get("lowerBound"));
        assertEquals(65, groups.get(0).get("upperBound"));
        assertEquals(25, groups.get(0).get("subjects"));
    }

    @Test
    void generateCacheKey_emptyFilters_returnsAllKey() throws Exception {
        PrivateESDataFetcher fetcher = PrivateESDataFetcherTestSupport.newFetcher(null);
        Map<String, Object> params = Map.of("race", List.of(""));

        String cacheKey = (String) PrivateESDataFetcherTestSupport.invoke(
                fetcher, "generateCacheKey", new Class[] {Map.class}, params);

        assertEquals("all", cacheKey);
    }

    @Test
    void generateCacheKey_includesFacetAndRangeValues() throws Exception {
        PrivateESDataFetcher fetcher = PrivateESDataFetcherTestSupport.newFetcher(null);
        Map<String, Object> params = Map.of(
                "race", List.of("Asian"),
                "age_at_diagnosis", List.of(0, 18));

        String cacheKey = (String) PrivateESDataFetcherTestSupport.invoke(
                fetcher, "generateCacheKey", new Class[] {Map.class}, params);

        assertTrue(cacheKey.contains("race"));
        assertTrue(cacheKey.contains("Asian"));
        assertTrue(cacheKey.contains("age_at_diagnosis"));
        assertTrue(cacheKey.contains("0"));
        assertTrue(cacheKey.contains("18"));
    }

    @Test
    void buildBatchQuery_buildsShouldClausesPerStudy() throws Exception {
        PrivateESDataFetcher fetcher = PrivateESDataFetcherTestSupport.newFetcher(null);
        Map<String, Set<String>> studyToParticipants = Map.of(
                "CCDI-STUDY", Set.of("p1", "p2"),
                "OTHER-STUDY", Set.of("p3"));

        @SuppressWarnings("unchecked")
        Map<String, Object> query = (Map<String, Object>) PrivateESDataFetcherTestSupport.invoke(
                fetcher, "buildBatchQuery", new Class[] {Map.class}, studyToParticipants);

        @SuppressWarnings("unchecked")
        Map<String, Object> bool = (Map<String, Object>) ((Map<String, Object>) query.get("query")).get("bool");
        @SuppressWarnings("unchecked")
        List<Object> shouldClauses = (List<Object>) bool.get("should");

        assertEquals(2, shouldClauses.size());
        assertEquals(10000, query.get("size"));
    }
}
