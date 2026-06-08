package gov.nih.nci.bento_ri.service;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Unit tests for {@link InventoryESService} aggregation and file-ID query builders.
 */
class InventoryESServiceAggregationBuildersTest {

    private static final Map<String, Object> BASE_QUERY = Map.of(
            "query", Map.of("match_all", Map.of()));

    private InventoryESService service;

    @BeforeEach
    void setUp() throws Exception {
        service = InventoryESServiceTestSupport.newService();
    }

    @AfterEach
    void tearDown() throws Exception {
        InventoryESServiceTestSupport.closeClient(service);
    }

    @Test
    void buildGetFileIDsQuery_setsSourceTermsSizeAndFrom() throws IOException {
        List<String> ids = List.of("id-1", "id-2");
        Map<String, Object> query = service.buildGetFileIDsQuery(ids);

        assertEquals(Set.of("id", "files"), query.get("_source"));
        assertEquals(Map.of("terms", Map.of("id", ids)), query.get("query"));
        assertEquals(2, query.get("size"));
        assertEquals(0, query.get("from"));
        InventoryESServiceTestSupport.assertJsonRoundTrip(query);
    }

    @Test
    void addNodeCountAggregations_setsSizeZeroAndTermsAgg() {
        Map<String, Object> query = service.addNodeCountAggregations(new HashMap<>(BASE_QUERY), "race");

        assertEquals(0, query.get("size"));
        @SuppressWarnings("unchecked")
        Map<String, Object> aggs = (Map<String, Object>) query.get("aggs");
        assertEquals(Map.of("terms", Map.of("field", "race", "size", 10000)), aggs.get("race"));
        InventoryESServiceTestSupport.assertJsonRoundTrip(query);
    }

    @Test
    @SuppressWarnings("unchecked")
    void addAggregations_withoutCardinality_buildsTermsOnly() {
        String[] fields = {"race", "sex_at_birth"};
        Map<String, Object> query = service.addAggregations(
                new HashMap<>(BASE_QUERY), fields, null, new String[]{}, List.of());

        assertEquals(0, query.get("size"));
        Map<String, Object> aggs = (Map<String, Object>) query.get("aggs");
        assertEquals(
                Map.of("terms", Map.of("field", "race", "size", 10000)),
                aggs.get("race"));
        assertEquals(
                Map.of("terms", Map.of("field", "sex_at_birth", "size", 10000)),
                aggs.get("sex_at_birth"));
        InventoryESServiceTestSupport.assertJsonRoundTrip(query);
    }

    @Test
    @SuppressWarnings("unchecked")
    void addAggregations_withCardinality_addsCardinalitySubAgg() {
        Map<String, Object> query = service.addAggregations(
                new HashMap<>(BASE_QUERY),
                new String[] {"race"},
                "participant_id",
                new String[] {},
                List.of());

        Map<String, Object> aggs = (Map<String, Object>) query.get("aggs");
        Map<String, Object> raceAgg = (Map<String, Object>) aggs.get("race");
        Map<String, Object> subAggs = (Map<String, Object>) raceAgg.get("aggs");
        Map<String, Object> cardinalityCount = (Map<String, Object>) subAggs.get("cardinality_count");
        assertEquals(
                Map.of("field", "participant_id", "precision_threshold", 40000),
                cardinalityCount.get("cardinality"));
        InventoryESServiceTestSupport.assertJsonRoundTrip(query);
    }

    @Test
    @SuppressWarnings("unchecked")
    void addAggregations_withOnlyIncludes_setsIncludeOnTerms() {
        Map<String, Object> query = service.addAggregations(
                new HashMap<>(BASE_QUERY),
                new String[] {"study_status"},
                null,
                new String[] {},
                List.of("Active", "Completed"));

        Map<String, Object> aggs = (Map<String, Object>) query.get("aggs");
        Map<String, Object> terms = (Map<String, Object>) ((Map<String, Object>) aggs.get("study_status")).get("terms");
        assertEquals(List.of("Active", "Completed"), terms.get("include"));
    }

    @Test
    void addAggregations_threeArgOverload_matchesFiveArgWithEmptyRanges() {
        Map<String, Object> viaThree = service.addAggregations(
                new HashMap<>(BASE_QUERY),
                new String[] {"race"},
                "participant_id",
                List.of());
        Map<String, Object> viaFive = service.addAggregations(
                new HashMap<>(BASE_QUERY),
                new String[] {"race"},
                "participant_id",
                new String[] {},
                List.of());

        assertEquals(viaFive.get("aggs"), viaThree.get("aggs"));
        assertEquals(0, viaThree.get("size"));
    }

    @Test
    @SuppressWarnings("unchecked")
    void addRangeCountAggregations_withCardinality_includesAgeBuckets() {
        Map<String, Object> query = service.addRangeCountAggregations(
                new HashMap<>(BASE_QUERY), "age_at_diagnosis", "participant_id");

        assertEquals(0, query.get("size"));
        Map<String, Object> aggs = (Map<String, Object>) query.get("aggs");
        Map<String, Object> ageAgg = (Map<String, Object>) aggs.get("age_at_diagnosis");
        Map<String, Object> range = (Map<String, Object>) ageAgg.get("range");
        assertEquals("age_at_diagnosis", range.get("field"));

        Set<Map<String, Object>> ranges = (Set<Map<String, Object>>) range.get("ranges");
        assertEquals(6, ranges.size());
        assertTrue(ranges.stream().anyMatch(r -> "0 - 4".equals(r.get("key"))));
        assertTrue(ranges.stream().anyMatch(r -> "> 29".equals(r.get("key"))));

        Map<String, Object> subAggs = (Map<String, Object>) ageAgg.get("aggs");
        assertNotNull(subAggs.get("cardinality_count"));
        InventoryESServiceTestSupport.assertJsonRoundTrip(query);
    }

    @Test
    @SuppressWarnings("unchecked")
    void addRangeCountAggregations_withoutCardinality_omitsSubAggs() {
        Map<String, Object> query = service.addRangeCountAggregations(
                new HashMap<>(BASE_QUERY), "age_at_diagnosis", null);

        Map<String, Object> ageAgg = (Map<String, Object>) ((Map<String, Object>) query.get("aggs")).get("age_at_diagnosis");
        assertFalse(ageAgg.containsKey("aggs"));
    }

    @Test
    @SuppressWarnings("unchecked")
    void addRangeAggregations_buildsInnerFilterAndStats() {
        Map<String, Object> query = service.addRangeAggregations(
                new HashMap<>(BASE_QUERY), "age_at_diagnosis", List.of());

        assertEquals(0, query.get("size"));
        Map<String, Object> inner = (Map<String, Object>) ((Map<String, Object>) query.get("aggs")).get("inner");
        assertEquals(
                Map.of("range", Map.of("age_at_diagnosis", Map.of("gt", -1))),
                inner.get("filter"));
        assertEquals(
                Map.of("stats", Map.of("field", "age_at_diagnosis")),
                ((Map<String, Object>) inner.get("aggs")).get("range_stats"));
        InventoryESServiceTestSupport.assertJsonRoundTrip(query);
    }

    @Test
    @SuppressWarnings("unchecked")
    void addCustomAggregations_emptyNestedProperty_rootLevelTerms() {
        Map<String, Object> query = service.addCustomAggregations(
                new HashMap<>(BASE_QUERY), "facetAgg", "study_status", "");

        Map<String, Object> facetAgg = (Map<String, Object>) ((Map<String, Object>) query.get("aggs")).get("facetAgg");
        assertEquals(Map.of("field", "study_status", "size", 1000), facetAgg.get("terms"));
        assertFalse(facetAgg.containsKey("nested"));
        InventoryESServiceTestSupport.assertJsonRoundTrip(query);
    }

    @Test
    @SuppressWarnings("unchecked")
    void addCustomAggregations_withNestedPath_buildsNestedAndReverseNested() {
        Map<String, Object> query = service.addCustomAggregations(
                new HashMap<>(BASE_QUERY),
                "facetAgg",
                "diagnosis",
                "sample_diagnosis_file_filters");

        Map<String, Object> facetAgg = (Map<String, Object>) ((Map<String, Object>) query.get("aggs")).get("facetAgg");
        Map<String, Object> nested = (Map<String, Object>) facetAgg.get("nested");
        assertEquals("sample_diagnosis_file_filters", nested.get("path"));

        Map<String, Object> aggBuckets =
                (Map<String, Object>) ((Map<String, Object>) facetAgg.get("aggs")).get("agg_buckets");
        Map<String, Object> terms = (Map<String, Object>) aggBuckets.get("terms");
        assertEquals("sample_diagnosis_file_filters.diagnosis", terms.get("field"));
        assertEquals(1000, terms.get("size"));

        Map<String, Object> bucketAggs = (Map<String, Object>) aggBuckets.get("aggs");
        assertTrue(((Map<String, Object>) bucketAggs.get("top_reverse_nested")).containsKey("reverse_nested"));
        InventoryESServiceTestSupport.assertJsonRoundTrip(query);
    }

    @Test
    @SuppressWarnings("unchecked")
    void addCardinalityHelper_matchesProductionShape() {
        Map<String, Object> helper = service.addCardinalityHelper("participant_id");

        Map<String, Object> cardinalityCount = (Map<String, Object>) helper.get("cardinality_count");
        assertEquals(
                Map.of("field", "participant_id", "precision_threshold", 40000),
                cardinalityCount.get("cardinality"));
    }
}
