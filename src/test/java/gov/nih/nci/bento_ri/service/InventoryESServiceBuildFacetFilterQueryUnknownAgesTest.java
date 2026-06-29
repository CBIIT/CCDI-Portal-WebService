package gov.nih.nci.bento_ri.service;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static gov.nih.nci.bento_ri.service.InventoryESServiceFacetFilterQueryTestSupport.buildFacetFilter;
import static gov.nih.nci.bento_ri.service.InventoryESServiceFacetFilterQueryTestSupport.combinedFiltersInnerFilters;
import static gov.nih.nci.bento_ri.service.InventoryESServiceFacetFilterQueryTestSupport.filesIndexFilters;
import static gov.nih.nci.bento_ri.service.InventoryESServiceFacetFilterQueryTestSupport.findNestedBlock;
import static gov.nih.nci.bento_ri.service.InventoryESServiceFacetFilterQueryTestSupport.findNestedInFilesCombinedFilters;
import static gov.nih.nci.bento_ri.service.InventoryESServiceFacetFilterQueryTestSupport.nestedInnerFilters;
import static gov.nih.nci.bento_ri.service.InventoryESServiceFacetFilterQueryTestSupport.nonFilesFiltersContainTerms;
import static gov.nih.nci.bento_ri.service.InventoryESServiceFacetFilterQueryTestSupport.nonFilesTopLevelFilters;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Phase 2: {@code *_unknownAges} handling in {@link InventoryESService#buildFacetFilterQuery}.
 */
class InventoryESServiceBuildFacetFilterQueryUnknownAgesTest {

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
    @SuppressWarnings("unchecked")
    void participants_unknownAgesExclude_addsExistsAndMustNotUnknown() throws IOException {
        Map<String, Object> params = new HashMap<>();
        params.put("age_at_diagnosis", List.of(10, 20));
        params.put("age_at_diagnosis_unknownAges", List.of("exclude"));

        Map<String, Object> query = buildFacetFilter(service, params, "participants");
        var nested = findNestedBlock(nonFilesTopLevelFilters(query), "sample_diagnosis_file_filters").orElseThrow();
        List<Object> inner = nestedInnerFilters(nested);

        assertTrue(inner.stream().anyMatch(clause -> {
            Map<String, Object> map = (Map<String, Object>) clause;
            if (!map.containsKey("range")) {
                return false;
            }
            Map<String, Object> range = (Map<String, Object>) map.get("range");
            return Map.of("gte", 10, "lte", 20).equals(range.get("sample_diagnosis_file_filters.age_at_diagnosis"));
        }));
        assertTrue(inner.stream().anyMatch(clause -> {
            if (!(clause instanceof Map<?, ?> map) || !map.containsKey("bool")) {
                return false;
            }
            Map<String, Object> bool = (Map<String, Object>) map.get("bool");
            return bool.containsKey("must") && bool.containsKey("must_not");
        }));
        InventoryESServiceTestSupport.assertJsonRoundTrip(query);
    }

    @Test
    void participants_unknownAgesOnly_matchesUnknownSentinelOnly() throws IOException {
        Map<String, Object> params = new HashMap<>();
        params.put("age_at_diagnosis", List.of(10, 20));
        params.put("age_at_diagnosis_unknownAges", List.of("only"));

        Map<String, Object> query = buildFacetFilter(service, params, "participants");
        var nested = findNestedBlock(nonFilesTopLevelFilters(query), "sample_diagnosis_file_filters").orElseThrow();
        assertTrue(nonFilesFiltersContainTerms(
                nestedInnerFilters(nested),
                "sample_diagnosis_file_filters.age_at_diagnosis",
                List.of(-999)));
        InventoryESServiceTestSupport.assertJsonRoundTrip(query);
    }

    @Test
    @SuppressWarnings("unchecked")
    void participants_unknownAgesInclude_usesPlainRangeWithoutUnknownSentinel() throws IOException {
        // Any non-empty unknownAges value disables default -999 matching; "include" uses strict range only.
        Map<String, Object> params = new HashMap<>();
        params.put("age_at_diagnosis", List.of(5, 15));
        params.put("age_at_diagnosis_unknownAges", List.of("include"));

        Map<String, Object> query = buildFacetFilter(service, params, "participants");
        var nested = findNestedBlock(nonFilesTopLevelFilters(query), "sample_diagnosis_file_filters").orElseThrow();
        Map<String, Object> ageClause = (Map<String, Object>) nestedInnerFilters(nested).get(0);
        assertTrue(ageClause.containsKey("range"));
        Map<String, Object> range = (Map<String, Object>) ageClause.get("range");
        assertEquals(Map.of("gte", 5, "lte", 15), range.get("sample_diagnosis_file_filters.age_at_diagnosis"));
        InventoryESServiceTestSupport.assertJsonRoundTrip(query);
    }

    @Test
    void samples_unknownAgesExclude_usesDiagnosisFiltersPath() throws IOException {
        Map<String, Object> params = new HashMap<>();
        params.put("age_at_diagnosis", List.of(7, 12));
        params.put("age_at_diagnosis_unknownAges", List.of("exclude"));

        Map<String, Object> query = buildFacetFilter(service, params, "samples");
        var nested = findNestedBlock(nonFilesTopLevelFilters(query), "diagnosis_filters").orElseThrow();
        List<Object> inner = nestedInnerFilters(nested);
        assertTrue(inner.stream().anyMatch(clause -> clause.toString().contains("must_not")));
        InventoryESServiceTestSupport.assertJsonRoundTrip(query);
    }

    @Test
    @SuppressWarnings("unchecked")
    void files_unknownAgesExclude_usesCombinedSampleDiagnosisFilters() throws IOException {
        Map<String, Object> params = new HashMap<>();
        params.put("age_at_diagnosis", List.of(7, 12));
        params.put("age_at_diagnosis_unknownAges", List.of("exclude"));

        Map<String, Object> query = buildFacetFilter(service, params, "files");
        var combined = findNestedInFilesCombinedFilters(filesIndexFilters(query)).orElseThrow();
        List<Object> inner = combinedFiltersInnerFilters(combined);
        assertTrue(inner.stream().anyMatch(clause -> {
            Map<String, Object> map = (Map<String, Object>) clause;
            return map.containsKey("nested")
                    && "combined_filters.sample_diagnosis_filters".equals(
                            ((Map<String, Object>) map.get("nested")).get("path"));
        }));
        InventoryESServiceTestSupport.assertJsonRoundTrip(query);
    }
}
