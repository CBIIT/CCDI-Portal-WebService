package gov.nih.nci.bento_ri.service;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static gov.nih.nci.bento_ri.service.InventoryESServiceFacetFilterQueryTestSupport.NESTED_FILTERS;
import static gov.nih.nci.bento_ri.service.InventoryESServiceFacetFilterQueryTestSupport.RANGE_PARAMS;
import static gov.nih.nci.bento_ri.service.InventoryESServiceFacetFilterQueryTestSupport.buildFacetFilter;
import static gov.nih.nci.bento_ri.service.InventoryESServiceFacetFilterQueryTestSupport.filesIndexFilters;
import static gov.nih.nci.bento_ri.service.InventoryESServiceFacetFilterQueryTestSupport.findNestedBlock;
import static gov.nih.nci.bento_ri.service.InventoryESServiceFacetFilterQueryTestSupport.nestedInnerFilters;
import static gov.nih.nci.bento_ri.service.InventoryESServiceFacetFilterQueryTestSupport.nestedInnerFiltersForFilesCombinedPath;
import static gov.nih.nci.bento_ri.service.InventoryESServiceFacetFilterQueryTestSupport.nonFilesFiltersContainTerms;
import static gov.nih.nci.bento_ri.service.InventoryESServiceFacetFilterQueryTestSupport.nonFilesTopLevelFilters;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Targets remaining JaCoCo branch gaps in {@link InventoryESService#buildFacetFilterQuery}.
 */
class InventoryESServiceBuildFacetFilterQueryRemainingBranchesTest {

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
    void files_ageAtDiagnosisDefault_includesUnknownSentinelInCombinedSampleDiagnosis() throws IOException {
        Map<String, Object> query = buildFacetFilter(service, Map.of("age_at_diagnosis", List.of(5, 15)), "files");

        List<Object> inner = nestedInnerFiltersForFilesCombinedPath(query, "combined_filters.sample_diagnosis_filters");
        Map<String, Object> ageClause = (Map<String, Object>) inner.get(0);
        assertTrue(ageClause.containsKey("bool"));
        assertTrue(ageClause.toString().contains("-999"));
        InventoryESServiceTestSupport.assertJsonRoundTrip(query);
    }

    @Test
    void files_nonAgeRangeField_usesTopLevelPlainRange() throws IOException {
        Set<String> extendedRange = new HashSet<>(RANGE_PARAMS);
        extendedRange.add("file_size");
        Map<String, Object> query = service.buildFacetFilterQuery(
                Map.of("file_size", List.of(100, 500)),
                extendedRange,
                Set.of(),
                Set.of(),
                NESTED_FILTERS,
                "files");

        assertTrue(filesIndexFilters(query).stream().anyMatch(clause -> clause.toString().contains("file_size")));
        InventoryESServiceTestSupport.assertJsonRoundTrip(query);
    }

    @Test
    @SuppressWarnings("unchecked")
    void participants_ageAtTreatmentStartUnknownAgesInclude_usesPlainNestedRange() throws IOException {
        Map<String, Object> params = new HashMap<>();
        params.put("age_at_treatment_start", List.of(8, 12));
        params.put("age_at_treatment_start_unknownAges", List.of("include"));

        Map<String, Object> query = buildFacetFilter(service, params, "participants");
        var nested = findNestedBlock(nonFilesTopLevelFilters(query), "treatment_filters").orElseThrow();
        Map<String, Object> ageClause = (Map<String, Object>) nestedInnerFilters(nested).get(0);
        assertTrue(ageClause.containsKey("range"));
        assertTrue(!ageClause.toString().contains("-999"));
        InventoryESServiceTestSupport.assertJsonRoundTrip(query);
    }

    @Test
    @SuppressWarnings("unchecked")
    void participants_ageAtResponseUnknownAgesInclude_usesPlainNestedRange() throws IOException {
        Map<String, Object> params = new HashMap<>();
        params.put("age_at_response", List.of(3, 9));
        params.put("age_at_response_unknownAges", List.of("include"));

        Map<String, Object> query = buildFacetFilter(service, params, "participants");
        var nested = findNestedBlock(nonFilesTopLevelFilters(query), "treatment_response_filters").orElseThrow();
        Map<String, Object> ageClause = (Map<String, Object>) nestedInnerFilters(nested).get(0);
        assertTrue(ageClause.containsKey("range"));
        InventoryESServiceTestSupport.assertJsonRoundTrip(query);
    }

    @Test
    @SuppressWarnings("unchecked")
    void participants_ageAtLastKnownSurvivalUnknownAgesInclude_usesPlainNestedRange() throws IOException {
        Map<String, Object> params = new HashMap<>();
        params.put("age_at_last_known_survival_status", List.of(1, 5));
        params.put("age_at_last_known_survival_status_unknownAges", List.of("include"));

        Map<String, Object> query = buildFacetFilter(service, params, "participants");
        var nested = findNestedBlock(nonFilesTopLevelFilters(query), "survival_filters").orElseThrow();
        Map<String, Object> ageClause = (Map<String, Object>) nestedInnerFilters(nested).get(0);
        assertTrue(ageClause.containsKey("range"));
        InventoryESServiceTestSupport.assertJsonRoundTrip(query);
    }

    @Test
    void participants_ageAtTreatmentStartUnknownAgesExclude_usesNestedTreatmentMustNot() throws IOException {
        Map<String, Object> params = new HashMap<>();
        params.put("age_at_treatment_start", List.of(8, 12));
        params.put("age_at_treatment_start_unknownAges", List.of("exclude"));

        Map<String, Object> query = buildFacetFilter(service, params, "participants");
        var nested = findNestedBlock(nonFilesTopLevelFilters(query), "treatment_filters").orElseThrow();
        assertTrue(nestedInnerFilters(nested).stream().anyMatch(clause -> clause.toString().contains("must_not")));
        InventoryESServiceTestSupport.assertJsonRoundTrip(query);
    }

    @Test
    void participants_ageAtResponseUnknownAgesExclude_usesNestedTreatmentResponseMustNot() throws IOException {
        Map<String, Object> params = new HashMap<>();
        params.put("age_at_response", List.of(3, 9));
        params.put("age_at_response_unknownAges", List.of("exclude"));

        Map<String, Object> query = buildFacetFilter(service, params, "participants");
        var nested = findNestedBlock(nonFilesTopLevelFilters(query), "treatment_response_filters").orElseThrow();
        assertTrue(nestedInnerFilters(nested).stream().anyMatch(clause -> clause.toString().contains("must_not")));
        InventoryESServiceTestSupport.assertJsonRoundTrip(query);
    }

    @Test
    void participants_ageAtLastKnownSurvivalUnknownAgesExclude_usesNestedSurvivalMustNot() throws IOException {
        Map<String, Object> params = new HashMap<>();
        params.put("age_at_last_known_survival_status", List.of(1, 5));
        params.put("age_at_last_known_survival_status_unknownAges", List.of("exclude"));

        Map<String, Object> query = buildFacetFilter(service, params, "participants");
        var nested = findNestedBlock(nonFilesTopLevelFilters(query), "survival_filters").orElseThrow();
        assertTrue(nestedInnerFilters(nested).stream().anyMatch(clause -> clause.toString().contains("must_not")));
        InventoryESServiceTestSupport.assertJsonRoundTrip(query);
    }

    @Test
    void participants_ageAtTreatmentStartUnknownAgesOnly_usesNestedTreatmentSentinelTerms() throws IOException {
        Map<String, Object> params = new HashMap<>();
        params.put("age_at_treatment_start", List.of(8, 12));
        params.put("age_at_treatment_start_unknownAges", List.of("only"));

        Map<String, Object> query = buildFacetFilter(service, params, "participants");
        var nested = findNestedBlock(nonFilesTopLevelFilters(query), "treatment_filters").orElseThrow();
        assertTrue(nonFilesFiltersContainTerms(
                nestedInnerFilters(nested), "treatment_filters.age_at_treatment_start", List.of(-999)));
        InventoryESServiceTestSupport.assertJsonRoundTrip(query);
    }

    @Test
    void participants_ageAtResponseUnknownAgesOnly_usesNestedTreatmentResponseSentinelTerms() throws IOException {
        Map<String, Object> params = new HashMap<>();
        params.put("age_at_response", List.of(3, 9));
        params.put("age_at_response_unknownAges", List.of("only"));

        Map<String, Object> query = buildFacetFilter(service, params, "participants");
        var nested = findNestedBlock(nonFilesTopLevelFilters(query), "treatment_response_filters").orElseThrow();
        assertTrue(nonFilesFiltersContainTerms(
                nestedInnerFilters(nested), "treatment_response_filters.age_at_response", List.of(-999)));
        InventoryESServiceTestSupport.assertJsonRoundTrip(query);
    }

    @Test
    void participants_ageAtLastKnownSurvivalUnknownAgesOnly_usesNestedSurvivalSentinelTerms() throws IOException {
        Map<String, Object> params = new HashMap<>();
        params.put("age_at_last_known_survival_status", List.of(1, 5));
        params.put("age_at_last_known_survival_status_unknownAges", List.of("only"));

        Map<String, Object> query = buildFacetFilter(service, params, "participants");
        var nested = findNestedBlock(nonFilesTopLevelFilters(query), "survival_filters").orElseThrow();
        assertTrue(nonFilesFiltersContainTerms(
                nestedInnerFilters(nested), "survival_filters.age_at_last_known_survival_status", List.of(-999)));
        InventoryESServiceTestSupport.assertJsonRoundTrip(query);
    }

    @Test
    void diagnosis_librarySelection_usesSampleFileFiltersNested() throws IOException {
        Map<String, Object> query = buildFacetFilter(service, Map.of("library_selection", List.of("Hybrid Selection")), "diagnosis");

        var nested = findNestedBlock(nonFilesTopLevelFilters(query), "sample_file_filters").orElseThrow();
        assertTrue(nonFilesFiltersContainTerms(
                nestedInnerFilters(nested), "sample_file_filters.library_selection", List.of("Hybrid Selection")));
        InventoryESServiceTestSupport.assertJsonRoundTrip(query);
    }
}
