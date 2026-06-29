package gov.nih.nci.bento_ri.service;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Arrays;
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
 * Additional branch coverage for {@link InventoryESService#buildFacetFilterQuery}.
 */
class InventoryESServiceBuildFacetFilterQueryBranchCoverageTest {

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
    void participants_ageAtTreatmentStart_usesTreatmentFiltersNestedWithUnknownSentinel() throws IOException {
        Map<String, Object> query = buildFacetFilter(
                service, Map.of("age_at_treatment_start", List.of(8, 12)), "participants");

        var nested = findNestedBlock(nonFilesTopLevelFilters(query), "treatment_filters").orElseThrow();
        Map<String, Object> ageClause = (Map<String, Object>) nestedInnerFilters(nested).get(0);
        List<Object> should = (List<Object>) ((Map<String, Object>) ageClause.get("bool")).get("should");
        assertEquals(2, should.size());
        InventoryESServiceTestSupport.assertJsonRoundTrip(query);
    }

    @Test
    @SuppressWarnings("unchecked")
    void participants_ageAtResponse_usesTreatmentResponseFiltersNestedWithUnknownSentinel() throws IOException {
        Map<String, Object> query = buildFacetFilter(
                service, Map.of("age_at_response", List.of(3, 9)), "participants");

        var nested = findNestedBlock(nonFilesTopLevelFilters(query), "treatment_response_filters").orElseThrow();
        Map<String, Object> ageClause = (Map<String, Object>) nestedInnerFilters(nested).get(0);
        assertTrue(ageClause.containsKey("bool"));
        InventoryESServiceTestSupport.assertJsonRoundTrip(query);
    }

    @Test
    @SuppressWarnings("unchecked")
    void participants_ageAtLastKnownSurvivalStatus_usesSurvivalFiltersNestedWithUnknownSentinel() throws IOException {
        Map<String, Object> query = buildFacetFilter(
                service, Map.of("age_at_last_known_survival_status", List.of(1, 5)), "participants");

        var nested = findNestedBlock(nonFilesTopLevelFilters(query), "survival_filters").orElseThrow();
        Map<String, Object> ageClause = (Map<String, Object>) nestedInnerFilters(nested).get(0);
        assertTrue(ageClause.containsKey("bool"));
        InventoryESServiceTestSupport.assertJsonRoundTrip(query);
    }

    @Test
    @SuppressWarnings("unchecked")
    void participants_participantAgeAtCollection_usesSampleDiagnosisFileFiltersNested() throws IOException {
        Map<String, Object> query = buildFacetFilter(
                service, Map.of("participant_age_at_collection", List.of(4, 8)), "participants");

        var nested = findNestedBlock(nonFilesTopLevelFilters(query), "sample_diagnosis_file_filters").orElseThrow();
        Map<String, Object> ageClause = (Map<String, Object>) nestedInnerFilters(nested).get(0);
        assertTrue(ageClause.containsKey("bool"));
        InventoryESServiceTestSupport.assertJsonRoundTrip(query);
    }

    @Test
    @SuppressWarnings("unchecked")
    void participants_ageRangeUpperBoundOnly_usesLteOnly() throws IOException {
        Map<String, Object> params = new HashMap<>();
        params.put("age_at_diagnosis", Arrays.asList(null, 20));

        Map<String, Object> query = buildFacetFilter(service, params, "participants");
        var nested = findNestedBlock(nonFilesTopLevelFilters(query), "sample_diagnosis_file_filters").orElseThrow();
        Map<String, Object> ageClause = (Map<String, Object>) nestedInnerFilters(nested).get(0);
        Map<String, Object> shouldBool = (Map<String, Object>) ageClause.get("bool");
        Map<String, Object> rangePart = (Map<String, Object>) ((List<Object>) shouldBool.get("should")).get(0);
        Map<String, Object> rangeSpec = (Map<String, Object>) rangePart.get("range");
        assertEquals(Map.of("lte", 20), rangeSpec.get("sample_diagnosis_file_filters.age_at_diagnosis"));
    }

    @Test
    void treatments_unknownAgesExclude_usesTopLevelExistsAndMustNot() throws IOException {
        Map<String, Object> params = new HashMap<>();
        params.put("age_at_treatment_start", List.of(5, 15));
        params.put("age_at_treatment_start_unknownAges", List.of("exclude"));

        Map<String, Object> query = buildFacetFilter(service, params, "treatments");
        List<Object> filters = nonFilesTopLevelFilters(query);
        assertTrue(filters.stream().anyMatch(clause -> clause.toString().contains("must_not")));
        InventoryESServiceTestSupport.assertJsonRoundTrip(query);
    }

    @Test
    void survivals_unknownAgesOnly_usesTopLevelUnknownSentinelTerms() throws IOException {
        Map<String, Object> params = new HashMap<>();
        params.put("age_at_last_known_survival_status", List.of(10, 20));
        params.put("age_at_last_known_survival_status_unknownAges", List.of("only"));

        Map<String, Object> query = buildFacetFilter(service, params, "survivals");
        assertTrue(nonFilesFiltersContainTerms(
                nonFilesTopLevelFilters(query), "age_at_last_known_survival_status", List.of(-999)));
        InventoryESServiceTestSupport.assertJsonRoundTrip(query);
    }

    @Test
    void treatmentResponses_unknownAgesExclude_usesTopLevelExistsAndMustNot() throws IOException {
        Map<String, Object> params = new HashMap<>();
        params.put("age_at_response", List.of(2, 8));
        params.put("age_at_response_unknownAges", List.of("exclude"));

        Map<String, Object> query = buildFacetFilter(service, params, "treatment_responses");
        assertTrue(nonFilesTopLevelFilters(query).stream().anyMatch(clause -> clause.toString().contains("must_not")));
        InventoryESServiceTestSupport.assertJsonRoundTrip(query);
    }

    @Test
    @SuppressWarnings("unchecked")
    void samples_ageAtDiagnosisDefault_includesUnknownSentinelInDiagnosisFilters() throws IOException {
        Map<String, Object> query = buildFacetFilter(service, Map.of("age_at_diagnosis", List.of(6, 12)), "samples");

        var nested = findNestedBlock(nonFilesTopLevelFilters(query), "diagnosis_filters").orElseThrow();
        Map<String, Object> ageClause = (Map<String, Object>) nestedInnerFilters(nested).get(0);
        assertTrue(ageClause.containsKey("bool"));
        InventoryESServiceTestSupport.assertJsonRoundTrip(query);
    }

    @Test
    void diagnosis_participantAgeAtCollectionUnknownExclude_usesSampleFileFilters() throws IOException {
        Map<String, Object> params = new HashMap<>();
        params.put("participant_age_at_collection", List.of(3, 7));
        params.put("participant_age_at_collection_unknownAges", List.of("exclude"));

        Map<String, Object> query = buildFacetFilter(service, params, "diagnosis");
        var nested = findNestedBlock(nonFilesTopLevelFilters(query), "sample_file_filters").orElseThrow();
        assertTrue(nestedInnerFilters(nested).stream().anyMatch(clause -> clause.toString().contains("must_not")));
        InventoryESServiceTestSupport.assertJsonRoundTrip(query);
    }

    @Test
    void participants_multipleNestedFilters_combinesAllNestedPaths() throws IOException {
        Map<String, Object> params = Map.of(
                "race", List.of("Asian"),
                "diagnosis", List.of("Leukemia"),
                "treatment_type", List.of("Chemotherapy"),
                "last_known_survival_status", List.of("Alive"));

        Map<String, Object> query = buildFacetFilter(service, params, "participants");
        List<Object> filters = nonFilesTopLevelFilters(query);
        assertTrue(nonFilesFiltersContainTerms(filters, "race", List.of("Asian")));
        assertTrue(findNestedBlock(filters, "sample_diagnosis_file_filters").isPresent());
        assertTrue(findNestedBlock(filters, "treatment_filters").isPresent());
        assertTrue(findNestedBlock(filters, "survival_filters").isPresent());
        InventoryESServiceTestSupport.assertJsonRoundTrip(query);
    }

    @Test
    @SuppressWarnings("unchecked")
    void files_ageAtTreatmentStart_usesCombinedTreatmentFiltersWithUnknownSentinel() throws IOException {
        Map<String, Object> query = buildFacetFilter(
                service, Map.of("age_at_treatment_start", List.of(6, 10)), "files");

        var combined = findNestedInFilesCombinedFilters(filesIndexFilters(query)).orElseThrow();
        List<Object> inner = combinedFiltersInnerFilters(combined);
        assertTrue(inner.stream().anyMatch(clause -> {
            Map<String, Object> map = (Map<String, Object>) clause;
            return map.containsKey("nested")
                    && "combined_filters.treatment_filters".equals(
                            ((Map<String, Object>) map.get("nested")).get("path"));
        }));
        InventoryESServiceTestSupport.assertJsonRoundTrip(query);
    }

    @Test
    @SuppressWarnings("unchecked")
    void files_ageAtResponse_usesCombinedTreatmentResponseFilters() throws IOException {
        Map<String, Object> query = buildFacetFilter(service, Map.of("age_at_response", List.of(1, 4)), "files");

        var combined = findNestedInFilesCombinedFilters(filesIndexFilters(query)).orElseThrow();
        List<Object> inner = combinedFiltersInnerFilters(combined);
        assertTrue(inner.stream().anyMatch(clause -> {
            Map<String, Object> map = (Map<String, Object>) clause;
            return map.containsKey("nested")
                    && "combined_filters.treatment_response_filters".equals(
                            ((Map<String, Object>) map.get("nested")).get("path"));
        }));
        InventoryESServiceTestSupport.assertJsonRoundTrip(query);
    }

    @Test
    @SuppressWarnings("unchecked")
    void files_ageAtLastKnownSurvivalStatus_usesCombinedSurvivalFilters() throws IOException {
        Map<String, Object> query = buildFacetFilter(
                service, Map.of("age_at_last_known_survival_status", List.of(2, 6)), "files");

        var combined = findNestedInFilesCombinedFilters(filesIndexFilters(query)).orElseThrow();
        List<Object> inner = combinedFiltersInnerFilters(combined);
        assertTrue(inner.stream().anyMatch(clause -> {
            Map<String, Object> map = (Map<String, Object>) clause;
            return map.containsKey("nested")
                    && "combined_filters.survival_filters".equals(
                            ((Map<String, Object>) map.get("nested")).get("path"));
        }));
        InventoryESServiceTestSupport.assertJsonRoundTrip(query);
    }

    @Test
    @SuppressWarnings("unchecked")
    void files_participantAgeAtCollection_usesCombinedSampleDiagnosisFilters() throws IOException {
        Map<String, Object> query = buildFacetFilter(
                service, Map.of("participant_age_at_collection", List.of(5, 9)), "files");

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

    @Test
    void files_diagnosisTerm_usesCombinedSampleDiagnosisFiltersNested() throws IOException {
        Map<String, Object> query = buildFacetFilter(service, Map.of("diagnosis", List.of("Lymphoma")), "files");

        var combined = findNestedInFilesCombinedFilters(filesIndexFilters(query)).orElseThrow();
        List<Object> inner = combinedFiltersInnerFilters(combined);
        assertTrue(inner.stream().anyMatch(clause -> clause.toString().contains("sample_diagnosis_filters")));
        InventoryESServiceTestSupport.assertJsonRoundTrip(query);
    }

    @Test
    void files_sampleTumorStatus_usesCombinedSampleDiagnosisFiltersNested() throws IOException {
        Map<String, Object> query = buildFacetFilter(service, Map.of("sample_tumor_status", List.of("Tumor")), "files");

        var combined = findNestedInFilesCombinedFilters(filesIndexFilters(query)).orElseThrow();
        assertTrue(combinedFiltersInnerFilters(combined).stream()
                .anyMatch(clause -> clause.toString().contains("sample_diagnosis_filters")));
        InventoryESServiceTestSupport.assertJsonRoundTrip(query);
    }

    @Test
    void files_responseCategory_usesCombinedTreatmentResponseFiltersNested() throws IOException {
        Map<String, Object> query = buildFacetFilter(
                service, Map.of("response_category", List.of("Partial Response")), "files");

        var combined = findNestedInFilesCombinedFilters(filesIndexFilters(query)).orElseThrow();
        assertTrue(combinedFiltersInnerFilters(combined).stream()
                .anyMatch(clause -> clause.toString().contains("treatment_response_filters")));
        InventoryESServiceTestSupport.assertJsonRoundTrip(query);
    }

    @Test
    void files_participantIdsAlias_usesCombinedParticipantFilters() throws IOException {
        Map<String, Object> query = buildFacetFilter(
                service, Map.of("participant_ids", List.of("ccdi-int-p001")), "files");

        var combined = findNestedInFilesCombinedFilters(filesIndexFilters(query)).orElseThrow();
        assertTrue(nonFilesFiltersContainTerms(
                combinedFiltersInnerFilters(combined), "combined_filters.participant_id", List.of("ccdi-int-p001")));
        InventoryESServiceTestSupport.assertJsonRoundTrip(query);
    }

    @Test
    void files_unknownAgesOnlyOnTreatmentStart_matchesUnknownSentinel() throws IOException {
        Map<String, Object> params = new HashMap<>();
        params.put("age_at_treatment_start", List.of(5, 10));
        params.put("age_at_treatment_start_unknownAges", List.of("only"));

        Map<String, Object> query = buildFacetFilter(service, params, "files");
        var combined = findNestedInFilesCombinedFilters(filesIndexFilters(query)).orElseThrow();
        assertTrue(combinedFiltersInnerFilters(combined).stream()
                .anyMatch(clause -> clause.toString().contains("treatment_filters")
                        && clause.toString().contains("-999")));
        InventoryESServiceTestSupport.assertJsonRoundTrip(query);
    }

    @Test
    void files_importDataInvalidJson_stillBuildsQueryWithoutThrowing() throws IOException {
        Map<String, Object> query = buildFacetFilter(service, Map.of("import_data", List.of("{bad-json")), "files");

        assertTrue(filesIndexFilters(query).stream().anyMatch(clause -> clause.toString().contains("should")));
        InventoryESServiceTestSupport.assertJsonRoundTrip(query);
    }
}
