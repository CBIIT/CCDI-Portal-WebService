package gov.nih.nci.bento_ri.service;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

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
 * Phase 1: {@link InventoryESService#buildFacetFilterQuery} for studies/cohorts indices and remaining edge cases.
 */
class InventoryESServiceBuildFacetFilterQueryStudiesCohortsAndEdgeCasesTest {

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
    void studies_noFilters_yieldsMatchAll() throws IOException {
        Map<String, Object> query = buildFacetFilter(service, Map.of(), "studies");

        assertEquals(Map.of("match_all", Map.of()), query.get("query"));
        InventoryESServiceTestSupport.assertJsonRoundTrip(query);
    }

    @Test
    void studies_studyIdTerm_usesTopLevelTerms() throws IOException {
        Map<String, Object> query = buildFacetFilter(
                service, Map.of("study_id", List.of("ccdi-int-study-1")), "studies");

        assertTrue(nonFilesFiltersContainTerms(
                nonFilesTopLevelFilters(query), "study_id", List.of("ccdi-int-study-1")));
        InventoryESServiceTestSupport.assertJsonRoundTrip(query);
    }

    @Test
    void cohorts_noFilters_yieldsMatchAll() throws IOException {
        Map<String, Object> query = buildFacetFilter(service, Map.of(), "cohorts");

        assertEquals(Map.of("match_all", Map.of()), query.get("query"));
        InventoryESServiceTestSupport.assertJsonRoundTrip(query);
    }

    @Test
    void cohorts_raceTerm_usesTopLevelTerms() throws IOException {
        Map<String, Object> query = buildFacetFilter(service, Map.of("race", List.of("Asian")), "cohorts");

        assertTrue(nonFilesFiltersContainTerms(nonFilesTopLevelFilters(query), "race", List.of("Asian")));
        InventoryESServiceTestSupport.assertJsonRoundTrip(query);
    }

    @Test
    void treatmentResponses_responseCategory_usesTopLevelTerms() throws IOException {
        Map<String, Object> query = buildFacetFilter(
                service, Map.of("response_category", List.of("Complete Response")), "treatment_responses");

        assertTrue(nonFilesFiltersContainTerms(
                nonFilesTopLevelFilters(query), "response_category", List.of("Complete Response")));
        InventoryESServiceTestSupport.assertJsonRoundTrip(query);
    }

    @Test
    void treatmentResponses_ageAtResponse_usesTopLevelRange() throws IOException {
        Map<String, Object> query = buildFacetFilter(
                service, Map.of("age_at_response", List.of(5, 15)), "treatment_responses");

        List<Object> filters = nonFilesTopLevelFilters(query);
        assertEquals(1, filters.size());
        assertTrue(filters.get(0).toString().contains("age_at_response"));
        assertTrue(nonFilesTopLevelFilters(query).stream()
                .noneMatch(f -> f.toString().contains("treatment_response_filters")));
        InventoryESServiceTestSupport.assertJsonRoundTrip(query);
    }

    @Test
    void samples_fileType_usesFileFiltersNested() throws IOException {
        Map<String, Object> query = buildFacetFilter(
                service, Map.of("file_type", List.of("BAM")), "samples");

        var nested = findNestedBlock(nonFilesTopLevelFilters(query), "file_filters");
        assertTrue(nested.isPresent());
        assertTrue(nonFilesFiltersContainTerms(
                nestedInnerFilters(nested.get()), "file_filters.file_type", List.of("BAM")));
        InventoryESServiceTestSupport.assertJsonRoundTrip(query);
    }

    @Test
    void participants_dataCategory_usesSampleDiagnosisFileFiltersNested() throws IOException {
        Map<String, Object> query = buildFacetFilter(
                service, Map.of("data_category", List.of("Sequencing")), "participants");

        var nested = findNestedBlock(nonFilesTopLevelFilters(query), "sample_diagnosis_file_filters");
        assertTrue(nested.isPresent());
        assertTrue(nonFilesFiltersContainTerms(
                nestedInnerFilters(nested.get()),
                "sample_diagnosis_file_filters.data_category",
                List.of("Sequencing")));
        InventoryESServiceTestSupport.assertJsonRoundTrip(query);
    }

    @Test
    void participants_excludedParam_isOmittedFromQuery() throws IOException {
        Map<String, Object> params = new HashMap<>();
        params.put("race", List.of("Asian"));
        params.put("first", 10);

        Map<String, Object> query = buildFacetFilter(service, params, Set.of("first"), "participants");

        assertTrue(nonFilesFiltersContainTerms(nonFilesTopLevelFilters(query), "race", List.of("Asian")));
        assertEquals(1, nonFilesTopLevelFilters(query).size());
        InventoryESServiceTestSupport.assertJsonRoundTrip(query);
    }

    @Test
    void participants_unknownAgesParam_isExcludedFromRegularFilters() throws IOException {
        Map<String, Object> params = new HashMap<>();
        params.put("race", List.of("Asian"));
        params.put("age_at_diagnosis_unknownAges", List.of("exclude"));

        Map<String, Object> query = buildFacetFilter(service, params, "participants");

        assertTrue(nonFilesFiltersContainTerms(nonFilesTopLevelFilters(query), "race", List.of("Asian")));
        assertEquals(1, nonFilesTopLevelFilters(query).size());
        InventoryESServiceTestSupport.assertJsonRoundTrip(query);
    }

    @Test
    @SuppressWarnings("unchecked")
    void files_importData_buildsShouldWithStudyAndParticipants() throws IOException {
        String importJson =
                "{\"study_id\":\"ccdi-int-study-1\",\"participant_id\":[\"ccdi-int-p001\",\"ccdi-int-p002\"]}";
        Map<String, Object> query = buildFacetFilter(service, Map.of("import_data", List.of(importJson)), "files");

        List<Object> filters = filesIndexFilters(query);
        Map<String, Object> importBool = filters.stream()
                .map(clause -> (Map<String, Object>) clause)
                .filter(map -> map.containsKey("bool"))
                .findFirst()
                .orElseThrow();
        List<Object> should = (List<Object>) ((Map<String, Object>) importBool.get("bool")).get("should");
        assertEquals(1, should.size());

        Map<String, Object> clause = (Map<String, Object>) should.get(0);
        List<Object> innerFilter = (List<Object>) ((Map<String, Object>) clause.get("bool")).get("filter");
        assertEquals(
                Map.of("term", Map.of("study_id", "ccdi-int-study-1")),
                innerFilter.get(0));
        assertEquals(
                Map.of("terms", Map.of("participant_id", List.of("ccdi-int-p001", "ccdi-int-p002"))),
                innerFilter.get(1));
        InventoryESServiceTestSupport.assertJsonRoundTrip(query);
    }

    @Test
    void files_treatmentType_usesCombinedTreatmentFiltersNested() throws IOException {
        Map<String, Object> query = buildFacetFilter(
                service, Map.of("treatment_type", List.of("Chemotherapy")), "files");

        var combined = findNestedInFilesCombinedFilters(filesIndexFilters(query)).orElseThrow();
        List<Object> inner = combinedFiltersInnerFilters(combined);
        assertTrue(inner.stream().anyMatch(clause -> {
            if (!(clause instanceof Map<?, ?> map)) {
                return false;
            }
            if (!((Map<?, ?>) map).containsKey("nested")) {
                return false;
            }
            @SuppressWarnings("unchecked")
            Map<String, Object> nested = (Map<String, Object>) ((Map<String, Object>) map).get("nested");
            return "combined_filters.treatment_filters".equals(nested.get("path"));
        }));
        InventoryESServiceTestSupport.assertJsonRoundTrip(query);
    }

    @Test
    void studyParticipants_suffixParticipants_usesParticipantNestedFilters() throws IOException {
        Map<String, Object> query = buildFacetFilter(
                service, Map.of("diagnosis", List.of("Cancer")), "study_participants");

        var nested = findNestedBlock(nonFilesTopLevelFilters(query), "sample_diagnosis_file_filters");
        assertTrue(nested.isPresent());
        InventoryESServiceTestSupport.assertJsonRoundTrip(query);
    }
}
