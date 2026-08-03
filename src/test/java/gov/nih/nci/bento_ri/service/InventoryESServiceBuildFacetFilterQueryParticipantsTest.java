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
import static gov.nih.nci.bento_ri.service.InventoryESServiceFacetFilterQueryTestSupport.findNestedBlock;
import static gov.nih.nci.bento_ri.service.InventoryESServiceFacetFilterQueryTestSupport.nestedInnerFilters;
import static gov.nih.nci.bento_ri.service.InventoryESServiceFacetFilterQueryTestSupport.nonFilesFiltersContainTerms;
import static gov.nih.nci.bento_ri.service.InventoryESServiceFacetFilterQueryTestSupport.nonFilesTopLevelFilters;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Phase 2: {@link InventoryESService#buildFacetFilterQuery} for non-files indices (participants focus).
 */
class InventoryESServiceBuildFacetFilterQueryParticipantsTest {

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
    void participants_diagnosisTerm_usesSampleDiagnosisFileFiltersNested() throws IOException {
        Map<String, Object> query = buildFacetFilter(
                service, Map.of("diagnosis", List.of("Acute lymphoblastic leukemia")), "participants");

        var nested = findNestedBlock(nonFilesTopLevelFilters(query), "sample_diagnosis_file_filters");
        assertTrue(nested.isPresent());
        assertTrue(nonFilesFiltersContainTerms(
                nestedInnerFilters(nested.get()),
                "sample_diagnosis_file_filters.diagnosis",
                List.of("Acute lymphoblastic leukemia")));
        InventoryESServiceTestSupport.assertJsonRoundTrip(query);
    }

    @Test
    void participants_survivalTerm_usesSurvivalFiltersNested() throws IOException {
        Map<String, Object> query = buildFacetFilter(
                service, Map.of("last_known_survival_status", List.of("Alive")), "participants");

        var nested = findNestedBlock(nonFilesTopLevelFilters(query), "survival_filters");
        assertTrue(nested.isPresent());
        assertTrue(nonFilesFiltersContainTerms(
                nestedInnerFilters(nested.get()),
                "survival_filters.last_known_survival_status",
                List.of("Alive")));
        InventoryESServiceTestSupport.assertJsonRoundTrip(query);
    }

    @Test
    void participants_treatmentTerm_usesTreatmentFiltersNested() throws IOException {
        Map<String, Object> query = buildFacetFilter(
                service, Map.of("treatment_type", List.of("Chemotherapy")), "participants");

        var nested = findNestedBlock(nonFilesTopLevelFilters(query), "treatment_filters");
        assertTrue(nested.isPresent());
        assertTrue(nonFilesFiltersContainTerms(
                nestedInnerFilters(nested.get()),
                "treatment_filters.treatment_type",
                List.of("Chemotherapy")));
        InventoryESServiceTestSupport.assertJsonRoundTrip(query);
    }

    @Test
    void participants_treatmentResponseTerm_usesTreatmentResponseFiltersNested() throws IOException {
        Map<String, Object> query = buildFacetFilter(
                service, Map.of("response_category", List.of("Complete Response")), "participants");

        var nested = findNestedBlock(nonFilesTopLevelFilters(query), "treatment_response_filters");
        assertTrue(nested.isPresent());
        assertTrue(nonFilesFiltersContainTerms(
                nestedInnerFilters(nested.get()),
                "treatment_response_filters.response_category",
                List.of("Complete Response")));
        InventoryESServiceTestSupport.assertJsonRoundTrip(query);
    }

    @Test
    void participants_sampleAnatomicSite_usesSampleDiagnosisFileFiltersNested() throws IOException {
        Map<String, Object> query = buildFacetFilter(
                service, Map.of("sample_anatomic_site", List.of("Bone marrow")), "participants");

        var nested = findNestedBlock(nonFilesTopLevelFilters(query), "sample_diagnosis_file_filters");
        assertTrue(nested.isPresent());
        assertTrue(nonFilesFiltersContainTerms(
                nestedInnerFilters(nested.get()),
                "sample_diagnosis_file_filters.sample_anatomic_site",
                List.of("Bone marrow")));
        InventoryESServiceTestSupport.assertJsonRoundTrip(query);
    }

    @Test
    void participants_participantIdsAlias_mapsToParticipantIdTerms() throws IOException {
        Map<String, Object> query = buildFacetFilter(
                service, Map.of("participant_ids", List.of("ccdi-int-p001")), "participants");

        assertTrue(nonFilesFiltersContainTerms(
                nonFilesTopLevelFilters(query), "participant_id", List.of("ccdi-int-p001")));
        InventoryESServiceTestSupport.assertJsonRoundTrip(query);
    }

    @Test
    @SuppressWarnings("unchecked")
    void participants_importData_buildsShouldWithStudyAndParticipants() throws IOException {
        String importJson =
                "{\"study_id\":\"ccdi-int-study-1\",\"participant_id\":[\"ccdi-int-p001\",\"ccdi-int-p002\"]}";
        Map<String, Object> query = buildFacetFilter(service, Map.of("import_data", List.of(importJson)), "participants");

        List<Object> filters = nonFilesTopLevelFilters(query);
        assertEquals(1, filters.size());
        Map<String, Object> importBool = (Map<String, Object>) filters.get(0);
        List<Object> should = (List<Object>) ((Map<String, Object>) importBool.get("bool")).get("should");
        assertEquals(1, should.size());

        Map<String, Object> clause = (Map<String, Object>) should.get(0);
        Map<String, Object> clauseBool = (Map<String, Object>) clause.get("bool");
        List<Object> innerFilter = (List<Object>) clauseBool.get("filter");
        assertEquals(
                Map.of("term", Map.of("study_id", "ccdi-int-study-1")),
                innerFilter.get(0));
        assertEquals(
                Map.of("terms", Map.of("participant_id", List.of("ccdi-int-p001", "ccdi-int-p002"))),
                innerFilter.get(1));
        InventoryESServiceTestSupport.assertJsonRoundTrip(query);
    }

    @Test
    @SuppressWarnings("unchecked")
    void participants_ageAtDiagnosis_openRangeLowerBoundOnly() throws IOException {
        Map<String, Object> params = new HashMap<>();
        params.put("age_at_diagnosis", Arrays.asList(10, null));

        Map<String, Object> query = buildFacetFilter(service, params, "participants");

        var nested = findNestedBlock(nonFilesTopLevelFilters(query), "sample_diagnosis_file_filters").orElseThrow();
        Map<String, Object> ageClause = (Map<String, Object>) nestedInnerFilters(nested).get(0);
        Map<String, Object> shouldBool = (Map<String, Object>) ageClause.get("bool");
        List<Object> should = (List<Object>) shouldBool.get("should");
        Map<String, Object> rangePart = (Map<String, Object>) should.get(0);
        Map<String, Object> rangeSpec = (Map<String, Object>) rangePart.get("range");
        assertEquals(Map.of("gte", 10), rangeSpec.get("sample_diagnosis_file_filters.age_at_diagnosis"));
        InventoryESServiceTestSupport.assertJsonRoundTrip(query);
    }

    @Test
    void participants_emptyFacetValue_skipsFilter() throws IOException {
        Map<String, Object> query = buildFacetFilter(service, Map.of("race", List.of("")), "participants");

        assertEquals(Map.of("match_all", Map.of()), query.get("query"));
    }
}
