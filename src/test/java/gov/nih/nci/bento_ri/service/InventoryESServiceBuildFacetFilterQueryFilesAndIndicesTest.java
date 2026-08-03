package gov.nih.nci.bento_ri.service;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
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
 * Phase 2: {@link InventoryESService#buildFacetFilterQuery} for files index and other entity indices.
 */
class InventoryESServiceBuildFacetFilterQueryFilesAndIndicesTest {

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
    void files_race_usesCombinedFiltersParticipantTerms() throws IOException {
        Map<String, Object> query = buildFacetFilter(service, Map.of("race", List.of("Asian")), "files");

        var combined = findNestedInFilesCombinedFilters(filesIndexFilters(query)).orElseThrow();
        assertTrue(nonFilesFiltersContainTerms(
                combinedFiltersInnerFilters(combined), "combined_filters.race", List.of("Asian")));
        InventoryESServiceTestSupport.assertJsonRoundTrip(query);
    }

    @Test
    @SuppressWarnings("unchecked")
    void files_survivalStatus_usesCombinedSurvivalFiltersNested() throws IOException {
        Map<String, Object> query = buildFacetFilter(
                service, Map.of("last_known_survival_status", List.of("Alive")), "files");

        var combined = findNestedInFilesCombinedFilters(filesIndexFilters(query)).orElseThrow();
        List<Object> inner = combinedFiltersInnerFilters(combined);
        assertTrue(inner.stream().anyMatch(clause -> {
            Map<String, Object> map = (Map<String, Object>) clause;
            if (!map.containsKey("nested")) {
                return false;
            }
            return "combined_filters.survival_filters"
                    .equals(((Map<String, Object>) map.get("nested")).get("path"));
        }));
        InventoryESServiceTestSupport.assertJsonRoundTrip(query);
    }

    @Test
    void samples_diagnosisTerm_usesDiagnosisFiltersNested() throws IOException {
        Map<String, Object> query = buildFacetFilter(
                service, Map.of("diagnosis", List.of("Cancer")), "samples");

        var nested = findNestedBlock(nonFilesTopLevelFilters(query), "diagnosis_filters");
        assertTrue(nested.isPresent());
        assertTrue(nonFilesFiltersContainTerms(
                nestedInnerFilters(nested.get()), "diagnosis_filters.diagnosis", List.of("Cancer")));
        InventoryESServiceTestSupport.assertJsonRoundTrip(query);
    }

    @Test
    void treatments_ageAtTreatmentStart_usesTopLevelRange() throws IOException {
        Map<String, Object> query = buildFacetFilter(
                service, Map.of("age_at_treatment_start", List.of(8, 12)), "treatments");

        List<Object> filters = nonFilesTopLevelFilters(query);
        assertEquals(1, filters.size());
        assertTrue(filters.get(0).toString().contains("age_at_treatment_start"));
        assertTrue(nonFilesTopLevelFilters(query).stream()
                .noneMatch(f -> f.toString().contains("treatment_filters")));
        InventoryESServiceTestSupport.assertJsonRoundTrip(query);
    }

    @Test
    void treatments_treatmentType_usesTopLevelTerms() throws IOException {
        Map<String, Object> query = buildFacetFilter(
                service, Map.of("treatment_type", List.of("Chemotherapy")), "treatments");

        assertTrue(nonFilesFiltersContainTerms(
                nonFilesTopLevelFilters(query), "treatment_type", List.of("Chemotherapy")));
        InventoryESServiceTestSupport.assertJsonRoundTrip(query);
    }

    @Test
    void survivals_lastKnownSurvivalStatus_usesTopLevelTerms() throws IOException {
        Map<String, Object> query = buildFacetFilter(
                service, Map.of("last_known_survival_status", List.of("Alive")), "survivals");

        assertTrue(nonFilesFiltersContainTerms(
                nonFilesTopLevelFilters(query), "last_known_survival_status", List.of("Alive")));
        InventoryESServiceTestSupport.assertJsonRoundTrip(query);
    }

    @Test
    void diagnosis_participantAgeAtCollection_usesSampleFileFiltersNested() throws IOException {
        Map<String, Object> query = buildFacetFilter(
                service, Map.of("participant_age_at_collection", List.of(5, 10)), "diagnosis");

        var nested = findNestedBlock(nonFilesTopLevelFilters(query), "sample_file_filters");
        assertTrue(nested.isPresent());
        InventoryESServiceTestSupport.assertJsonRoundTrip(query);
    }
}
