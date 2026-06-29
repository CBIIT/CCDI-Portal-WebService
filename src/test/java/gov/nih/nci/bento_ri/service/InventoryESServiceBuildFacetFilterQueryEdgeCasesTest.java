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
import static gov.nih.nci.bento_ri.service.InventoryESServiceFacetFilterQueryTestSupport.nestedInnerFiltersForFilesCombinedPath;
import static gov.nih.nci.bento_ri.service.InventoryESServiceFacetFilterQueryTestSupport.nonFilesFiltersContainTerms;
import static gov.nih.nci.bento_ri.service.InventoryESServiceFacetFilterQueryTestSupport.nonFilesTopLevelFilters;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Edge-case branch coverage for {@link InventoryESService#buildFacetFilterQuery}.
 */
class InventoryESServiceBuildFacetFilterQueryEdgeCasesTest {

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
    void participants_rangeWithSingleValue_skipsRangeAndReturnsMatchAll() throws IOException {
        Map<String, Object> query = buildFacetFilter(service, Map.of("age_at_diagnosis", List.of(10)), "participants");

        assertEquals(Map.of("match_all", Map.of()), query.get("query"));
    }

    @Test
    void participants_emptyImportData_yieldsMatchAll() throws IOException {
        Map<String, Object> query = buildFacetFilter(service, Map.of("import_data", List.of("")), "participants");

        assertEquals(Map.of("match_all", Map.of()), query.get("query"));
    }

    @Test
    void files_emptyImportData_requiresFileIdExists() throws IOException {
        Map<String, Object> query = buildFacetFilter(service, Map.of("import_data", List.of("")), "files");

        assertEquals(
                Map.of("bool", Map.of("must", Map.of("exists", Map.of("field", "file_id")))),
                query.get("query"));
    }

    @Test
    void participants_unknownAgesEmptyString_keepsDefaultUnknownSentinelBehavior() throws IOException {
        Map<String, Object> params = new HashMap<>();
        params.put("age_at_diagnosis", List.of(10, 20));
        params.put("age_at_diagnosis_unknownAges", List.of(""));

        Map<String, Object> query = buildFacetFilter(service, params, "participants");
        var nested = findNestedBlock(nonFilesTopLevelFilters(query), "sample_diagnosis_file_filters").orElseThrow();
        assertTrue(nestedInnerFilters(nested).get(0).toString().contains("should"));
    }

    @Test
    void participants_participantAgeAtCollectionUnknownOnly_matchesSentinel() throws IOException {
        Map<String, Object> params = new HashMap<>();
        params.put("participant_age_at_collection", List.of(4, 8));
        params.put("participant_age_at_collection_unknownAges", List.of("only"));

        Map<String, Object> query = buildFacetFilter(service, params, "participants");
        var nested = findNestedBlock(nonFilesTopLevelFilters(query), "sample_diagnosis_file_filters").orElseThrow();
        assertTrue(nonFilesFiltersContainTerms(
                nestedInnerFilters(nested),
                "sample_diagnosis_file_filters.participant_age_at_collection",
                List.of(-999)));
    }

    @Test
    void participants_participantAgeAtCollectionUnknownExclude_addsMustNot() throws IOException {
        Map<String, Object> params = new HashMap<>();
        params.put("participant_age_at_collection", List.of(4, 8));
        params.put("participant_age_at_collection_unknownAges", List.of("exclude"));

        Map<String, Object> query = buildFacetFilter(service, params, "participants");
        var nested = findNestedBlock(nonFilesTopLevelFilters(query), "sample_diagnosis_file_filters").orElseThrow();
        assertTrue(nestedInnerFilters(nested).stream().anyMatch(clause -> clause.toString().contains("must_not")));
    }

    @Test
    void treatments_unknownAgesOnly_usesTopLevelSentinelTerms() throws IOException {
        Map<String, Object> params = new HashMap<>();
        params.put("age_at_treatment_start", List.of(5, 15));
        params.put("age_at_treatment_start_unknownAges", List.of("only"));

        Map<String, Object> query = buildFacetFilter(service, params, "treatments");
        assertTrue(nonFilesFiltersContainTerms(
                nonFilesTopLevelFilters(query), "age_at_treatment_start", List.of(-999)));
    }

    @Test
    void survivals_unknownAgesExclude_usesTopLevelMustNot() throws IOException {
        Map<String, Object> params = new HashMap<>();
        params.put("age_at_last_known_survival_status", List.of(10, 20));
        params.put("age_at_last_known_survival_status_unknownAges", List.of("exclude"));

        Map<String, Object> query = buildFacetFilter(service, params, "survivals");
        assertTrue(nonFilesTopLevelFilters(query).stream().anyMatch(clause -> clause.toString().contains("must_not")));
    }

    @Test
    void treatmentResponses_unknownAgesOnly_usesTopLevelSentinelTerms() throws IOException {
        Map<String, Object> params = new HashMap<>();
        params.put("age_at_response", List.of(2, 8));
        params.put("age_at_response_unknownAges", List.of("only"));

        Map<String, Object> query = buildFacetFilter(service, params, "treatment_responses");
        assertTrue(nonFilesFiltersContainTerms(nonFilesTopLevelFilters(query), "age_at_response", List.of(-999)));
    }

    @Test
    void samples_unknownAgesOnly_usesDiagnosisFiltersSentinel() throws IOException {
        Map<String, Object> params = new HashMap<>();
        params.put("age_at_diagnosis", List.of(6, 12));
        params.put("age_at_diagnosis_unknownAges", List.of("only"));

        Map<String, Object> query = buildFacetFilter(service, params, "samples");
        var nested = findNestedBlock(nonFilesTopLevelFilters(query), "diagnosis_filters").orElseThrow();
        assertTrue(nonFilesFiltersContainTerms(
                nestedInnerFilters(nested), "diagnosis_filters.age_at_diagnosis", List.of(-999)));
    }

    @Test
    void diagnosis_participantAgeAtCollectionUnknownOnly_usesSampleFileFiltersSentinel() throws IOException {
        Map<String, Object> params = new HashMap<>();
        params.put("participant_age_at_collection", List.of(3, 7));
        params.put("participant_age_at_collection_unknownAges", List.of("only"));

        Map<String, Object> query = buildFacetFilter(service, params, "diagnosis");
        var nested = findNestedBlock(nonFilesTopLevelFilters(query), "sample_file_filters").orElseThrow();
        assertTrue(nonFilesFiltersContainTerms(
                nestedInnerFilters(nested), "sample_file_filters.participant_age_at_collection", List.of(-999)));
    }

    @Test
    void samples_fileType_usesFileFiltersNested() throws IOException {
        Map<String, Object> query = buildFacetFilter(service, Map.of("file_type", List.of("BAM")), "samples");

        var nested = findNestedBlock(nonFilesTopLevelFilters(query), "file_filters").orElseThrow();
        assertTrue(nonFilesFiltersContainTerms(
                nestedInnerFilters(nested), "file_filters.file_type", List.of("BAM")));
    }

    @Test
    void files_fileType_usesTopLevelTermsNotCombinedFilters() throws IOException {
        Map<String, Object> query = buildFacetFilter(service, Map.of("file_type", List.of("BAM")), "files");

        assertTrue(filesIndexFilters(query).stream().anyMatch(clause -> clause.toString().contains("file_type")));
        InventoryESServiceTestSupport.assertJsonRoundTrip(query);
    }

    @Test
    void files_sexAtBirth_usesCombinedParticipantFilters() throws IOException {
        Map<String, Object> query = buildFacetFilter(service, Map.of("sex_at_birth", List.of("Male")), "files");

        var combined = findNestedInFilesCombinedFilters(filesIndexFilters(query)).orElseThrow();
        assertTrue(nonFilesFiltersContainTerms(
                combinedFiltersInnerFilters(combined), "combined_filters.sex_at_birth", List.of("Male")));
    }

    @Test
    void files_unknownAgesExclude_ageAtDiagnosis_usesCombinedSampleDiagnosisMustNot() throws IOException {
        Map<String, Object> params = new HashMap<>();
        params.put("age_at_diagnosis", List.of(7, 12));
        params.put("age_at_diagnosis_unknownAges", List.of("exclude"));

        Map<String, Object> query = buildFacetFilter(service, params, "files");
        List<Object> inner = nestedInnerFiltersForFilesCombinedPath(query, "combined_filters.sample_diagnosis_filters");
        assertTrue(inner.stream().anyMatch(clause -> clause.toString().contains("must_not")));
    }

    @Test
    void files_unknownAgesExclude_participantAgeAtCollection_usesCombinedSampleDiagnosisMustNot() throws IOException {
        Map<String, Object> params = new HashMap<>();
        params.put("participant_age_at_collection", List.of(5, 9));
        params.put("participant_age_at_collection_unknownAges", List.of("exclude"));

        Map<String, Object> query = buildFacetFilter(service, params, "files");
        List<Object> inner = nestedInnerFiltersForFilesCombinedPath(query, "combined_filters.sample_diagnosis_filters");
        assertTrue(inner.stream().anyMatch(clause -> clause.toString().contains("must_not")));
    }

    @Test
    void files_unknownAgesExclude_ageAtResponse_usesCombinedTreatmentResponseMustNot() throws IOException {
        Map<String, Object> params = new HashMap<>();
        params.put("age_at_response", List.of(1, 4));
        params.put("age_at_response_unknownAges", List.of("exclude"));

        Map<String, Object> query = buildFacetFilter(service, params, "files");
        List<Object> inner = nestedInnerFiltersForFilesCombinedPath(query, "combined_filters.treatment_response_filters");
        assertTrue(inner.stream().anyMatch(clause -> clause.toString().contains("must_not")));
    }

    @Test
    void files_unknownAgesExclude_ageAtLastKnownSurvivalStatus_usesCombinedSurvivalMustNot() throws IOException {
        Map<String, Object> params = new HashMap<>();
        params.put("age_at_last_known_survival_status", List.of(2, 6));
        params.put("age_at_last_known_survival_status_unknownAges", List.of("exclude"));

        Map<String, Object> query = buildFacetFilter(service, params, "files");
        List<Object> inner = nestedInnerFiltersForFilesCombinedPath(query, "combined_filters.survival_filters");
        assertTrue(inner.stream().anyMatch(clause -> clause.toString().contains("must_not")));
    }

    @Test
    void files_unknownAgesExclude_ageAtTreatmentStart_usesCombinedTreatmentMustNot() throws IOException {
        Map<String, Object> params = new HashMap<>();
        params.put("age_at_treatment_start", List.of(6, 10));
        params.put("age_at_treatment_start_unknownAges", List.of("exclude"));

        Map<String, Object> query = buildFacetFilter(service, params, "files");
        List<Object> inner = nestedInnerFiltersForFilesCombinedPath(query, "combined_filters.treatment_filters");
        assertTrue(inner.stream().anyMatch(clause -> clause.toString().contains("must_not")));
    }

    @Test
    void files_unknownAgesOnly_ageAtDiagnosis_matchesSentinelInCombinedSampleDiagnosis() throws IOException {
        Map<String, Object> params = new HashMap<>();
        params.put("age_at_diagnosis", List.of(7, 12));
        params.put("age_at_diagnosis_unknownAges", List.of("only"));

        Map<String, Object> query = buildFacetFilter(service, params, "files");
        List<Object> inner = nestedInnerFiltersForFilesCombinedPath(query, "combined_filters.sample_diagnosis_filters");
        assertTrue(inner.stream().anyMatch(clause -> clause.toString().contains("-999")));
    }

    @Test
    void files_unknownAgesOnly_participantAgeAtCollection_matchesSentinel() throws IOException {
        Map<String, Object> params = new HashMap<>();
        params.put("participant_age_at_collection", List.of(5, 9));
        params.put("participant_age_at_collection_unknownAges", List.of("only"));

        Map<String, Object> query = buildFacetFilter(service, params, "files");
        List<Object> inner = nestedInnerFiltersForFilesCombinedPath(query, "combined_filters.sample_diagnosis_filters");
        assertTrue(inner.stream().anyMatch(clause -> clause.toString().contains("-999")));
    }

    @Test
    void files_unknownAgesOnly_ageAtResponse_matchesSentinelInCombinedTreatmentResponse() throws IOException {
        Map<String, Object> params = new HashMap<>();
        params.put("age_at_response", List.of(1, 4));
        params.put("age_at_response_unknownAges", List.of("only"));

        Map<String, Object> query = buildFacetFilter(service, params, "files");
        List<Object> inner = nestedInnerFiltersForFilesCombinedPath(query, "combined_filters.treatment_response_filters");
        assertTrue(inner.stream().anyMatch(clause -> clause.toString().contains("-999")));
    }

    @Test
    void files_unknownAgesOnly_ageAtLastKnownSurvivalStatus_matchesSentinelInCombinedSurvival() throws IOException {
        Map<String, Object> params = new HashMap<>();
        params.put("age_at_last_known_survival_status", List.of(2, 6));
        params.put("age_at_last_known_survival_status_unknownAges", List.of("only"));

        Map<String, Object> query = buildFacetFilter(service, params, "files");
        List<Object> inner = nestedInnerFiltersForFilesCombinedPath(query, "combined_filters.survival_filters");
        assertTrue(inner.stream().anyMatch(clause -> clause.toString().contains("-999")));
    }

    @Test
    @SuppressWarnings("unchecked")
    void files_unknownAgesInclude_ageAtDiagnosis_usesPlainRangeWithoutSentinel() throws IOException {
        Map<String, Object> params = new HashMap<>();
        params.put("age_at_diagnosis", List.of(5, 15));
        params.put("age_at_diagnosis_unknownAges", List.of("include"));

        Map<String, Object> query = buildFacetFilter(service, params, "files");
        List<Object> inner = nestedInnerFiltersForFilesCombinedPath(query, "combined_filters.sample_diagnosis_filters");
        Map<String, Object> ageClause = (Map<String, Object>) inner.get(0);
        assertTrue(ageClause.containsKey("range"));
        assertTrue(inner.stream().noneMatch(clause -> clause.toString().contains("-999")));
    }

    @Test
    void files_rangeBothBoundsNull_throwsIOException() {
        Map<String, Object> params = new HashMap<>();
        params.put("age_at_diagnosis", Arrays.asList(null, null));

        assertThrows(IOException.class, () -> buildFacetFilter(service, params, "files"));
    }

    @Test
    @SuppressWarnings("unchecked")
    void participants_importDataMixedValidAndInvalid_includesValidEntryOnly() throws IOException {
        String valid =
                "{\"study_id\":\"ccdi-int-study-1\",\"participant_id\":[\"ccdi-int-p001\"]}";
        Map<String, Object> query = buildFacetFilter(
                service, Map.of("import_data", List.of("{bad-json", valid)), "participants");

        List<Object> filters = nonFilesTopLevelFilters(query);
        Map<String, Object> importBool = (Map<String, Object>) filters.get(0);
        List<Object> should = (List<Object>) ((Map<String, Object>) importBool.get("bool")).get("should");
        assertEquals(1, should.size());
    }

    @Test
    @SuppressWarnings("unchecked")
    void files_overall_withFilters_usesBoolFilterWithoutExistsMust() throws IOException {
        Map<String, Object> query = buildFacetFilter(
                service, Map.of("data_category", List.of("Sequencing")), "files_overall");

        Map<String, Object> rootQuery = (Map<String, Object>) query.get("query");
        List<Object> should = (List<Object>) ((Map<String, Object>) rootQuery.get("bool")).get("should");
        Map<String, Object> boolBody = (Map<String, Object>) ((Map<String, Object>) should.get(0)).get("bool");
        assertTrue(boolBody.containsKey("filter"));
        assertTrue(!boolBody.containsKey("must"));
    }
}
