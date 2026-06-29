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
import static gov.nih.nci.bento_ri.service.InventoryESServiceFacetFilterQueryTestSupport.nonFilesTopLevelFilters;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Core {@link InventoryESService#buildFacetFilterQuery} smoke tests (Phase 1 baseline).
 * See also Phase 2 classes: {@link InventoryESServiceBuildFacetFilterQueryParticipantsTest},
 * {@link InventoryESServiceBuildFacetFilterQueryUnknownAgesTest},
 * {@link InventoryESServiceBuildFacetFilterQueryFilesAndIndicesTest}.
 */
class InventoryESServiceBuildFacetFilterQueryTest {

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
    void participants_noFilters_yieldsMatchAll() throws IOException {
        Map<String, Object> query = buildFacetFilter(service, Map.of(), "participants");

        assertEquals(Map.of("match_all", Map.of()), query.get("query"));
        InventoryESServiceTestSupport.assertJsonRoundTrip(query);
    }

    @Test
    void participants_raceTerm_yieldsBoolFilterWithTopLevelTerms() throws IOException {
        Map<String, Object> params = Map.of("race", List.of("Asian"));
        Map<String, Object> query = buildFacetFilter(service, params, "participants");

        Map<String, Object> expected = Map.of(
                "bool",
                Map.of(
                        "filter",
                        List.of(Map.of("terms", Map.of("race", List.of("Asian"))))));
        assertEquals(expected, query.get("query"));
        InventoryESServiceTestSupport.assertJsonRoundTrip(query);
    }

    @Test
    @SuppressWarnings("unchecked")
    void participants_ageAtDiagnosisRange_defaultIncludesUnknownSentinel() throws IOException {
        Map<String, Object> params = Map.of("age_at_diagnosis", List.of(10, 20));
        Map<String, Object> query = buildFacetFilter(service, params, "participants");

        var nested = findNestedBlock(nonFilesTopLevelFilters(query), "sample_diagnosis_file_filters").orElseThrow();
        List<Object> innerFilter = nestedInnerFilters(nested);
        assertEquals(1, innerFilter.size());

        Map<String, Object> ageClause = (Map<String, Object>) innerFilter.get(0);
        List<Object> should = (List<Object>) ((Map<String, Object>) ageClause.get("bool")).get("should");
        assertEquals(2, should.size());

        Map<String, Object> rangeSpec = (Map<String, Object>) ((Map<String, Object>) should.get(0)).get("range");
        assertEquals(Map.of("gte", 10, "lte", 20), rangeSpec.get("sample_diagnosis_file_filters.age_at_diagnosis"));

        Map<String, Object> termPart = (Map<String, Object>) should.get(1);
        assertEquals(
                Map.of("sample_diagnosis_file_filters.age_at_diagnosis", -999),
                ((Map<?, ?>) termPart.get("term")));

        InventoryESServiceTestSupport.assertJsonRoundTrip(query);
    }

    @Test
    void filesOverall_noFacetFilters_yieldsMatchAll() throws IOException {
        Map<String, Object> query = buildFacetFilter(service, Map.of(), "files_overall");

        assertEquals(Map.of("match_all", Map.of()), query.get("query"));
        InventoryESServiceTestSupport.assertJsonRoundTrip(query);
    }

    @Test
    void files_noFacetFilters_requiresFileIdExists() throws IOException {
        Map<String, Object> query = buildFacetFilter(service, Map.of(), "files");

        Map<String, Object> expected = Map.of(
                "bool",
                Map.of("must", Map.of("exists", Map.of("field", "file_id"))));
        assertEquals(expected, query.get("query"));
        InventoryESServiceTestSupport.assertJsonRoundTrip(query);
    }

    @Test
    @SuppressWarnings("unchecked")
    void files_dataCategoryTerm_wrapsWithExistsFileIdAndNestedCombinedFilters() throws IOException {
        Map<String, Object> params = Map.of("data_category", List.of("Sequencing"));
        Map<String, Object> query = buildFacetFilter(service, params, "files");

        Map<String, Object> rootQuery = (Map<String, Object>) query.get("query");
        List<Object> should = (List<Object>) ((Map<String, Object>) rootQuery.get("bool")).get("should");
        assertEquals(1, should.size());

        Map<String, Object> boolBody = (Map<String, Object>) ((Map<String, Object>) should.get(0)).get("bool");
        assertEquals(Map.of("exists", Map.of("field", "file_id")), boolBody.get("must"));

        List<Object> filter = (List<Object>) boolBody.get("filter");
        assertEquals(2, filter.size());
        assertEquals(Map.of("terms", Map.of("data_category", List.of("Sequencing"))), filter.get(0));

        Map<String, Object> nestedBody = (Map<String, Object>) ((Map<String, Object>) filter.get(1)).get("nested");
        assertEquals("combined_filters", nestedBody.get("path"));
        assertTrue(nestedInnerFilters(nestedBody).isEmpty());

        InventoryESServiceTestSupport.assertJsonRoundTrip(query);
    }

    @Test
    void participants_rangeBothBoundsNull_throwsIOException() {
        Map<String, Object> params = new HashMap<>();
        params.put("age_at_diagnosis", Arrays.asList(null, null));

        assertThrows(IOException.class, () -> buildFacetFilter(service, params, "participants"));
    }
}
