package gov.nih.nci.bento_ri.service;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Unit tests for {@link InventoryESService#buildListQuery(Map, Set, boolean)}.
 */
class InventoryESServiceBuildListQueryTest {

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
    void emptyParams_yieldsEmptyFilter() {
        Map<String, Object> query = service.buildListQuery(Map.of(), Set.of());

        assertEquals(Map.of("bool", Map.of("filter", List.of())), query.get("query"));
        InventoryESServiceTestSupport.assertJsonRoundTrip(query);
    }

    @Test
    void singleTerm_addsTermsClause() {
        Map<String, Object> params = Map.of("race", List.of("Asian", "White"));
        Map<String, Object> query = service.buildListQuery(params, Set.of());

        Map<String, Object> expected = Map.of(
                "bool",
                Map.of(
                        "filter",
                        List.of(Map.of("terms", Map.of("race", List.of("Asian", "White"))))));
        assertEquals(expected, query.get("query"));
        InventoryESServiceTestSupport.assertJsonRoundTrip(query);
    }

    @Test
    void excludedParam_isSkipped() {
        Map<String, Object> params = new HashMap<>();
        params.put("race", List.of("Asian"));
        params.put("page_size", List.of("10"));

        Map<String, Object> query = service.buildListQuery(params, Set.of("page_size"));

        Map<String, Object> expected = Map.of(
                "bool",
                Map.of("filter", List.of(Map.of("terms", Map.of("race", List.of("Asian"))))));
        assertEquals(expected, query.get("query"));
    }

    @Test
    void singleEmptyString_meansAll_skipsThatParam() {
        Map<String, Object> params = Map.of("race", List.of(""));
        Map<String, Object> query = service.buildListQuery(params, Set.of());

        assertEquals(Map.of("bool", Map.of("filter", List.of())), query.get("query"));
    }

    @Test
    @SuppressWarnings("unchecked")
    void ignoreCase_lowercasesTermValues() {
        Map<String, Object> params = Map.of("race", List.of("Asian", "WHITE"));
        Map<String, Object> query = service.buildListQuery(params, Set.of(), true);

        Map<String, Object> bool = (Map<String, Object>) ((Map<String, Object>) query.get("query")).get("bool");
        List<Object> filter = (List<Object>) bool.get("filter");
        Map<String, Object> terms = (Map<String, Object>) ((Map<String, Object>) filter.get(0)).get("terms");
        assertEquals(List.of("asian", "white"), terms.get("race"));
        InventoryESServiceTestSupport.assertJsonRoundTrip(query);
    }

    @Test
    void integerParam_coercedToStringTerms() {
        Map<String, Object> params = Map.of("study_id", 42);
        Map<String, Object> query = service.buildListQuery(params, Set.of());

        Map<String, Object> expected = Map.of(
                "bool",
                Map.of("filter", List.of(Map.of("terms", Map.of("study_id", List.of("42"))))));
        assertEquals(expected, query.get("query"));
    }

    @Test
    @SuppressWarnings("unchecked")
    void stringParam_wrappedAsSingleValueList() {
        Map<String, Object> params = Map.of("study_status", "active");
        Map<String, Object> query = service.buildListQuery(params, Set.of());

        Map<String, Object> bool = (Map<String, Object>) ((Map<String, Object>) query.get("query")).get("bool");
        List<Object> filter = (List<Object>) bool.get("filter");
        Map<String, Object> terms = (Map<String, Object>) ((Map<String, Object>) filter.get(0)).get("terms");
        assertEquals(List.of("active"), terms.get("study_status"));
    }

    @Test
    @SuppressWarnings("unchecked")
    void multipleParams_producesMultipleFilterClauses() {
        Map<String, Object> params = Map.of(
                "race", List.of("Asian"),
                "sex_at_birth", List.of("Female"));
        Map<String, Object> query = service.buildListQuery(params, Set.of());

        Map<String, Object> bool = (Map<String, Object>) ((Map<String, Object>) query.get("query")).get("bool");
        List<Object> filter = (List<Object>) bool.get("filter");
        assertEquals(2, filter.size());
        assertTrue(filter.stream().anyMatch(clause -> clause.toString().contains("race")));
        assertTrue(filter.stream().anyMatch(clause -> clause.toString().contains("sex_at_birth")));
        InventoryESServiceTestSupport.assertJsonRoundTrip(query);
    }
}
