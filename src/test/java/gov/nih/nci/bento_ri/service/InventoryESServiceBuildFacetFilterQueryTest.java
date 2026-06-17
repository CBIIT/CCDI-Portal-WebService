package gov.nih.nci.bento_ri.service;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonElement;
import com.google.gson.JsonParser;
import gov.nih.nci.bento.model.ConfigurationDAO;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.opensearch.client.RestClient;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Unit tests for {@link InventoryESService#buildFacetFilterQuery(Map, Set, Set, Set, String, String)}.
 * <p>
 * The method materializes OpenSearch query DSL as nested {@code Map} structures (serialized with Gson
 * elsewhere when calling ES). Tests lock expected clause shapes for representative filters and ensure JSON
 * produced from those maps is parseable (same path as production {@code gson.toJson(query)}).
 */
class InventoryESServiceBuildFacetFilterQueryTest {

    private static final Set<String> RANGE_PARAMS = Set.of(
            "age_at_diagnosis",
            "participant_age_at_collection",
            "age_at_treatment_start",
            "age_at_response",
            "age_at_last_known_survival_status");

    private static final String NESTED_FILTERS = "nested_filters";

    private final Gson gson = new GsonBuilder().serializeNulls().create();

    private InventoryESService service;

    @BeforeEach
    void setUp() throws Exception {
        ConfigurationDAO config = Mockito.mock(ConfigurationDAO.class);
        Mockito.when(config.isEsSignRequests()).thenReturn(false);
        Mockito.when(config.getEsHost()).thenReturn("localhost");
        Mockito.when(config.getEsPort()).thenReturn(9200);
        Mockito.when(config.getEsScheme()).thenReturn("http");

        Constructor<InventoryESService> ctor = InventoryESService.class.getDeclaredConstructor(ConfigurationDAO.class);
        ctor.setAccessible(true);
        service = ctor.newInstance(config);
    }

    @AfterEach
    void tearDown() throws Exception {
        Field clientField = InventoryESService.class.getDeclaredField("client");
        clientField.setAccessible(true);
        RestClient client = (RestClient) clientField.get(service);
        if (client != null) {
            client.close();
        }
    }

    @Test
    void participants_noFilters_yieldsMatchAll() throws IOException {
        Map<String, Object> query = service.buildFacetFilterQuery(
                Map.of(),
                RANGE_PARAMS,
                Set.of(),
                Set.of(),
                NESTED_FILTERS,
                "participants");

        assertEquals(Map.of("match_all", Map.of()), query.get("query"));
        assertJsonRoundTrip(query);
    }

    @Test
    void participants_raceTerm_yieldsBoolFilterWithTopLevelTerms() throws IOException {
        Map<String, Object> params = Map.of("race", List.of("Asian"));
        Map<String, Object> query = service.buildFacetFilterQuery(
                params,
                RANGE_PARAMS,
                Set.of(),
                Set.of(),
                NESTED_FILTERS,
                "participants");

        Map<String, Object> expected = Map.of(
                "bool",
                Map.of(
                        "filter",
                        List.of(Map.of("terms", Map.of("race", List.of("Asian"))))));

        assertEquals(expected, query.get("query"));
        assertJsonRoundTrip(query);
    }

    @Test
    @SuppressWarnings("unchecked")
    void participants_ageAtDiagnosisRange_defaultIncludesUnknownSentinel() throws IOException {
        Map<String, Object> params = Map.of("age_at_diagnosis", List.of(10, 20));
        Map<String, Object> query = service.buildFacetFilterQuery(
                params,
                RANGE_PARAMS,
                Set.of(),
                Set.of(),
                NESTED_FILTERS,
                "participants");

        Map<String, Object> queryBody = (Map<String, Object>) query.get("query");
        Map<String, Object> boolClause = (Map<String, Object>) queryBody.get("bool");
        List<Object> filter = (List<Object>) boolClause.get("filter");

        assertEquals(1, filter.size(), "single nested block for sample_diagnosis_file_filters on participants index");

        Map<String, Object> diagnosisNested = (Map<String, Object>) filter.get(0);
        assertEquals("nested", diagnosisNested.keySet().iterator().next());
        Map<String, Object> nestedBody = (Map<String, Object>) diagnosisNested.get("nested");
        assertEquals("sample_diagnosis_file_filters", nestedBody.get("path"));

        Map<String, Object> innerBool = (Map<String, Object>) ((Map<String, Object>) nestedBody.get("query")).get("bool");
        List<Object> innerFilter = (List<Object>) innerBool.get("filter");
        assertEquals(1, innerFilter.size());

        Map<String, Object> ageClause = (Map<String, Object>) innerFilter.get(0);
        Map<String, Object> shouldBool = (Map<String, Object>) ageClause.get("bool");
        List<Object> should = (List<Object>) shouldBool.get("should");
        assertEquals(2, should.size());

        Map<String, Object> rangePart = (Map<String, Object>) should.get(0);
        assertTrue(rangePart.containsKey("range"));
        Map<String, Object> rangeSpec = (Map<String, Object>) rangePart.get("range");
        assertEquals(Map.of("gte", 10, "lte", 20), rangeSpec.get("sample_diagnosis_file_filters.age_at_diagnosis"));

        Map<String, Object> termPart = (Map<String, Object>) should.get(1);
        assertEquals(
                Map.of("sample_diagnosis_file_filters.age_at_diagnosis", -999),
                ((Map<?, ?>) termPart.get("term")));

        assertJsonRoundTrip(query);
    }

    @Test
    void filesOverall_noFacetFilters_yieldsMatchAll() throws IOException {
        Map<String, Object> query = service.buildFacetFilterQuery(
                Map.of(),
                RANGE_PARAMS,
                Set.of(),
                Set.of(),
                NESTED_FILTERS,
                "files_overall");

        assertEquals(Map.of("match_all", Map.of()), query.get("query"));
        assertJsonRoundTrip(query);
    }

    @Test
    void files_noFacetFilters_requiresFileIdExists() throws IOException {
        Map<String, Object> query = service.buildFacetFilterQuery(
                Map.of(),
                RANGE_PARAMS,
                Set.of(),
                Set.of(),
                NESTED_FILTERS,
                "files");

        Map<String, Object> expected = Map.of(
                "bool",
                Map.of("must", Map.of("exists", Map.of("field", "file_id"))));
        assertEquals(expected, query.get("query"));
        assertJsonRoundTrip(query);
    }

    @Test
    @SuppressWarnings("unchecked")
    void files_dataCategoryTerm_wrapsWithExistsFileIdAndNestedCombinedFilters() throws IOException {
        Map<String, Object> params = Map.of("data_category", List.of("Sequencing"));
        Map<String, Object> query = service.buildFacetFilterQuery(
                params,
                RANGE_PARAMS,
                Set.of(),
                Set.of(),
                NESTED_FILTERS,
                "files");

        Map<String, Object> rootQuery = (Map<String, Object>) query.get("query");
        List<Object> should = (List<Object>) ((Map<String, Object>) rootQuery.get("bool")).get("should");
        assertEquals(1, should.size());

        Map<String, Object> outerBool = (Map<String, Object>) should.get(0);
        Map<String, Object> boolBody = (Map<String, Object>) outerBool.get("bool");
        assertEquals(Map.of("exists", Map.of("field", "file_id")), boolBody.get("must"));

        List<Object> filter = (List<Object>) boolBody.get("filter");
        assertEquals(2, filter.size());

        assertEquals(Map.of("terms", Map.of("data_category", List.of("Sequencing"))), filter.get(0));

        Map<String, Object> nested = (Map<String, Object>) filter.get(1);
        assertEquals("nested", nested.keySet().iterator().next());
        Map<String, Object> nestedBody = (Map<String, Object>) nested.get("nested");
        assertEquals("combined_filters", nestedBody.get("path"));
        Map<String, Object> nestedQuery = (Map<String, Object>) nestedBody.get("query");
        Map<String, Object> innerBool = (Map<String, Object>) nestedQuery.get("bool");
        List<Object> innerFilter = (List<Object>) innerBool.get("filter");
        assertTrue(innerFilter.isEmpty());

        assertJsonRoundTrip(query);
    }

    @Test
    void participants_rangeBothBoundsNull_throwsIOException() {
        Map<String, Object> params = new HashMap<>();
        params.put("age_at_diagnosis", Arrays.asList(null, null));

        assertThrows(IOException.class, () -> service.buildFacetFilterQuery(
                params,
                RANGE_PARAMS,
                Set.of(),
                Set.of(),
                NESTED_FILTERS,
                "participants"));
    }

    /**
     * Mirrors production serialization of search bodies; invalid nested structures still parse as JSON but
     * this catches gross serialization failures.
     */
    private void assertJsonRoundTrip(Map<String, Object> body) {
        String json = gson.toJson(body);
        JsonElement tree = JsonParser.parseString(json);
        assertTrue(tree.isJsonObject());
    }
}
