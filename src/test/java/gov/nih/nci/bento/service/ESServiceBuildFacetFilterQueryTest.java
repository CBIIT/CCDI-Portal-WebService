package gov.nih.nci.bento.service;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Phase 3: unit tests for {@link ESService#buildFacetFilterQuery}.
 */
class ESServiceBuildFacetFilterQueryTest {

    private ESService service;

    @BeforeEach
    void setUp() {
        service = ESServiceTestSupport.newService();
    }

    @AfterEach
    void tearDown() throws Exception {
        ESServiceTestSupport.closeClient(service);
    }

    @Test
    void termParam_yieldsTermsFilter() throws IOException {
        Map<String, Object> query = service.buildFacetFilterQuery(Map.of("race", List.of("Asian")));

        assertTrue(query.toString().contains("race"));
        assertTrue(query.toString().contains("Asian"));
        ESServiceTestSupport.assertJsonRoundTrip(query);
    }

    @Test
    void rangeParam_yieldsRangeFilter() throws IOException {
        Map<String, Object> params = Map.of("age_at_index", List.of(10.0, 20.0));
        Map<String, Object> query = service.buildFacetFilterQuery(params, Set.of("age_at_index"));

        assertTrue(query.toString().contains("range"));
        assertTrue(query.toString().contains("age_at_index"));
        ESServiceTestSupport.assertJsonRoundTrip(query);
    }

    @Test
    void rangeBothBoundsNull_throwsIOException() {
        Map<String, Object> params = new HashMap<>();
        params.put("age_at_index", Arrays.asList(null, null));

        assertThrows(IOException.class,
                () -> service.buildFacetFilterQuery(params, Set.of("age_at_index")));
    }

    @Test
    void excludedParam_isOmitted() throws IOException {
        Map<String, Object> params = Map.of("race", List.of("Asian"), "offset", 5);
        Map<String, Object> query = service.buildFacetFilterQuery(params, Set.of(), Set.of("offset"));

        assertTrue(query.toString().contains("race"));
        assertTrue(!query.toString().contains("offset"));
    }
}
