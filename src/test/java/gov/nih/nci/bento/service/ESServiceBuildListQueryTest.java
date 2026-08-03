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
 * Phase 3: unit tests for {@link ESService} query builders (no live OpenSearch).
 */
class ESServiceBuildListQueryTest {

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
    void emptyParams_yieldsEmptyFilter() {
        Map<String, Object> query = service.buildListQuery(Map.of(), Set.of());

        assertEquals(Map.of("bool", Map.of("filter", List.of())), query.get("query"));
        ESServiceTestSupport.assertJsonRoundTrip(query);
    }

    @Test
    void ignoreCase_lowercasesTermValues() {
        Map<String, Object> query = service.buildListQuery(Map.of("race", List.of("Asian")), Set.of(), true);

        assertTrue(query.toString().contains("asian"));
        ESServiceTestSupport.assertJsonRoundTrip(query);
    }

    @Test
    void stringParam_coercedToTermsList() {
        Map<String, Object> query = service.buildListQuery(Map.of("study_id", "study-1"), Set.of());

        assertTrue(query.toString().contains("study-1"));
        ESServiceTestSupport.assertJsonRoundTrip(query);
    }

    @Test
    void singleEmptyString_skipsFilter() {
        Map<String, Object> query = service.buildListQuery(Map.of("race", List.of("")), Set.of());

        assertEquals(Map.of("bool", Map.of("filter", List.of())), query.get("query"));
    }

    @Test
    void excludedParam_isSkipped() {
        Map<String, Object> params = Map.of("race", List.of("Asian"), "first", 10);
        Map<String, Object> query = service.buildListQuery(params, Set.of("first"));

        assertTrue(query.toString().contains("race"));
        assertTrue(!query.toString().contains("first"));
    }
}
