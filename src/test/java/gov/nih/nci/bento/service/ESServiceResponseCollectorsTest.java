package gov.nih.nci.bento.service;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Phase 3: unit tests for {@link ESService} response parsing helpers.
 */
class ESServiceResponseCollectorsTest {

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
    void collectTermAggs_returnsNamedBucketArrays() {
        JsonObject response = ESServiceTestSupport.loadResponseFixture("terms_aggs.json");

        Map<String, JsonArray> aggs = service.collectTermAggs(response, new String[] {"study_status"});

        assertEquals(2, aggs.get("study_status").size());
    }

    @Test
    void collectTerms_extractsBucketKeys() {
        JsonObject response = ESServiceTestSupport.loadResponseFixture("terms_aggs.json");

        List<String> keys = service.collectTerms(response, "study_status");

        assertEquals(List.of("active", "completed"), keys);
    }

    @Test
    void collectRangeAggs_returnsStatsObjects() {
        JsonObject response = ESServiceTestSupport.loadResponseFixture("range_aggs.json");

        Map<String, JsonObject> aggs = service.collectRangeAggs(response, new String[] {"age_at_diagnosis"});

        assertTrue(aggs.get("age_at_diagnosis").has("count"));
    }

    @Test
    void collectBucketKeys_extractsKeysFromArray() {
        JsonArray buckets = ESServiceTestSupport.loadResponseFixture("terms_aggs.json")
                .getAsJsonObject("aggregations")
                .getAsJsonObject("study_status")
                .getAsJsonArray("buckets");

        List<String> keys = service.collectBucketKeys(buckets);

        assertEquals(List.of("active", "completed"), keys);
    }

    @Test
    void getTotalHits_readsNestedTotalValue() {
        JsonObject response = ESServiceTestSupport.loadResponseFixture("total_hits.json");

        assertEquals(123, service.getTotalHits(response));
    }

    @Test
    void collectPage_mapsScalarsAndNestedObjects() throws IOException {
        JsonObject response = ESServiceTestSupport.loadResponseFixture("search_hits_page.json");
        String[][] properties = {
            {"participant_id", "participant_id"},
            {"race", "race"},
            {"study", "study"}
        };

        List<Map<String, Object>> page = service.collectPage(response, properties, 10);

        assertEquals(2, page.size());
        assertEquals("ccdi-int-p001", page.get(0).get("participant_id"));
        assertInstanceOf(Map.class, page.get(0).get("study"));
    }

    @Test
    void addAggregations_setsSizeZeroAndTermsAggs() {
        Map<String, Object> query = Map.of("query", Map.of("match_all", Map.of()));
        Map<String, Object> withAggs = service.addAggregations(query, new String[] {"programs"});

        assertEquals(0, withAggs.get("size"));
        assertTrue(withAggs.toString().contains("aggregations"));
        assertTrue(withAggs.toString().contains("programs"));
    }

    @Test
    void addSubAggregations_nestsUnderMainAgg() {
        Map<String, Object> query = service.addAggregations(Map.of("query", Map.of("match_all", Map.of())),
                new String[] {"programs"});
        service.addSubAggregations(query, "programs", new String[] {"studies"});

        assertTrue(query.toString().contains("studies"));
    }
}
