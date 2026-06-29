package gov.nih.nci.bento_ri.service;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.opensearch.client.Response;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Phase 3: unit tests for {@link InventoryESService} OpenSearch response parsing (no live cluster).
 */
class InventoryESServiceResponseCollectorsTest {

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
    void collectCustomTerms_flatBuckets_usesDocCount() {
        JsonObject response = InventoryESServiceTestSupport.loadResponseFixture("custom_terms_flat.json");

        Map<String, Integer> terms = service.collectCustomTerms(response, "facetAgg");

        assertEquals(2, terms.size());
        assertEquals(40919, terms.get("Active"));
        assertEquals(19703, terms.get("Completed"));
    }

    @Test
    void collectCustomTerms_nestedBuckets_usesReverseNestedDocCount() {
        JsonObject response = InventoryESServiceTestSupport.loadResponseFixture("custom_terms_nested.json");

        Map<String, Integer> terms = service.collectCustomTerms(response, "facetAgg");

        assertEquals(2, terms.size());
        assertEquals(6374, terms.get("Asian"));
        assertEquals(4100, terms.get("White"));
    }

    @Test
    void collectFileIDs_flattensFilesFromAllHits() {
        JsonObject response = InventoryESServiceTestSupport.loadResponseFixture("file_ids_hits.json");

        List<String> fileIds = service.collectFileIDs(response);

        assertEquals(List.of("ccdi-int-f001", "ccdi-int-f002", "ccdi-int-f003"), fileIds);
    }

    @Test
    void collectTerms_extractsBucketKeys() {
        JsonObject response = InventoryESServiceTestSupport.loadResponseFixture("terms_aggs.json");

        List<String> keys = service.collectTerms(response, "study_status");

        assertEquals(List.of("active", "completed"), keys);
    }

    @Test
    void collectNodeCountAggs_returnsBucketsArray() {
        JsonObject response = InventoryESServiceTestSupport.loadResponseFixture("node_count_aggs.json");

        Map<String, JsonArray> aggs = service.collectNodeCountAggs(response, "race");

        assertEquals(2, aggs.get("race").size());
        assertEquals("Asian", aggs.get("race").get(0).getAsJsonObject().get("key").getAsString());
    }

    @Test
    void collectRangCountAggs_returnsRangeBuckets() {
        JsonObject response = InventoryESServiceTestSupport.loadResponseFixture("range_count_aggs.json");

        Map<String, JsonArray> aggs = service.collectRangCountAggs(response, "age_at_diagnosis");

        assertEquals(2, aggs.get("age_at_diagnosis").size());
        assertEquals("0 - 4", aggs.get("age_at_diagnosis").get(0).getAsJsonObject().get("key").getAsString());
    }

    @Test
    void collectRangAggs_returnsInnerRangeStats() {
        JsonObject response = InventoryESServiceTestSupport.loadResponseFixture("range_stats_aggs.json");

        Map<String, JsonObject> aggs = service.collectRangAggs(response, "age_at_diagnosis");

        JsonObject stats = aggs.get("age_at_diagnosis");
        assertEquals(100, stats.get("count").getAsInt());
        assertEquals(5.0, stats.get("min").getAsDouble());
        assertEquals(65.0, stats.get("max").getAsDouble());
    }

    @Test
    void collectTermAggs_returnsNamedBucketArrays() {
        JsonObject response = InventoryESServiceTestSupport.loadResponseFixture("node_count_aggs.json");

        Map<String, JsonArray> aggs = service.collectTermAggs(response, new String[] {"race"});

        assertEquals(2, aggs.get("race").size());
    }

    @Test
    void collectRangeAggs_returnsAggregationObjects() {
        JsonObject response = InventoryESServiceTestSupport.loadResponseFixture("range_count_aggs.json");

        Map<String, JsonObject> aggs = service.collectRangeAggs(response, new String[] {"age_at_diagnosis"});

        assertTrue(aggs.get("age_at_diagnosis").has("buckets"));
    }

    @Test
    void collectPage_mapsScalarsAndNestedObjects() throws IOException {
        JsonObject response = InventoryESServiceTestSupport.loadResponseFixture("search_hits_page.json");
        String[][] properties = {
            {"participant_id", "participant_id"},
            {"race", "race"},
            {"study", "study"}
        };

        List<Map<String, Object>> page = service.collectPage(response, properties, 10);

        assertEquals(2, page.size());
        assertEquals("ccdi-int-p001", page.get(0).get("participant_id"));
        assertEquals("Asian", page.get(0).get("race"));
        Object study = page.get(0).get("study");
        assertInstanceOf(Map.class, study);
        @SuppressWarnings("unchecked")
        Map<String, Object> studyMap = (Map<String, Object>) study;
        assertEquals("ccdi-int-study-1", studyMap.get("study_id"));
        assertEquals("CCDI Integration Fixture Study", studyMap.get("study_name"));
    }

    @Test
    void collectPage_respectsPageSizeAndOffset() throws IOException {
        JsonObject response = InventoryESServiceTestSupport.loadResponseFixture("search_hits_page.json");
        String[][] properties = {{"participant_id", "participant_id"}};

        List<Map<String, Object>> page = service.collectPage(response, properties, null, 1, 1);

        assertEquals(1, page.size());
        assertEquals("ccdi-int-p002", page.get(0).get("participant_id"));
    }

    @Test
    void getJSonFromResponse_parsesEntityBody() throws IOException {
        String body = "{\"count\":42}";
        Response response = mock(Response.class);
        when(response.getEntity()).thenReturn(new StringEntity(body, ContentType.APPLICATION_JSON));

        JsonObject parsed = service.getJSonFromResponse(response);

        assertNotNull(parsed);
        assertEquals(42, parsed.get("count").getAsInt());
    }
}
