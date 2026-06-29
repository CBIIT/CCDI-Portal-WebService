package gov.nih.nci.bento_ri.model;

import com.google.gson.JsonObject;
import gov.nih.nci.bento_ri.service.InventoryESService;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.client.Request;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Unit tests for global search helpers on {@link PrivateESDataFetcher}.
 */
@ExtendWith(MockitoExtension.class)
class PrivateESDataFetcherGlobalSearchTest {

    @Test
    void addHighlight_addsFieldsFromCategory() throws Exception {
        PrivateESDataFetcher fetcher = PrivateESDataFetcherTestSupport.newFetcher(null);
        Map<String, Object> query = Map.of("query", Map.of("match_all", Map.of()));
        Map<String, Object> category = Map.of("search_field", List.of("participant_id_gs", "study_id_gs"));

        @SuppressWarnings("unchecked")
        Map<String, Object> result = (Map<String, Object>) PrivateESDataFetcherTestSupport.invoke(
                fetcher,
                "addHighlight",
                new Class[] {Map.class, Map.class},
                query,
                category);

        @SuppressWarnings("unchecked")
        Map<String, Object> highlight = (Map<String, Object>) result.get("highlight");
        @SuppressWarnings("unchecked")
        Map<String, Object> fields = (Map<String, Object>) highlight.get("fields");
        assertTrue(fields.containsKey("participant_id_gs"));
        assertTrue(fields.containsKey("study_id_gs"));
    }

    @Test
    void getGlobalSearchQuery_subjectCategory_buildsShouldClauses() throws Exception {
        PrivateESDataFetcher fetcher = PrivateESDataFetcherTestSupport.newFetcher(null);
        Map<String, Object> category = Map.of(
                "search_field", List.of("participant_id_gs"),
                "category_type", "subject");

        @SuppressWarnings("unchecked")
        Map<String, Object> result = (Map<String, Object>) PrivateESDataFetcherTestSupport.invoke(
                fetcher,
                "getGlobalSearchQuery",
                new Class[] {String.class, Map.class},
                "ccdi",
                category);

        @SuppressWarnings("unchecked")
        Map<String, Object> bool = (Map<String, Object>) ((Map<String, Object>) result.get("query")).get("bool");
        assertNotNull(bool.get("should"));
    }

    @Test
    void getGlobalSearchQuery_fileCategory_requiresFileIdExists() throws Exception {
        PrivateESDataFetcher fetcher = PrivateESDataFetcherTestSupport.newFetcher(null);
        Map<String, Object> category = Map.of(
                "search_field", List.of("file_name_gs"),
                "category_type", "file");

        @SuppressWarnings("unchecked")
        Map<String, Object> result = (Map<String, Object>) PrivateESDataFetcherTestSupport.invoke(
                fetcher,
                "getGlobalSearchQuery",
                new Class[] {String.class, Map.class},
                "bam",
                category);

        @SuppressWarnings("unchecked")
        Map<String, Object> bool = (Map<String, Object>) ((Map<String, Object>) result.get("query")).get("bool");
        assertNotNull(bool.get("must"));
    }

    @Test
    void searchAboutPage_mapsHighlightedParagraphs() throws Exception {
        InventoryESService inventoryESService = mock(InventoryESService.class);
        when(inventoryESService.send(any(Request.class)))
                .thenReturn(PrivateESDataFetcherTestSupport.loadFixture("about_search_hits.json"));

        PrivateESDataFetcher fetcher = PrivateESDataFetcherTestSupport.newFetcher(inventoryESService);

        @SuppressWarnings("unchecked")
        List<Map<String, Object>> result = (List<Map<String, Object>>) PrivateESDataFetcherTestSupport.invoke(
                fetcher, "searchAboutPage", new Class[] {String.class}, "search term");

        assertEquals(1, result.size());
        assertEquals("about", result.get(0).get("page"));
        assertEquals("About CCDI", result.get(0).get("title"));
    }

    @Test
    void globalSearch_aggregatesCountsAndAboutResults() throws Exception {
        InventoryESService inventoryESService = mock(InventoryESService.class);
        JsonObject countResponse = PrivateESDataFetcherTestSupport.countResponse(2);
        JsonObject aboutResponse = PrivateESDataFetcherTestSupport.loadFixture("about_search_hits.json");
        when(inventoryESService.send(any(Request.class))).thenAnswer(invocation -> {
            Request request = invocation.getArgument(0);
            if (request.getEndpoint().contains("ccdi_hub_static_pages")) {
                return aboutResponse;
            }
            return countResponse;
        });
        when(inventoryESService.collectPage(
                        any(Request.class), any(), any(String[][].class), anyInt(), anyInt()))
                .thenReturn(new ArrayList<>(List.of(new HashMap<>(Map.of("participant_id", "p1")))));

        PrivateESDataFetcher fetcher = PrivateESDataFetcherTestSupport.newFetcher(inventoryESService);
        Map<String, Object> params = Map.of("input", "ccdi", "first", 10, "offset", 0);

        @SuppressWarnings("unchecked")
        Map<String, Object> result = (Map<String, Object>) PrivateESDataFetcherTestSupport.invoke(
                fetcher, "globalSearch", new Class[] {Map.class}, params);

        assertEquals(2, result.get("participant_count"));
        assertEquals(1, result.get("about_count"));
        assertNotNull(result.get("participants"));
    }
}
