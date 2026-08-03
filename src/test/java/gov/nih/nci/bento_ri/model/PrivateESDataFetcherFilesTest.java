package gov.nih.nci.bento_ri.model;

import gov.nih.nci.bento_ri.service.InventoryESService;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.client.Request;

import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anySet;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Unit tests for file search resolver on {@link PrivateESDataFetcher}.
 */
@ExtendWith(MockitoExtension.class)
class PrivateESDataFetcherFilesTest {

    @Test
    void getFilenames_returnsPagedFilesAndTotalCount() throws Exception {
        InventoryESService inventoryESService = mock(InventoryESService.class);
        when(inventoryESService.buildFacetFilterQuery(
                        any(), anySet(), anySet(), anySet(), anyString(), anyString()))
                .thenReturn(PrivateESDataFetcherTestSupport.mutableQuery());
        when(inventoryESService.send(any(Request.class)))
                .thenReturn(PrivateESDataFetcherTestSupport.loadFixture("filenames_total_hits.json"));
        when(inventoryESService.collectPage(
                        any(Request.class), any(), any(String[][].class), anyInt(), anyInt()))
                .thenReturn(List.of(Map.of("file_id", "f1", "file_name", "sample.bam")));

        PrivateESDataFetcher fetcher = PrivateESDataFetcherTestSupport.newFetcher(inventoryESService);
        Map<String, Object> params = Map.of(
                "filename", "bam",
                "first", 10,
                "offset", 0,
                "order_by", "file_name",
                "sort_direction", "asc");

        @SuppressWarnings("unchecked")
        Map<String, Object> result = (Map<String, Object>) PrivateESDataFetcherTestSupport.invoke(
                fetcher, "getFilenames", new Class[] {Map.class}, params);

        assertEquals(3, result.get("totalCount"));
        @SuppressWarnings("unchecked")
        List<Map<String, Object>> files = (List<Map<String, Object>>) result.get("files");
        assertEquals("sample.bam", files.get(0).get("file_name"));
    }
}
