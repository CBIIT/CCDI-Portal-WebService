package gov.nih.nci.bento_ri.model;

import gov.nih.nci.bento_ri.service.InventoryESService;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.client.Request;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anySet;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Phase 4: unit tests for entity overview resolvers on {@link PrivateESDataFetcher}.
 */
@ExtendWith(MockitoExtension.class)
class PrivateESDataFetcherOverviewsTest {

    @Test
    void diagnosisOverview_collectsPagedDiagnosisRecords() throws Exception {
        InventoryESService inventoryESService = mock(InventoryESService.class);
        when(inventoryESService.buildFacetFilterQuery(
                        any(), anySet(), anySet(), anySet(), any(), eq("diagnosis")))
                .thenReturn(PrivateESDataFetcherTestSupport.mutableQuery());
        List<Map<String, Object>> page = List.of(
                Map.of("diagnosis_id", "dx-001", "participant_id", "p1", "diagnosis", "Leukemia"));
        when(inventoryESService.collectPage(
                        any(Request.class), any(), any(String[][].class), anyInt(), anyInt()))
                .thenReturn(page);

        PrivateESDataFetcher fetcher = PrivateESDataFetcherTestSupport.newFetcher(inventoryESService);
        Map<String, Object> params = Map.of(
                "first", 5,
                "offset", 0,
                "order_by", "diagnosis_id",
                "sort_direction", "asc");

        @SuppressWarnings("unchecked")
        List<Map<String, Object>> result = (List<Map<String, Object>>) PrivateESDataFetcherTestSupport.invoke(
                fetcher, "diagnosisOverview", new Class[] {Map.class}, params);

        assertEquals(1, result.size());
        assertEquals("dx-001", result.get(0).get("diagnosis_id"));
        verify(inventoryESService)
                .collectPage(any(Request.class), any(), any(String[][].class), eq(5), eq(0));
    }

    @Test
    void sampleOverview_collectsPagedSampleRecords() throws Exception {
        InventoryESService inventoryESService = mock(InventoryESService.class);
        when(inventoryESService.buildFacetFilterQuery(
                        any(), anySet(), anySet(), anySet(), any(), eq("samples")))
                .thenReturn(PrivateESDataFetcherTestSupport.mutableQuery());
        List<Map<String, Object>> page = List.of(
                Map.of("sample_id", "smp-001", "participant_id", "p1", "study_id", "STUDY-A"));
        when(inventoryESService.collectPage(
                        any(Request.class), any(), any(String[][].class), anyInt(), anyInt()))
                .thenReturn(page);

        PrivateESDataFetcher fetcher = PrivateESDataFetcherTestSupport.newFetcher(inventoryESService);
        Map<String, Object> params = Map.of(
                "first", 10,
                "offset", 0,
                "order_by", "sample_id",
                "sort_direction", "desc");

        @SuppressWarnings("unchecked")
        List<Map<String, Object>> result = (List<Map<String, Object>>) PrivateESDataFetcherTestSupport.invoke(
                fetcher, "sampleOverview", new Class[] {Map.class}, params);

        assertEquals("smp-001", result.get(0).get("sample_id"));
    }

    @Test
    void fileOverview_collectsPagedFileRecords() throws Exception {
        InventoryESService inventoryESService = mock(InventoryESService.class);
        when(inventoryESService.buildFacetFilterQuery(
                        any(), anySet(), anySet(), anySet(), any(), eq("files")))
                .thenReturn(PrivateESDataFetcherTestSupport.mutableQuery());
        List<Map<String, Object>> page = List.of(
                Map.of("file_id", "file-001", "file_name", "sample.bam", "data_category", "Genomics"));
        when(inventoryESService.collectPage(
                        any(Request.class), any(), any(String[][].class), anyInt(), anyInt()))
                .thenReturn(page);

        PrivateESDataFetcher fetcher = PrivateESDataFetcherTestSupport.newFetcher(inventoryESService);
        Map<String, Object> params = Map.of(
                "first", 10,
                "offset", 0,
                "order_by", "file_name",
                "sort_direction", "asc");

        @SuppressWarnings("unchecked")
        List<Map<String, Object>> result = (List<Map<String, Object>>) PrivateESDataFetcherTestSupport.invoke(
                fetcher, "fileOverview", new Class[] {Map.class}, params);

        assertEquals("file-001", result.get(0).get("file_id"));
    }
}
