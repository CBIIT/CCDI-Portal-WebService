package gov.nih.nci.bento_ri.model;

import gov.nih.nci.bento_ri.model.FormattedCPIResponse.CPIDataItem;
import gov.nih.nci.bento_ri.service.CPIFetcherService;
import gov.nih.nci.bento_ri.service.InventoryESService;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.client.Request;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anySet;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Phase 4: unit tests for CPI enrichment helpers on {@link PrivateESDataFetcher}.
 */
@ExtendWith(MockitoExtension.class)
class PrivateESDataFetcherCpiEnrichmentTest {

    @Test
    void executeBatchQuery_mapsParticipantHits() throws Exception {
        InventoryESService inventoryESService = mock(InventoryESService.class);
        when(inventoryESService.send(any(Request.class)))
                .thenReturn(PrivateESDataFetcherTestSupport.loadFixture("cpi_batch_hits.json"));

        PrivateESDataFetcher fetcher = PrivateESDataFetcherTestSupport.newFetcher(inventoryESService);
        Map<String, Set<String>> studyToParticipants = Map.of("CCDI-STUDY", Set.of("assoc-p001"));

        @SuppressWarnings("unchecked")
        List<Map<String, Object>> results = (List<Map<String, Object>>) PrivateESDataFetcherTestSupport.invoke(
                fetcher, "executeBatchQuery", new Class[] {Map.class}, studyToParticipants);

        assertEquals(1, results.size());
        assertEquals("internal-pid-1", results.get(0).get("id"));
        assertEquals("assoc-p001", results.get(0).get("participant_id"));
        assertEquals("CCDI-STUDY", results.get(0).get("study_id"));
    }

    @Test
    void enrichCPIDataWithParticipantInfo_marksInternalAndExternalRecords() throws Exception {
        InventoryESService inventoryESService = mock(InventoryESService.class);
        when(inventoryESService.send(any(Request.class)))
                .thenReturn(PrivateESDataFetcherTestSupport.loadFixture("cpi_batch_hits.json"));

        PrivateESDataFetcher fetcher = PrivateESDataFetcherTestSupport.newFetcher(inventoryESService);
        List<FormattedCPIResponse> cpiData = List.of(
                PrivateESDataFetcherTestSupport.cpiResponseWithMapItems(
                        "p1",
                        "CCDI-STUDY",
                        Map.of(
                                "associated_id", "assoc-p001",
                                "repository_of_synonym_id", "CCDI-STUDY")),
                PrivateESDataFetcherTestSupport.cpiResponseWithMapItems(
                        "p2",
                        "OTHER-STUDY",
                        Map.of(
                                "associated_id", "external-p001",
                                "repository_of_synonym_id", "OTHER-STUDY")));

        PrivateESDataFetcherTestSupport.invoke(
                fetcher, "enrichCPIDataWithParticipantInfo", new Class[] {List.class}, cpiData);

        @SuppressWarnings("unchecked")
        List<Map<String, Object>> internalItems = (List<Map<String, Object>>) PrivateESDataFetcherTestSupport.invoke(
                fetcher, "getFieldValue", new Class[] {FormattedCPIResponse.class, String.class},
                cpiData.get(0),
                "cpiData");
        assertEquals("internal-pid-1", internalItems.get(0).get("p_id"));
        assertEquals("internal", internalItems.get(0).get("data_type"));

        @SuppressWarnings("unchecked")
        List<Map<String, Object>> externalItems = (List<Map<String, Object>>) PrivateESDataFetcherTestSupport.invoke(
                fetcher, "getFieldValue", new Class[] {FormattedCPIResponse.class, String.class},
                cpiData.get(1),
                "cpiData");
        assertNull(externalItems.get(0).get("p_id"));
        assertEquals("external", externalItems.get(0).get("data_type"));
    }

    @Test
    void enrichCpiDataWithBatchResults_enrichesMatchingAssociatedIds() throws Exception {
        PrivateESDataFetcher fetcher = PrivateESDataFetcherTestSupport.newFetcher(null);
        FormattedCPIResponse record = PrivateESDataFetcherTestSupport.cpiResponseWithMapItems(
                "p1",
                "CCDI-STUDY",
                Map.of("associated_id", "assoc-p001", "repository_of_synonym_id", "CCDI-STUDY"));
        List<Map<String, Object>> batchResults = List.of(Map.of(
                "participant_id", "assoc-p001",
                "study_id", "CCDI-STUDY",
                "id", "internal-pid-1"));

        PrivateESDataFetcherTestSupport.invoke(
                fetcher,
                "enrichCpiDataWithBatchResults",
                new Class[] {List.class, List.class},
                List.of(record),
                batchResults);

        @SuppressWarnings("unchecked")
        List<Map<String, Object>> enrichedItems = (List<Map<String, Object>>) PrivateESDataFetcherTestSupport.invoke(
                fetcher, "getFieldValue", new Class[] {FormattedCPIResponse.class, String.class},
                record,
                "cpiData");
        assertEquals("internal-pid-1", enrichedItems.get(0).get("p_id"));
        assertEquals("internal", enrichedItems.get(0).get("data_type"));
    }

    @Test
    void updateParticipantListWithEnrichedCPIData_copiesCpiArrayOntoParticipants() throws Exception {
        PrivateESDataFetcher fetcher = PrivateESDataFetcherTestSupport.newFetcher(null);
        List<Map<String, Object>> participants = new ArrayList<>();
        participants.add(new HashMap<>(Map.of("participant_id", "p1", "study_id", "CCDI-STUDY")));
        List<CPIDataItem> cpiItems = List.of(
                new CPIDataItem("assoc-p001", "CCDI-STUDY", "desc", "Research", "s3://bucket"));
        FormattedCPIResponse enriched = new FormattedCPIResponse("p1", "CCDI-STUDY", cpiItems);

        PrivateESDataFetcherTestSupport.invoke(
                fetcher,
                "updateParticipantListWithEnrichedCPIData",
                new Class[] {List.class, List.class},
                participants,
                List.of(enriched));

        assertNotNull(participants.get(0).get("cpi_data"));
        assertEquals(cpiItems, participants.get(0).get("cpi_data"));
    }

    @Test
    void participantOverview_enrichesParticipantsWhenCpiServicePresent() throws Exception {
        InventoryESService inventoryESService = mock(InventoryESService.class);
        CPIFetcherService cpiFetcherService = mock(CPIFetcherService.class);
        when(inventoryESService.buildFacetFilterQuery(
                        any(), anySet(), anySet(), anySet(), any(), eq("participants")))
                .thenReturn(PrivateESDataFetcherTestSupport.mutableQuery());
        List<Map<String, Object>> page = List.of(
                new HashMap<>(Map.of("participant_id", "p1", "study_id", "CCDI-STUDY")));
        when(inventoryESService.collectPage(
                        any(Request.class), any(), any(String[][].class), anyInt(), anyInt()))
                .thenReturn(page);
        FormattedCPIResponse cpiResponse = PrivateESDataFetcherTestSupport.cpiResponseWithMapItems(
                "p1",
                "CCDI-STUDY",
                Map.of("associated_id", "assoc-p001", "repository_of_synonym_id", "CCDI-STUDY"));
        when(cpiFetcherService.fetchAssociatedParticipantIds(anyList())).thenReturn(List.of(cpiResponse));
        when(inventoryESService.send(any(Request.class)))
                .thenReturn(PrivateESDataFetcherTestSupport.loadFixture("cpi_batch_hits.json"));

        PrivateESDataFetcher fetcher =
                PrivateESDataFetcherTestSupport.newFetcher(inventoryESService, cpiFetcherService);

        @SuppressWarnings("unchecked")
        List<Map<String, Object>> result = (List<Map<String, Object>>) PrivateESDataFetcherTestSupport.invoke(
                fetcher,
                "participantOverview",
                new Class[] {Map.class},
                PrivateESDataFetcherTestSupport.defaultOverviewParams());

        assertEquals(1, result.size());
        assertNotNull(result.get(0).get("cpi_data"));
        verify(cpiFetcherService).fetchAssociatedParticipantIds(anyList());
        verify(inventoryESService).send(any(Request.class));
    }
}
