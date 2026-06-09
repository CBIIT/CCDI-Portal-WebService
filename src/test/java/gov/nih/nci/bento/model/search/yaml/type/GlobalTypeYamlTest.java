package gov.nih.nci.bento.model.search.yaml.type;

import gov.nih.nci.bento.constants.Const;
import gov.nih.nci.bento.model.search.MultipleRequests;
import gov.nih.nci.bento.model.search.yaml.IFilterType;
import gov.nih.nci.bento.model.search.yaml.ITypeQuery;
import gov.nih.nci.bento.service.ESService;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.junit.jupiter.MockitoExtension;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Phase 6: unit tests for {@link GlobalTypeYaml}.
 */
@ExtendWith(MockitoExtension.class)
class GlobalTypeYamlTest {

    @Test
    void createSearchQuery_privateAccess_registersGlobalYamlQueries() throws Exception {
        ESService esService = mock(ESService.class);
        GlobalTypeYaml loader = new GlobalTypeYaml(esService, Const.ES_ACCESS_TYPE.PRIVATE);
        Map<String, DataFetcher> queries = new HashMap<>();

        loader.createSearchQuery(queries, YamlTypeTestSupport.typeQuery(esService), YamlTypeTestSupport.filterType(esService));

        assertTrue(queries.containsKey("globalSearch"));
    }

    @Test
    void dataFetcher_emptyInput_clearsCountsAndLists() throws Exception {
        ESService esService = mock(ESService.class);
        Map<String, Object> multiResult = new HashMap<>();
        multiResult.put("programs_count", 12);
        multiResult.put("programsResult", new ArrayList<>(List.of("program-a")));
        when(esService.elasticMultiSend(any())).thenReturn(multiResult);

        GlobalTypeYaml loader = new GlobalTypeYaml(esService, Const.ES_ACCESS_TYPE.PRIVATE);
        Map<String, DataFetcher> queries = new HashMap<>();
        ITypeQuery typeQuery = YamlTypeTestSupport.typeQuery(esService);
        IFilterType filterType = YamlTypeTestSupport.filterType(esService);
        loader.createSearchQuery(queries, typeQuery, filterType);

        DataFetchingEnvironment env = YamlTypeTestSupport.mockEnvironment(Map.of(
                Const.ES_PARAMS.INPUT, "",
                Const.ES_PARAMS.PAGE_SIZE, 10,
                Const.ES_PARAMS.OFFSET, 0));
        @SuppressWarnings("unchecked")
        Map<String, Object> result = (Map<String, Object>) queries.get("globalSearch").get(env);

        assertEquals(0, result.get("programs_count"));
        assertTrue(((List<?>) result.get("programsResult")).isEmpty());
    }

    @Test
    void dataFetcher_withInput_preservesMultiSendResults() throws Exception {
        ESService esService = mock(ESService.class);
        when(esService.elasticMultiSend(any())).thenReturn(Map.of(
                "programs_count", 5,
                "programsResult", List.of("program-a")));

        GlobalTypeYaml loader = new GlobalTypeYaml(esService, Const.ES_ACCESS_TYPE.PRIVATE);
        Map<String, DataFetcher> queries = new HashMap<>();
        loader.createSearchQuery(queries, YamlTypeTestSupport.typeQuery(esService), YamlTypeTestSupport.filterType(esService));

        DataFetchingEnvironment env = YamlTypeTestSupport.mockEnvironment(Map.of(
                Const.ES_PARAMS.INPUT, "cancer",
                Const.ES_PARAMS.PAGE_SIZE, 10,
                Const.ES_PARAMS.OFFSET, 0));
        @SuppressWarnings("unchecked")
        Map<String, Object> result = (Map<String, Object>) queries.get("globalSearch").get(env);

        assertEquals(5, result.get("programs_count"));
        assertEquals(List.of("program-a"), result.get("programsResult"));

        ArgumentCaptor<List<MultipleRequests>> captor = ArgumentCaptor.forClass(List.class);
        verify(esService).elasticMultiSend(captor.capture());
        assertEquals(2, captor.getValue().size());
    }
}
