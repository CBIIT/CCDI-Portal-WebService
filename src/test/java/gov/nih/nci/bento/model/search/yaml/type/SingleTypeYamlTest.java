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
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Phase 6: unit tests for {@link SingleTypeYaml}.
 */
@ExtendWith(MockitoExtension.class)
class SingleTypeYamlTest {

    @Test
    void createSearchQuery_privateAccess_registersSingleYamlQueries() throws Exception {
        ESService esService = mock(ESService.class);
        SingleTypeYaml loader = new SingleTypeYaml(esService, Const.ES_ACCESS_TYPE.PRIVATE);
        Map<String, DataFetcher> queries = new HashMap<>();

        loader.createSearchQuery(queries, YamlTypeTestSupport.typeQuery(esService), YamlTypeTestSupport.filterType(esService));

        assertTrue(queries.containsKey("filesInList"));
    }

    @Test
    void createSearchQuery_publicAccess_skipsWhenResourceMissing() throws Exception {
        ESService esService = mock(ESService.class);
        SingleTypeYaml loader = new SingleTypeYaml(esService, Const.ES_ACCESS_TYPE.PUBLIC);
        Map<String, DataFetcher> queries = new HashMap<>();

        loader.createSearchQuery(queries, YamlTypeTestSupport.typeQuery(esService), YamlTypeTestSupport.filterType(esService));

        assertTrue(queries.isEmpty());
    }

    @Test
    void dataFetcher_invokesElasticMultiSendForConfiguredQuery() throws Exception {
        ESService esService = mock(ESService.class);
        when(esService.elasticMultiSend(any())).thenReturn(Map.of("filesInList", List.of(Map.of("file_id", "f1"))));

        SingleTypeYaml loader = new SingleTypeYaml(esService, Const.ES_ACCESS_TYPE.PRIVATE);
        Map<String, DataFetcher> queries = new HashMap<>();
        ITypeQuery typeQuery = YamlTypeTestSupport.typeQuery(esService);
        IFilterType filterType = YamlTypeTestSupport.filterType(esService);
        loader.createSearchQuery(queries, typeQuery, filterType);

        DataFetchingEnvironment env = YamlTypeTestSupport.mockEnvironment(Map.of(
                "id", List.of("file-1"),
                Const.ES_PARAMS.PAGE_SIZE, 10,
                Const.ES_PARAMS.OFFSET, 0,
                Const.ES_PARAMS.ORDER_BY, "file_name",
                Const.ES_PARAMS.SORT_DIRECTION, "asc"));
        Object result = queries.get("filesInList").get(env);

        assertNotNull(result);
        ArgumentCaptor<List<MultipleRequests>> captor = ArgumentCaptor.forClass(List.class);
        verify(esService).elasticMultiSend(captor.capture());
        assertEquals(1, captor.getValue().size());
        assertEquals("filesInList", captor.getValue().get(0).getName());
    }
}
