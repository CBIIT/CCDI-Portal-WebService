package gov.nih.nci.bento.model.search.yaml.type;

import gov.nih.nci.bento.constants.Const;
import gov.nih.nci.bento.model.search.MultipleRequests;
import gov.nih.nci.bento.model.search.yaml.GroupTypeQuery;
import gov.nih.nci.bento.model.search.yaml.IFilterType;
import gov.nih.nci.bento.model.search.yaml.ITypeQuery;
import gov.nih.nci.bento.service.ESService;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.core.io.ClassPathResource;

import java.io.IOException;
import java.lang.reflect.Method;
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
 * Phase 6: unit tests for {@link GroupTypeYaml}.
 */
@ExtendWith(MockitoExtension.class)
class GroupTypeYamlTest {

    @Test
    void createSearchQuery_privateAccess_registersFacetGroups() throws Exception {
        ESService esService = mock(ESService.class);
        GroupTypeYaml loader = new GroupTypeYaml(esService, Const.ES_ACCESS_TYPE.PRIVATE);
        Map<String, DataFetcher> queries = new HashMap<>();

        loader.createSearchQuery(queries, YamlTypeTestSupport.typeQuery(esService), YamlTypeTestSupport.filterType(esService));

        assertTrue(queries.containsKey("searchSubjects"));
    }

    @Test
    void readYamlFile_appliesGlobalRangeFieldsToReturnFields() throws Exception {
        ESService esService = mock(ESService.class);
        GroupTypeYaml loader = new GroupTypeYaml(esService, Const.ES_ACCESS_TYPE.PRIVATE);

        @SuppressWarnings("unchecked")
        List<GroupTypeQuery.Group> groups = (List<GroupTypeQuery.Group>) invoke(
                loader,
                "readYamlFile",
                new Class[] {ClassPathResource.class},
                new ClassPathResource("yaml/facet_search_es.yml"));

        GroupTypeQuery.Group searchSubjects = groups.stream()
                .filter(group -> "searchSubjects".equals(group.getName()))
                .findFirst()
                .orElseThrow();

        assertTrue(searchSubjects.getReturnFields().stream()
                .anyMatch(field -> field.getFilter().getRangeFilterFields() != null
                        && field.getFilter().getRangeFilterFields().contains("age_at_index")));
    }

    @Test
    void dataFetcher_invokesElasticMultiSendForAllReturnFields() throws Exception {
        ESService esService = mock(ESService.class);
        Map<String, Object> multiResult = Map.of(
                "numberOfPrograms", 3,
                "numberOfStudies", 2,
                "numberOfSubjects", 10);
        when(esService.elasticMultiSend(any())).thenReturn(multiResult);

        GroupTypeYaml loader = new GroupTypeYaml(esService, Const.ES_ACCESS_TYPE.PRIVATE);
        Map<String, DataFetcher> queries = new HashMap<>();
        loader.createSearchQuery(queries, YamlTypeTestSupport.typeQuery(esService), YamlTypeTestSupport.filterType(esService));

        DataFetchingEnvironment env = YamlTypeTestSupport.mockEnvironment(Map.of());
        Object result = queries.get("searchSubjects").get(env);

        assertNotNull(result);
        @SuppressWarnings("unchecked")
        Map<String, Object> resultMap = (Map<String, Object>) result;

        assertEquals(3, resultMap.get("numberOfPrograms"));

        ArgumentCaptor<List<MultipleRequests>> captor = ArgumentCaptor.forClass(List.class);
        verify(esService).elasticMultiSend(captor.capture());
        assertFalse(captor.getValue().isEmpty());
        assertTrue(captor.getValue().stream().anyMatch(req -> "numberOfPrograms".equals(req.getName())));
    }

    private static Object invoke(Object target, String methodName, Class<?>[] paramTypes, Object... args)
            throws Exception {
        Method method = target.getClass().getDeclaredMethod(methodName, paramTypes);
        method.setAccessible(true);
        return method.invoke(target, args);
    }
}
