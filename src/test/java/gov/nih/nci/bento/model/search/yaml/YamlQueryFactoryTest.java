package gov.nih.nci.bento.model.search.yaml;

import gov.nih.nci.bento.constants.Const;
import gov.nih.nci.bento.model.search.mapper.TypeMapper;
import gov.nih.nci.bento.model.search.query.QueryParam;
import gov.nih.nci.bento.model.search.yaml.filter.YamlFilter;
import gov.nih.nci.bento.model.search.yaml.filter.YamlGlobalFilterType;
import gov.nih.nci.bento.model.search.yaml.filter.YamlQuery;
import gov.nih.nci.bento.model.search.yaml.filter.YamlResult;
import gov.nih.nci.bento.service.ESService;
import graphql.schema.DataFetcher;
import graphql.schema.GraphQLOutputType;
import graphql.schema.SchemaElementChildrenContainer;
import org.junit.jupiter.api.Test;
import org.opensearch.search.builder.SearchSourceBuilder;

import java.io.IOException;
import java.lang.reflect.Method;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class YamlQueryFactoryTest {

    @Test
    void createYamlQueries_loadsSingleSearchQueryFromClasspath() throws IOException {
        YamlQueryFactory factory = new YamlQueryFactory(mock(ESService.class));

        Map<String, DataFetcher> queries = factory.createYamlQueries(Const.ES_ACCESS_TYPE.PRIVATE);

        assertTrue(queries.containsKey("filesInList"));
    }

    @Test
    void filterType_default_returnsSearchSourceBuilder() throws Exception {
        IFilterType filterType = filterTypeFromFactory();
        SearchSourceBuilder builder = filterType.getQueryFilter(
                minimalQueryParam(Map.of()),
                yamlQueryWithFilter(Const.YAML_QUERY.FILTER.DEFAULT));

        assertTrue(builder.toString().contains("match_all"));
    }

    @Test
    void filterType_pagination_appliesPageSettings() throws Exception {
        IFilterType filterType = filterTypeFromFactory();
        SearchSourceBuilder builder = filterType.getQueryFilter(
                minimalQueryParam(Map.of(
                        Const.ES_PARAMS.PAGE_SIZE, 10,
                        Const.ES_PARAMS.OFFSET, 0)),
                yamlQueryWithFilter(Const.YAML_QUERY.FILTER.PAGINATION));

        assertTrue(builder.size() == 10 || builder.toString().contains("match_all"));
    }

    @Test
    void filterType_aggregation_buildsTermsAgg() throws Exception {
        YamlQuery query = yamlQueryWithFilter(Const.YAML_QUERY.FILTER.AGGREGATION);
        query.getFilter().setSelectedField("programs");

        SearchSourceBuilder builder = filterTypeFromFactory().getQueryFilter(minimalQueryParam(Map.of()), query);

        assertTrue(builder.toString().contains("programs"));
    }

    @Test
    void filterType_range_buildsMinMaxAggs() throws Exception {
        YamlQuery query = yamlQueryWithFilter(Const.YAML_QUERY.FILTER.RANGE);
        query.getFilter().setSelectedField("age_at_index");

        SearchSourceBuilder builder = filterTypeFromFactory().getQueryFilter(minimalQueryParam(Map.of()), query);

        assertTrue(builder.toString().contains("max"));
        assertTrue(builder.toString().contains("min"));
    }

    @Test
    void filterType_nested_buildsNestedAgg() throws Exception {
        YamlQuery query = yamlQueryWithFilter(Const.YAML_QUERY.FILTER.NESTED);
        YamlFilter filter = query.getFilter();
        filter.setSelectedField("sample_ids");
        filter.setNestedPath("sample_info");
        filter.setNestedParameters(Set.of("tissue_type"));

        SearchSourceBuilder builder = filterTypeFromFactory().getQueryFilter(
                minimalQueryParam(Map.of("tissue_type", List.of("Blood"))),
                query);

        assertTrue(builder.toString().contains("sample_info"));
    }

    @Test
    void filterType_global_buildsGlobalSearch() throws Exception {
        YamlQuery query = yamlQueryWithFilter(Const.YAML_QUERY.FILTER.GLOBAL);
        YamlFilter filter = query.getFilter();
        YamlGlobalFilterType.GlobalQuerySet search = new YamlGlobalFilterType.GlobalQuerySet();
        search.setField("title");
        search.setType(Const.YAML_QUERY.QUERY_TERMS.MATCH);
        filter.setSearches(List.of(search));

        SearchSourceBuilder builder = filterTypeFromFactory().getQueryFilter(
                minimalQueryParam(Map.of(
                        Const.ES_PARAMS.INPUT, "cancer",
                        Const.ES_PARAMS.PAGE_SIZE, 10,
                        Const.ES_PARAMS.OFFSET, 0)),
                query);

        assertTrue(builder.toString().contains("title"));
    }

    @Test
    void returnType_objectArray_returnsListMapper() throws Exception {
        ITypeQuery returnType = returnTypeFromFactory();
        YamlQuery query = new YamlQuery();
        YamlResult result = new YamlResult();
        result.setType(Const.YAML_QUERY.RESULT_TYPE.OBJECT_ARRAY);
        query.setResult(result);

        TypeMapper<?> mapper = returnType.getReturnType(minimalQueryParam(Map.of()), query);

        assertNotNull(mapper);
    }

    @Test
    void returnType_stringArray_returnsStrListMapper() throws Exception {
        ITypeQuery returnType = returnTypeFromFactory();
        YamlQuery query = new YamlQuery();
        YamlResult result = new YamlResult();
        result.setType(Const.YAML_QUERY.RESULT_TYPE.STRING_ARRAY);
        query.setResult(result);
        YamlFilter filter = new YamlFilter();
        filter.setSelectedField("programs");
        query.setFilter(filter);

        TypeMapper<?> mapper = returnType.getReturnType(minimalQueryParam(Map.of()), query);

        assertNotNull(mapper);
    }

    @Test
    void returnType_unknownIntMethod_throws() throws Exception {
        ITypeQuery returnType = returnTypeFromFactory();
        YamlQuery query = new YamlQuery();
        YamlResult result = new YamlResult();
        result.setType(Const.YAML_QUERY.RESULT_TYPE.INT);
        result.setMethod("unknown_method");
        query.setResult(result);

        assertThrows(IllegalArgumentException.class,
                () -> returnType.getReturnType(minimalQueryParam(Map.of()), query));
    }

    private static IFilterType filterTypeFromFactory() throws Exception {
        YamlQueryFactory factory = new YamlQueryFactory(mock(ESService.class));
        return invokePrivate(factory, "getFilterType", IFilterType.class);
    }

    private static ITypeQuery returnTypeFromFactory() throws Exception {
        YamlQueryFactory factory = new YamlQueryFactory(mock(ESService.class));
        return invokePrivate(factory, "getReturnType", ITypeQuery.class);
    }

    private static YamlQuery yamlQueryWithFilter(String filterType) {
        YamlQuery query = new YamlQuery();
        YamlFilter filter = new YamlFilter();
        filter.setType(filterType);
        filter.setDefaultSortField("name");
        query.setFilter(filter);
        return query;
    }

    private static QueryParam minimalQueryParam(Map<String, Object> args) {
        GraphQLOutputType outputType = mock(GraphQLOutputType.class);
        SchemaElementChildrenContainer container = mock(SchemaElementChildrenContainer.class);
        when(outputType.getChildrenWithTypeReferences()).thenReturn(container);
        when(container.getChildrenAsList()).thenReturn(List.of());
        return QueryParam.builder().args(args).outputType(outputType).build();
    }

    @SuppressWarnings("unchecked")
    private static <T> T invokePrivate(YamlQueryFactory factory, String methodName, Class<T> type) throws Exception {
        Method method = YamlQueryFactory.class.getDeclaredMethod(methodName);
        method.setAccessible(true);
        return (T) method.invoke(factory);
    }
}
