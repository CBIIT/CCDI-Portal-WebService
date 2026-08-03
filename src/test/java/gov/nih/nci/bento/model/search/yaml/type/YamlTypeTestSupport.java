package gov.nih.nci.bento.model.search.yaml.type;

import gov.nih.nci.bento.constants.Const;
import gov.nih.nci.bento.model.search.query.QueryParam;
import gov.nih.nci.bento.model.search.yaml.IFilterType;
import gov.nih.nci.bento.model.search.yaml.ITypeQuery;
import gov.nih.nci.bento.model.search.yaml.YamlQueryFactory;
import gov.nih.nci.bento.service.ESService;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import graphql.schema.GraphQLOutputType;
import graphql.schema.SchemaElementChildrenContainer;
import org.springframework.core.io.ClassPathResource;

import java.lang.reflect.Method;
import java.util.List;
import java.util.Map;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Shared helpers for YAML type loader unit tests.
 */
final class YamlTypeTestSupport {

    private YamlTypeTestSupport() {
    }

    static IFilterType filterType(ESService esService) throws Exception {
        return invokeYamlQueryFactory(esService, "getFilterType", IFilterType.class);
    }

    static ITypeQuery typeQuery(ESService esService) throws Exception {
        return invokeYamlQueryFactory(esService, "getReturnType", ITypeQuery.class);
    }

    static DataFetchingEnvironment mockEnvironment(Map<String, Object> args) {
        DataFetchingEnvironment env = mock(DataFetchingEnvironment.class);
        GraphQLOutputType outputType = mockOutputType();
        when(env.getArguments()).thenReturn(args);
        when(env.getFieldType()).thenReturn(outputType);
        return env;
    }

    static GraphQLOutputType mockOutputType() {
        GraphQLOutputType outputType = mock(GraphQLOutputType.class);
        SchemaElementChildrenContainer container = mock(SchemaElementChildrenContainer.class);
        when(outputType.getChildrenWithTypeReferences()).thenReturn(container);
        when(container.getChildrenAsList()).thenReturn(List.of());
        return outputType;
    }

    static QueryParam queryParam(Map<String, Object> args) {
        return QueryParam.builder().args(args).outputType(mockOutputType()).build();
    }

    static String resolveYamlFileName(Const.ES_ACCESS_TYPE accessType, String fileName) throws Exception {
        Method method = AbstractYamlType.class.getDeclaredMethod(
                "getYamlFileName", Const.ES_ACCESS_TYPE.class, String.class);
        method.setAccessible(true);
        TestYamlType helper = new TestYamlType();
        return (String) method.invoke(helper, accessType, fileName);
    }

    static boolean yamlResourceExists(Const.ES_ACCESS_TYPE accessType, String fileName) throws Exception {
        String resolved = resolveYamlFileName(accessType, fileName);
        return new ClassPathResource(Const.YAML_QUERY.SUB_FOLDER + resolved).exists();
    }

    @SuppressWarnings("unchecked")
    private static <T> T invokeYamlQueryFactory(ESService esService, String methodName, Class<T> type)
            throws Exception {
        YamlQueryFactory factory = new YamlQueryFactory(esService);
        Method method = YamlQueryFactory.class.getDeclaredMethod(methodName);
        method.setAccessible(true);
        return (T) method.invoke(factory);
    }

    /** Concrete subclass so protected {@link AbstractYamlType#getYamlFileName} can be exercised. */
    private static final class TestYamlType extends AbstractYamlType {
        @Override
        public void createSearchQuery(
                Map<String, DataFetcher> resultMap, ITypeQuery iTypeQuery, IFilterType iFilterType) {
            // not used
        }
    }
}
