package gov.nih.nci.bento.model.search.filter;

import gov.nih.nci.bento.constants.Const;
import gov.nih.nci.bento.model.search.yaml.filter.YamlFilter;
import gov.nih.nci.bento.model.search.yaml.filter.YamlGlobalFilterType;
import org.junit.jupiter.api.Test;
import org.opensearch.search.builder.SearchSourceBuilder;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class GlobalQueryFilterTest {

    @Test
    void termSearch_buildsShouldQueryWithPagination() {
        FilterParam param = FilterParam.builder()
                .args(Map.of(
                        Const.ES_PARAMS.INPUT, "leukemia",
                        Const.ES_PARAMS.PAGE_SIZE, 10,
                        Const.ES_PARAMS.OFFSET, 5))
                .build();
        var query = FilterTestSupport.globalYamlQuery(
                List.of(FilterTestSupport.globalQuerySet("title", Const.YAML_QUERY.QUERY_TERMS.TERM)));

        SearchSourceBuilder builder = new GlobalQueryFilter(param, query).getSourceFilter();

        assertEquals(10, builder.size());
        assertEquals(5, builder.from());
        String dsl = builder.toString();
        assertTrue(dsl.contains("title"));
        assertTrue(dsl.contains("leukemia"));
    }

    @Test
    void wildcardSearch_buildsWildcardShouldClause() {
        FilterParam param = FilterParam.builder()
                .args(Map.of(Const.ES_PARAMS.INPUT, "cancer", Const.ES_PARAMS.PAGE_SIZE, 20, Const.ES_PARAMS.OFFSET, 0))
                .build();
        var query = FilterTestSupport.globalYamlQuery(
                List.of(FilterTestSupport.globalQuerySet("summary", Const.YAML_QUERY.QUERY_TERMS.WILD_CARD)));

        SearchSourceBuilder builder = new GlobalQueryFilter(param, query).getSourceFilter();

        assertTrue(builder.toString().contains("wildcard"));
        assertTrue(builder.toString().contains("summary"));
    }

    @Test
    void typedBooleanSearch_addsConditionalTerm() {
        FilterParam param = FilterParam.builder()
                .args(Map.of(Const.ES_PARAMS.INPUT, "value is true", Const.ES_PARAMS.PAGE_SIZE, 10, Const.ES_PARAMS.OFFSET, 0))
                .build();
        var query = FilterTestSupport.globalYamlQuery(List.of());
        YamlFilter filter = query.getFilter();
        YamlGlobalFilterType.GlobalQuerySet typed = new YamlGlobalFilterType.GlobalQuerySet();
        typed.setField("is_active");
        typed.setType(Const.YAML_QUERY.QUERY_TERMS.TERM);
        typed.setOption(Const.YAML_QUERY.QUERY_TERMS.BOOLEAN);
        filter.setTypedSearch(List.of(typed));

        SearchSourceBuilder builder = new GlobalQueryFilter(param, query).getSourceFilter();

        assertTrue(builder.toString().contains("is_active"));
        assertTrue(builder.toString().contains("true"));
    }

    @Test
    void highlightConfig_appliesHighlighterFields() {
        FilterParam param = FilterParam.builder()
                .args(Map.of(Const.ES_PARAMS.INPUT, "about", Const.ES_PARAMS.PAGE_SIZE, 5, Const.ES_PARAMS.OFFSET, 0))
                .build();
        var query = FilterTestSupport.globalYamlQueryWithHighlight(List.of("title", "text"));

        SearchSourceBuilder builder = new GlobalQueryFilter(param, query).getSourceFilter();

        String dsl = builder.toString();
        assertTrue(dsl.contains("highlight"));
        assertTrue(dsl.contains("title"));
        assertTrue(dsl.contains("text"));
    }
}
