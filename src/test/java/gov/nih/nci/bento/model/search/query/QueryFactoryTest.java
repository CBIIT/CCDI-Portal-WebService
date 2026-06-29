package gov.nih.nci.bento.model.search.query;

import gov.nih.nci.bento.constants.Const;
import gov.nih.nci.bento.model.search.filter.FilterParam;
import org.junit.jupiter.api.Test;
import org.opensearch.index.query.QueryBuilder;

import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertTrue;

class QueryFactoryTest {

    @Test
    void emptyArgs_yieldsMatchAll() {
        QueryFactory factory = new QueryFactory(FilterParam.builder().args(Map.of()).build());

        QueryBuilder query = factory.getQuery();

        assertTrue(query.toString().contains("match_all"));
    }

    @Test
    void listParam_yieldsTermsFilter() {
        FilterParam param = FilterParam.builder()
                .args(Map.of("race", List.of("Asian", "White")))
                .build();
        QueryFactory factory = new QueryFactory(param);

        QueryBuilder query = factory.getQuery();

        assertTrue(query.toString().contains("race"));
        assertTrue(query.toString().contains("Asian"));
    }

    @Test
    void stringParam_coercedToSingleValueTerms() {
        FilterParam param = FilterParam.builder()
                .args(Map.of("study_id", "ccdi-int-study-1"))
                .build();
        QueryFactory factory = new QueryFactory(param);

        QueryBuilder query = factory.getQuery();

        assertTrue(query.toString().contains("study_id"));
        assertTrue(query.toString().contains("ccdi-int-study-1"));
    }

    @Test
    void rangeField_yieldsRangeFilter() {
        FilterParam param = FilterParam.builder()
                .args(Map.of("age_at_diagnosis", List.of("10", "20")))
                .rangeFilterFields(Set.of("age_at_diagnosis"))
                .build();
        QueryFactory factory = new QueryFactory(param);

        QueryBuilder query = factory.getQuery();

        assertTrue(query.toString().contains("range"));
        assertTrue(query.toString().contains("age_at_diagnosis"));
        assertTrue(query.toString().contains("10"));
        assertTrue(query.toString().contains("20"));
    }

    @Test
    void caseInsensitive_yieldsWildcardShouldClauses() {
        FilterParam param = FilterParam.builder()
                .args(Map.of("race", List.of("asian")))
                .caseInsensitive(true)
                .build();
        QueryFactory factory = new QueryFactory(param);

        QueryBuilder query = factory.getQuery();

        assertTrue(query.toString().contains("wildcard"));
        assertTrue(query.toString().contains("race"));
    }

    @Test
    void sortParams_areExcludedFromQuery() {
        FilterParam param = FilterParam.builder()
                .args(Map.of(
                        "race", List.of("Asian"),
                        Const.ES_PARAMS.PAGE_SIZE, 10,
                        Const.ES_PARAMS.OFFSET, 0,
                        Const.ES_PARAMS.ORDER_BY, "race",
                        Const.ES_PARAMS.SORT_DIRECTION, "asc"))
                .build();
        QueryFactory factory = new QueryFactory(param);

        QueryBuilder query = factory.getQuery();

        String dsl = query.toString();
        assertTrue(dsl.contains("race"));
        assertTrue(!dsl.contains("order_by") && !dsl.contains("sort_direction"));
    }

    @Test
    void extraFilters_areMergedIntoQuery() {
        FilterParam param = FilterParam.builder()
                .args(Map.of("race", List.of("Asian")))
                .extraFilters(Map.of("study_id", List.of("study-1")))
                .build();
        QueryFactory factory = new QueryFactory(param);

        QueryBuilder query = factory.getQuery();

        String dsl = query.toString();
        assertTrue(dsl.contains("race"));
        assertTrue(dsl.contains("study_id"));
    }

    @Test
    void ignoreSelectedField_removesSelectedFieldFromArgs() {
        FilterParam param = FilterParam.builder()
                .args(Map.of("programs", List.of("CCDI"), "studies", List.of("Study A")))
                .selectedField("programs")
                .isIgnoreSelectedField(true)
                .build();
        QueryFactory factory = new QueryFactory(param);

        QueryBuilder query = factory.getQuery();

        String dsl = query.toString();
        assertTrue(!dsl.contains("programs"));
        assertTrue(dsl.contains("studies"));
    }
}
