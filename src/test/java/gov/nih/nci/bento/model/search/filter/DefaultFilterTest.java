package gov.nih.nci.bento.model.search.filter;

import gov.nih.nci.bento.constants.Const;
import org.junit.jupiter.api.Test;
import org.opensearch.search.builder.SearchSourceBuilder;

import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class DefaultFilterTest {

    @Test
    void noFilters_yieldsMatchAllWithMaxSize() {
        SearchSourceBuilder builder = new DefaultFilter(FilterTestSupport.argsOnly(Map.of())).getSourceFilter();

        assertEquals(Const.ES_UNITS.MAX_SIZE, builder.size());
        assertTrue(builder.toString().contains("match_all"));
    }

    @Test
    void withOrderBy_appliesSort() {
        FilterParam param = FilterParam.builder()
                .args(Map.of(Const.ES_PARAMS.ORDER_BY, "file_name", Const.ES_PARAMS.SORT_DIRECTION, "asc"))
                .defaultSortField("file_name")
                .returnFields(Set.of("file_name"))
                .build();

        SearchSourceBuilder builder = new DefaultFilter(param).getSourceFilter();

        assertTrue(builder.toString().contains("file_name"));
    }

    @Test
    void ignoreEmptyArrays_shortCircuitsToZeroSize() {
        Map<String, Object> args = Map.of("id", List.of());
        FilterParam param = FilterTestSupport.withIgnoreIfEmpty(args, Set.of("id"));

        SearchSourceBuilder builder = new DefaultFilter(param).getSourceFilter();

        assertEquals(0, builder.size());
    }
}
