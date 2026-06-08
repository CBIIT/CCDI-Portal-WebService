package gov.nih.nci.bento.model.search.filter;

import gov.nih.nci.bento.constants.Const;
import org.junit.jupiter.api.Test;
import org.opensearch.search.builder.SearchSourceBuilder;

import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class PaginationFilterTest {

    @Test
    void appliesPageSizeOffsetAndSort() {
        FilterParam param = FilterParam.builder()
                .args(Map.of(
                        Const.ES_PARAMS.PAGE_SIZE, 15,
                        Const.ES_PARAMS.OFFSET, 30,
                        Const.ES_PARAMS.ORDER_BY, "file_name",
                        Const.ES_PARAMS.SORT_DIRECTION, "asc"))
                .defaultSortField("file_name")
                .returnFields(Set.of("file_name", "file_id"))
                .build();

        SearchSourceBuilder builder = new PaginationFilter(param).getSourceFilter();

        assertEquals(15, builder.size());
        assertEquals(30, builder.from());
        assertTrue(builder.toString().contains("file_name"));
    }

    @Test
    void withFacetFilter_includesTermsInQuery() {
        FilterParam param = FilterParam.builder()
                .args(Map.of(
                        Const.ES_PARAMS.PAGE_SIZE, 10,
                        Const.ES_PARAMS.OFFSET, 0,
                        "race", List.of("Asian")))
                .defaultSortField("participant_id")
                .returnFields(Set.of("participant_id"))
                .build();

        SearchSourceBuilder builder = new PaginationFilter(param).getSourceFilter();

        assertTrue(builder.toString().contains("race"));
        assertTrue(builder.toString().contains("Asian"));
    }
}
