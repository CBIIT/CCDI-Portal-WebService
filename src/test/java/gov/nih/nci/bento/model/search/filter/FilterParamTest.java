package gov.nih.nci.bento.model.search.filter;

import gov.nih.nci.bento.constants.Const;
import org.junit.jupiter.api.Test;
import org.opensearch.search.sort.SortOrder;

import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;

class FilterParamTest {

    @Test
    void pagination_readsPageSizeAndOffset() {
        FilterParam param = FilterParam.builder()
                .args(Map.of(
                        Const.ES_PARAMS.PAGE_SIZE, 25,
                        Const.ES_PARAMS.OFFSET, 50))
                .build();

        FilterParam.Pagination page = param.getPagination();
        assertEquals(25, page.getPageSize());
        assertEquals(50, page.getOffSet());
    }

    @Test
    void pagination_capsPageSizeAtMax() {
        FilterParam param = FilterParam.builder()
                .args(Map.of(Const.ES_PARAMS.PAGE_SIZE, Const.ES_UNITS.MAX_SIZE + 1000))
                .build();

        assertEquals(Const.ES_UNITS.MAX_SIZE, param.getPagination().getPageSize());
    }

    @Test
    void pagination_sortDirectionAsc() {
        FilterParam param = FilterParam.builder()
                .args(Map.of(Const.ES_PARAMS.SORT_DIRECTION, "asc"))
                .defaultSortField("name")
                .returnFields(Set.of("name"))
                .build();

        assertEquals(SortOrder.ASC, param.getPagination().getSortDirection());
    }

    @Test
    void pagination_orderByUsesDefaultWhenNotInReturnFields() {
        FilterParam param = FilterParam.builder()
                .args(Map.of(Const.ES_PARAMS.ORDER_BY, "unknown_field"))
                .defaultSortField("file_name")
                .returnFields(Set.of("file_name", "file_id"))
                .build();

        assertEquals("file_name", param.getPagination().getOrderBy());
    }

    @Test
    void pagination_alternativeSortFieldMapping() {
        FilterParam param = FilterParam.builder()
                .args(Map.of(Const.ES_PARAMS.ORDER_BY, "file_name"))
                .defaultSortField("file_name")
                .alternativeSortField(Map.of("file_name", "file_name.keyword"))
                .returnFields(Set.of("file_name"))
                .build();

        assertEquals("file_name.keyword", param.getPagination().getPageOrderBy());
    }

    @Test
    void searchText_readFromInputArg() {
        FilterParam param = FilterParam.builder()
                .args(Map.of(Const.ES_PARAMS.INPUT, "leukemia"))
                .build();

        assertEquals("leukemia", param.getSearchText());
    }
}
