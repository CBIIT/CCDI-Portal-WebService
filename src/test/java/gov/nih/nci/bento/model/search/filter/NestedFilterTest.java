package gov.nih.nci.bento.model.search.filter;

import gov.nih.nci.bento.constants.Const;
import org.junit.jupiter.api.Test;
import org.opensearch.search.builder.SearchSourceBuilder;

import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class NestedFilterTest {

    @Test
    void buildsNestedAggregationWithInnerTerms() {
        FilterParam param = FilterParam.builder()
                .args(Map.of("tissue_type", List.of("Blood")))
                .selectedField("sample_ids")
                .nestedPath("sample_info")
                .nestedParameters(Set.of("tissue_type", "composition"))
                .build();

        SearchSourceBuilder builder = new NestedFilter(param).getSourceFilter();

        assertEquals(0, builder.size());
        String dsl = builder.toString();
        assertTrue(dsl.contains(Const.ES_PARAMS.NESTED_SEARCH));
        assertTrue(dsl.contains("sample_info"));
        assertTrue(dsl.contains("sample_ids"));
    }

    @Test
    void ignoreSelectedField_skipsSelectedFieldInNestedShouldClauses() {
        FilterParam param = FilterParam.builder()
                .args(Map.of("sample_ids", List.of("S1"), "composition", List.of("DNA")))
                .selectedField("sample_ids")
                .nestedPath("sample_info")
                .nestedParameters(Set.of("sample_ids", "composition"))
                .isIgnoreSelectedField(true)
                .build();

        SearchSourceBuilder builder = new NestedFilter(param).getSourceFilter();

        String dsl = builder.toString();
        assertTrue(dsl.contains("composition"));
        assertTrue(dsl.contains("DNA"));
        assertTrue(!dsl.contains("S1"));
    }
}
