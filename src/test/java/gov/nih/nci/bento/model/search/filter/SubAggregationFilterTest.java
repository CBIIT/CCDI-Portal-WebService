package gov.nih.nci.bento.model.search.filter;

import gov.nih.nci.bento.constants.Const;
import org.junit.jupiter.api.Test;
import org.opensearch.search.builder.SearchSourceBuilder;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SubAggregationFilterTest {

    @Test
    void buildsTermsSubAggregation() {
        FilterParam param = FilterParam.builder()
                .args(Map.of())
                .selectedField("programs")
                .subAggSelectedField("studies")
                .build();

        SearchSourceBuilder builder = new SubAggregationFilter(param).getSourceFilter();

        assertEquals(0, builder.size());
        String dsl = builder.toString();
        assertTrue(dsl.contains(Const.ES_PARAMS.TERMS_AGGS));
        assertTrue(dsl.contains("programs"));
        assertTrue(dsl.contains("studies"));
    }
}
