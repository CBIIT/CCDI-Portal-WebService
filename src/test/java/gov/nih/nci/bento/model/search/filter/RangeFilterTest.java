package gov.nih.nci.bento.model.search.filter;

import org.junit.jupiter.api.Test;
import org.opensearch.search.builder.SearchSourceBuilder;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class RangeFilterTest {

    @Test
    void buildsMinMaxAggregations() {
        FilterParam param = FilterParam.builder()
                .args(Map.of())
                .selectedField("age_at_diagnosis")
                .isRangeFilter(true)
                .build();

        SearchSourceBuilder builder = new RangeFilter(param).getSourceFilter();

        assertEquals(0, builder.size());
        String dsl = builder.toString();
        assertTrue(dsl.contains("max"));
        assertTrue(dsl.contains("min"));
        assertTrue(dsl.contains("age_at_diagnosis"));
    }

    @Test
    void tracksSelectedFieldInStaticRangeFields() {
        FilterParam param = FilterParam.builder()
                .args(Map.of())
                .selectedField("survival_time")
                .isRangeFilter(true)
                .build();

        new RangeFilter(param);

        assertTrue(RangeFilter.getRangeFields().contains("survival_time"));
    }
}
