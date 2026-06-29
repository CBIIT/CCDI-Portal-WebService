package gov.nih.nci.bento.model.search.filter;

import gov.nih.nci.bento.constants.Const;
import org.junit.jupiter.api.Test;
import org.opensearch.search.builder.SearchSourceBuilder;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class AggregationFilterTest {

    @Test
    void buildsTermsAggregationOnSelectedField() {
        FilterParam param = FilterTestSupport.withSelectedField(Map.of(), "programs");

        SearchSourceBuilder builder = new AggregationFilter(param).getSourceFilter();

        assertEquals(0, builder.size());
        assertTrue(builder.toString().contains(Const.ES_PARAMS.TERMS_AGGS));
        assertTrue(builder.toString().contains("programs"));
    }
}
