package gov.nih.nci.bento.model.search.filter;

import gov.nih.nci.bento.constants.Const;
import org.junit.jupiter.api.Test;
import org.opensearch.search.builder.SearchSourceBuilder;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SumFilterTest {

    @Test
    void buildsSumAggregationOnSelectedField() {
        FilterParam param = FilterTestSupport.withSelectedField(Map.of(), "file_size");

        SearchSourceBuilder builder = new SumFilter(param).getSourceFilter();

        assertEquals(0, builder.size());
        String dsl = builder.toString();
        assertTrue(dsl.contains("sum"));
        assertTrue(dsl.contains("file_size"));
    }
}
