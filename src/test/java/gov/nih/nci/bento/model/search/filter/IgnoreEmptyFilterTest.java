package gov.nih.nci.bento.model.search.filter;

import gov.nih.nci.bento.constants.Const;
import org.junit.jupiter.api.Test;
import org.opensearch.search.builder.SearchSourceBuilder;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class IgnoreEmptyFilterTest {

    @Test
    void allEmptyArrays_setsIgnoreConditionWithZeroSize() {
        Map<String, Object> args = Map.of("race", List.of(), "sex_at_birth", List.of());
        FilterParam param = FilterTestSupport.withIgnoreIfEmpty(args, Set.of("race", "sex_at_birth"));

        IgnoreEmptyFilter filter = new IgnoreEmptyFilter(param);

        assertTrue(filter.isIgnoreCondition());
        SearchSourceBuilder builder = filter.getIgnoreSearches();
        assertEquals(0, builder.size());
    }

    @Test
    void allEmptyStrings_setsMatchAllWithMaxSize() {
        Map<String, Object> args = Map.of("race", List.of(""), "sex_at_birth", List.of(""));
        FilterParam param = FilterTestSupport.withIgnoreIfEmpty(args, Set.of("race", "sex_at_birth"));

        IgnoreEmptyFilter filter = new IgnoreEmptyFilter(param);

        assertTrue(filter.isIgnoreCondition());
        SearchSourceBuilder builder = filter.getIgnoreSearches();
        assertEquals(Const.ES_UNITS.MAX_SIZE, builder.size());
        assertTrue(builder.toString().contains("match_all"));
    }

    @Test
    void realValues_doesNotIgnore() {
        Map<String, Object> args = Map.of("race", List.of("Asian"));
        FilterParam param = FilterTestSupport.withIgnoreIfEmpty(args, Set.of("race"));

        IgnoreEmptyFilter filter = new IgnoreEmptyFilter(param);

        assertFalse(filter.isIgnoreCondition());
    }

    @Test
    void missingIgnoreField_doesNotIgnore() {
        Map<String, Object> args = new HashMap<>();
        args.put("race", List.of());
        FilterParam param = FilterTestSupport.withIgnoreIfEmpty(args, Set.of("race", "sex_at_birth"));

        IgnoreEmptyFilter filter = new IgnoreEmptyFilter(param);

        assertFalse(filter.isIgnoreCondition());
    }
}
