package gov.nih.nci.bento.model.search.filter;

import gov.nih.nci.bento.constants.Const;
import gov.nih.nci.bento.model.search.yaml.filter.YamlFilter;
import gov.nih.nci.bento.model.search.yaml.filter.YamlGlobalFilterType;
import gov.nih.nci.bento.model.search.yaml.filter.YamlHighlight;
import gov.nih.nci.bento.model.search.yaml.filter.YamlQuery;

import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Shared builders for filter-layer unit tests.
 */
final class FilterTestSupport {

    private FilterTestSupport() {
    }

    static FilterParam argsOnly(Map<String, Object> args) {
        return FilterParam.builder().args(args).build();
    }

    static FilterParam withSelectedField(Map<String, Object> args, String selectedField) {
        return FilterParam.builder().args(args).selectedField(selectedField).build();
    }

    static FilterParam withIgnoreIfEmpty(Map<String, Object> args, Set<String> ignoreIfEmpty) {
        return FilterParam.builder().args(args).ignoreIfEmpty(ignoreIfEmpty).build();
    }

    static YamlQuery globalYamlQuery(List<YamlGlobalFilterType.GlobalQuerySet> searches) {
        YamlQuery query = new YamlQuery();
        YamlFilter filter = new YamlFilter();
        filter.setSearches(searches);
        filter.setDefaultSortField("title");
        query.setFilter(filter);
        return query;
    }

    static YamlGlobalFilterType.GlobalQuerySet globalQuerySet(String field, String type) {
        YamlGlobalFilterType.GlobalQuerySet set = new YamlGlobalFilterType.GlobalQuerySet();
        set.setField(field);
        set.setType(type);
        return set;
    }

    static YamlQuery globalYamlQueryWithHighlight(List<String> fields) {
        YamlQuery query = globalYamlQuery(List.of(globalQuerySet("title", Const.YAML_QUERY.QUERY_TERMS.MATCH)));
        YamlHighlight highlight = new YamlHighlight();
        highlight.setFields(fields);
        highlight.setPreTag("<em>");
        highlight.setPostTag("</em>");
        highlight.setFragmentSize(120);
        query.setHighlight(highlight);
        return query;
    }
}
