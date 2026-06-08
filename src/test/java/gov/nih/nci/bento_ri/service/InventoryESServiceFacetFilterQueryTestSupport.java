package gov.nih.nci.bento_ri.service;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * Shared constants and assertions for {@link InventoryESService#buildFacetFilterQuery} tests.
 */
final class InventoryESServiceFacetFilterQueryTestSupport {

    static final Set<String> RANGE_PARAMS = Set.of(
            "age_at_diagnosis",
            "participant_age_at_collection",
            "age_at_treatment_start",
            "age_at_response",
            "age_at_last_known_survival_status");

    static final String NESTED_FILTERS = "nested_filters";

    private InventoryESServiceFacetFilterQueryTestSupport() {
    }

    static Map<String, Object> buildFacetFilter(
            InventoryESService service, Map<String, Object> params, String indexType) throws IOException {
        return buildFacetFilter(service, params, Set.of(), indexType);
    }

    static Map<String, Object> buildFacetFilter(
            InventoryESService service,
            Map<String, Object> params,
            Set<String> excludedParams,
            String indexType) throws IOException {
        return service.buildFacetFilterQuery(
                params, RANGE_PARAMS, excludedParams, Set.of(), NESTED_FILTERS, indexType);
    }

    @SuppressWarnings("unchecked")
    static List<Object> nonFilesTopLevelFilters(Map<String, Object> query) {
        Map<String, Object> queryBody = (Map<String, Object>) query.get("query");
        if (queryBody.containsKey("match_all")) {
            return List.of();
        }
        Map<String, Object> bool = (Map<String, Object>) queryBody.get("bool");
        return (List<Object>) bool.get("filter");
    }

    @SuppressWarnings("unchecked")
    static Optional<Map<String, Object>> findNestedBlock(List<Object> filters, String path) {
        for (Object clause : filters) {
            Map<String, Object> map = (Map<String, Object>) clause;
            if (map.containsKey("nested")) {
                Map<String, Object> nested = (Map<String, Object>) map.get("nested");
                if (path.equals(nested.get("path"))) {
                    return Optional.of(nested);
                }
            }
        }
        return Optional.empty();
    }

    @SuppressWarnings("unchecked")
    static List<Object> nestedInnerFilters(Map<String, Object> nestedBlock) {
        Map<String, Object> innerBool = (Map<String, Object>) ((Map<String, Object>) nestedBlock.get("query")).get("bool");
        return (List<Object>) innerBool.get("filter");
    }

    @SuppressWarnings("unchecked")
    static boolean nonFilesFiltersContainTerms(List<Object> filters, String fieldPath, List<?> values) {
        for (Object clause : filters) {
            Map<String, Object> map = (Map<String, Object>) clause;
            if (map.containsKey("terms")) {
                Map<String, Object> terms = (Map<String, Object>) map.get("terms");
                if (values.equals(terms.get(fieldPath))) {
                    return true;
                }
            }
        }
        return false;
    }

    /**
     * Files index wraps filters under {@code bool.should[0].bool.filter}.
     */
    @SuppressWarnings("unchecked")
    static List<Object> filesIndexFilters(Map<String, Object> query) {
        Map<String, Object> queryBody = (Map<String, Object>) query.get("query");
        if (queryBody.containsKey("match_all")) {
            return List.of();
        }
        Map<String, Object> rootBool = (Map<String, Object>) queryBody.get("bool");
        if (rootBool.containsKey("must")) {
            return List.of();
        }
        List<Object> should = (List<Object>) rootBool.get("should");
        Map<String, Object> innerBool = (Map<String, Object>) ((Map<String, Object>) should.get(0)).get("bool");
        return (List<Object>) innerBool.get("filter");
    }

    @SuppressWarnings("unchecked")
    static Optional<Map<String, Object>> findNestedInFilesCombinedFilters(List<Object> filesFilters) {
        for (Object clause : filesFilters) {
            Map<String, Object> map = (Map<String, Object>) clause;
            if (!map.containsKey("nested")) {
                continue;
            }
            Map<String, Object> nested = (Map<String, Object>) map.get("nested");
            if ("combined_filters".equals(nested.get("path"))) {
                return Optional.of(nested);
            }
        }
        return Optional.empty();
    }

    @SuppressWarnings("unchecked")
    static List<Object> combinedFiltersInnerFilters(Map<String, Object> nestedCombinedFilters) {
        return nestedInnerFilters(nestedCombinedFilters);
    }
}
