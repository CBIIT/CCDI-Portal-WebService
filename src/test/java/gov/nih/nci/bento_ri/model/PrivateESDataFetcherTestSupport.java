package gov.nih.nci.bento_ri.model;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import gov.nih.nci.bento_ri.service.CacheService;
import gov.nih.nci.bento_ri.service.CPIFetcherService;
import gov.nih.nci.bento_ri.service.InventoryESService;

import java.io.InputStream;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.github.benmanes.caffeine.cache.Cache;

/**
 * Shared setup for {@link PrivateESDataFetcher} unit tests (mocked OpenSearch, no live cluster).
 */
final class PrivateESDataFetcherTestSupport {

    private PrivateESDataFetcherTestSupport() {
    }

    static PrivateESDataFetcher newFetcher(InventoryESService inventoryESService) throws Exception {
        return newFetcher(inventoryESService, null);
    }

    static PrivateESDataFetcher newFetcher(InventoryESService inventoryESService, CPIFetcherService cpiFetcherService)
            throws Exception {
        PrivateESDataFetcher fetcher = new PrivateESDataFetcher(inventoryESService);
        setField(fetcher, "caffeineCache", new CacheService().caffeineCache());
        if (cpiFetcherService != null) {
            setField(fetcher, "cpiFetcherService", cpiFetcherService);
        }
        return fetcher;
    }

    static Object invoke(PrivateESDataFetcher fetcher, String methodName, Class<?>[] paramTypes, Object... args)
            throws Exception {
        Method method = PrivateESDataFetcher.class.getDeclaredMethod(methodName, paramTypes);
        method.setAccessible(true);
        return method.invoke(fetcher, args);
    }

    static JsonObject loadFixture(String fileName) {
        String path = "/opensearch/responses/" + fileName;
        try (InputStream in = PrivateESDataFetcherTestSupport.class.getResourceAsStream(path)) {
            if (in == null) {
                throw new IllegalArgumentException("Missing classpath resource: " + path);
            }
            String text = new String(in.readAllBytes(), StandardCharsets.UTF_8);
            return JsonParser.parseString(text).getAsJsonObject();
        } catch (Exception e) {
            throw new IllegalStateException("Failed to load fixture: " + path, e);
        }
    }

    static JsonArray termBuckets(String fixtureFileName, String aggName) {
        JsonObject root = loadFixture(fixtureFileName);
        return root.getAsJsonObject("aggregations")
                .getAsJsonObject(aggName)
                .getAsJsonArray("buckets");
    }

    static Map<String, Object> defaultOverviewParams() {
        return Map.of(
                "first", 10,
                "offset", 0,
                "order_by", "participant_id",
                "sort_direction", "asc");
    }

    static Map<String, Object> mutableQuery() {
        return new HashMap<>(Map.of("query", Map.of("match_all", Map.of())));
    }

    static Map<String, Object> overviewParams(int pageSize, int offset, String orderBy, String direction) {
        return Map.of(
                "first", pageSize,
                "offset", offset,
                "order_by", orderBy,
                "sort_direction", direction);
    }

    static Map<String, Object> emptyIdListParams() {
        Map<String, Object> params = new HashMap<>();
        params.put("participant_ids", List.of(""));
        params.put("diagnosis_ids", List.of(""));
        params.put("study_ids", List.of(""));
        params.put("sample_ids", List.of(""));
        params.put("file_ids", List.of(""));
        return params;
    }

    static JsonObject countResponse(int count) {
        JsonObject json = new JsonObject();
        json.addProperty("count", count);
        return json;
    }

    static JsonArray termBucket(String key, int subjects) {
        JsonObject bucket = new JsonObject();
        bucket.addProperty("key", key);
        bucket.addProperty("doc_count", subjects);
        JsonObject cardinality = new JsonObject();
        cardinality.addProperty("value", subjects);
        bucket.add("cardinality_count", cardinality);
        JsonArray buckets = new JsonArray();
        buckets.add(bucket);
        return buckets;
    }

    /** Files index query shape when facet filters are present ({@code bool.should[0].bool.filter}). */
    @SuppressWarnings("unchecked")
    static Map<String, Object> filesQueryWithShouldFilter() {
        List<Object> filter = new ArrayList<>();
        Map<String, Object> innerBool = new HashMap<>();
        innerBool.put("must", Map.of("exists", Map.of("field", "file_id")));
        innerBool.put("filter", filter);
        Map<String, Object> shouldEntry = Map.of("bool", innerBool);
        Map<String, Object> rootBool = new HashMap<>();
        rootBool.put("should", List.of(shouldEntry));
        return new HashMap<>(Map.of("query", Map.of("bool", rootBool)));
    }

    /** Files index query shape with no facet filters ({@code bool.must.exists}). */
    static Map<String, Object> filesQueryWithMustOnly() {
        return new HashMap<>(Map.of(
                "query",
                Map.of("bool", Map.of("must", Map.of("exists", Map.of("field", "file_id"))))));
    }

    static JsonObject filenamesTotalHits(int total) {
        JsonObject json = new JsonObject();
        JsonObject hits = new JsonObject();
        JsonObject totalObj = new JsonObject();
        totalObj.addProperty("value", total);
        hits.add("total", totalObj);
        json.add("hits", hits);
        return json;
    }

    static void putCacheEntry(PrivateESDataFetcher fetcher, String key, Map<String, Object> value) throws Exception {
        Field cacheField = fetcher.getClass().getDeclaredField("caffeineCache");
        cacheField.setAccessible(true);
        @SuppressWarnings("unchecked")
        Cache<String, Object> cache = (Cache<String, Object>) cacheField.get(fetcher);
        cache.put(key, value);
    }

    static JsonArray rangeCountBuckets(String fixtureFileName, String aggName) {
        JsonObject root = loadFixture(fixtureFileName);
        return root.getAsJsonObject("aggregations")
                .getAsJsonObject(aggName)
                .getAsJsonArray("buckets");
    }

    static FormattedCPIResponse cpiResponseWithMapItems(
            String participantId, String studyId, Map<String, Object> cpiItem) throws Exception {
        FormattedCPIResponse response = new FormattedCPIResponse(participantId, studyId, null);
        setField(response, "cpiData", List.of(new HashMap<>(cpiItem)));
        return response;
    }

    private static void setField(Object target, String name, Object value) throws Exception {
        Field field = target.getClass().getDeclaredField(name);
        field.setAccessible(true);
        field.set(target, value);
    }
}
