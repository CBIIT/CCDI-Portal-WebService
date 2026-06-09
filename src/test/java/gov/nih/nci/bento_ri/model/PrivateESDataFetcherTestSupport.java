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
