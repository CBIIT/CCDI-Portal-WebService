package gov.nih.nci.bento_ri.service;

import com.github.benmanes.caffeine.cache.Cache;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

class CacheServiceTest {

    @Test
    void caffeineCache_storesAndRetrievesValues() {
        CacheService cacheService = new CacheService();
        Cache<String, Object> cache = cacheService.caffeineCache();

        assertNotNull(cache);
        cache.put("test-key", "test-value");
        assertEquals("test-value", cache.getIfPresent("test-key"));
    }

    @Test
    void caffeineCache_missingKey_returnsNull() {
        Cache<String, Object> cache = new CacheService().caffeineCache();

        assertNull(cache.getIfPresent("missing-key"));
    }
}
