package gov.nih.nci.bento_ri.service;

import java.util.concurrent.TimeUnit;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;

@Configuration
public class CacheService {
    @Bean
    public Cache<String, Object> caffeineCache() {
        return Caffeine.newBuilder()
                // Set a fixed time to expire after the last write or access.
                .expireAfterWrite(8, TimeUnit.HOURS)
                // The initial cache size 5MB
                .initialCapacity(5000000)
                // The maximum of cached entries: 10MB
                .maximumSize(10000000)
                .build();
    }
}