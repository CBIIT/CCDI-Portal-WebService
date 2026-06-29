package gov.nih.nci.bento_ri.service;

import java.io.InputStream;
import java.lang.reflect.Field;
import java.net.http.HttpClient;
import java.nio.charset.StandardCharsets;

/**
 * Test helpers for {@link CPIFetcherService} (field injection without Spring context).
 */
final class CPIFetcherServiceTestSupport {

    private CPIFetcherServiceTestSupport() {
    }

    static CPIFetcherService newServiceWithHttpClient(HttpClient httpClient, CacheService cacheService)
            throws Exception {
        CPIFetcherService service = new CPIFetcherService(cacheService.caffeineCache());
        setField(service, "httpClient", httpClient);
        setField(service, "clientId", "test-client-id");
        setField(service, "clientSecret", "test-client-secret");
        setField(service, "tokenUri", "https://auth.example.com/oauth/token");
        setField(service, "apiUrl", "https://cpi.example.com/v1/associated_participant_ids");
        setField(service, "domainsUrl", "https://cpi.example.com/v1/domains");
        setField(service, "scope", "custom");
        return service;
    }

    static String loadFixture(String fileName) {
        String path = "/cpi/" + fileName;
        try (InputStream in = CPIFetcherServiceTestSupport.class.getResourceAsStream(path)) {
            if (in == null) {
                throw new IllegalArgumentException("Missing classpath resource: " + path);
            }
            return new String(in.readAllBytes(), StandardCharsets.UTF_8);
        } catch (Exception e) {
            throw new IllegalStateException("Failed to load CPI fixture: " + path, e);
        }
    }

    private static void setField(Object target, String name, Object value) throws Exception {
        Field field = target.getClass().getDeclaredField(name);
        field.setAccessible(true);
        field.set(target, value);
    }
}
