package gov.nih.nci.bento_ri.service;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import gov.nih.nci.bento.model.ConfigurationDAO;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.mockito.Mockito;
import org.opensearch.client.Request;
import org.opensearch.client.Response;
import org.opensearch.client.RestClient;

import java.io.InputStream;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.nio.charset.StandardCharsets;
import java.util.Map;

/**
 * Shared setup for {@link InventoryESService} unit tests (local OpenSearch client, no network calls).
 */
final class InventoryESServiceTestSupport {

    static final Gson GSON = new GsonBuilder().serializeNulls().create();

    private InventoryESServiceTestSupport() {
    }

    static InventoryESService newService() throws Exception {
        ConfigurationDAO config = Mockito.mock(ConfigurationDAO.class);
        Mockito.when(config.isEsSignRequests()).thenReturn(false);
        Mockito.when(config.getEsHost()).thenReturn("localhost");
        Mockito.when(config.getEsPort()).thenReturn(9200);
        Mockito.when(config.getEsScheme()).thenReturn("http");

        Constructor<InventoryESService> ctor =
                InventoryESService.class.getDeclaredConstructor(ConfigurationDAO.class);
        ctor.setAccessible(true);
        return ctor.newInstance(config);
    }

    static void closeClient(InventoryESService service) throws Exception {
        Field clientField = InventoryESService.class.getDeclaredField("client");
        clientField.setAccessible(true);
        RestClient client = (RestClient) clientField.get(service);
        if (client != null) {
            client.close();
        }
    }

    /**
     * Replaces the OpenSearch client with a test double (closes any existing client first).
     */
    static void setClientForTest(InventoryESService service, RestClient client) throws Exception {
        Field clientField = InventoryESService.class.getDeclaredField("client");
        clientField.setAccessible(true);
        RestClient existing = (RestClient) clientField.get(service);
        if (existing != null) {
            existing.close();
        }
        clientField.set(service, client);
    }

    static Response mockJsonResponse(int statusCode, String jsonBody) {
        Response response = Mockito.mock(Response.class);
        org.apache.http.StatusLine statusLine = Mockito.mock(org.apache.http.StatusLine.class);
        Mockito.when(statusLine.getStatusCode()).thenReturn(statusCode);
        Mockito.when(response.getStatusLine()).thenReturn(statusLine);
        if (statusCode == 200) {
            Mockito.when(response.getEntity()).thenReturn(new StringEntity(jsonBody, ContentType.APPLICATION_JSON));
        }
        return response;
    }

    static Response mockResponseFromFixture(int statusCode, String fixtureFileName) {
        JsonObject body = loadResponseFixture(fixtureFileName);
        return mockJsonResponse(statusCode, GSON.toJson(body));
    }

    static JsonObject scrollHitsResponse(String scrollId, int hitCount, String fieldName, int idStart) {
        JsonObject root = new JsonObject();
        root.addProperty("_scroll_id", scrollId);
        JsonObject hitsWrapper = new JsonObject();
        JsonArray hits = new JsonArray();
        for (int i = 0; i < hitCount; i++) {
            JsonObject hit = new JsonObject();
            JsonObject source = new JsonObject();
            source.addProperty(fieldName, fieldName + "-" + (idStart + i));
            hit.add("_source", source);
            hits.add(hit);
        }
        hitsWrapper.add("hits", hits);
        root.add("hits", hitsWrapper);
        return root;
    }

    static JsonObject emptyScrollResponse(String scrollId) {
        return scrollHitsResponse(scrollId, 0, "participant_id", 0);
    }

    static void assertJsonRoundTrip(Map<String, Object> body) {
        String json = GSON.toJson(body);
        JsonElement tree = JsonParser.parseString(json);
        if (!tree.isJsonObject()) {
            throw new AssertionError("Expected JSON object after Gson round-trip");
        }
    }

    /**
     * Loads a canned OpenSearch response body from {@code src/test/resources/opensearch/responses/}.
     */
    static JsonObject loadResponseFixture(String fileName) {
        String path = "/opensearch/responses/" + fileName;
        try (InputStream in = InventoryESServiceTestSupport.class.getResourceAsStream(path)) {
            if (in == null) {
                throw new IllegalArgumentException("Missing classpath resource: " + path);
            }
            String text = new String(in.readAllBytes(), StandardCharsets.UTF_8);
            return JsonParser.parseString(text).getAsJsonObject();
        } catch (Exception e) {
            throw new IllegalStateException("Failed to load response fixture: " + path, e);
        }
    }
}
