package gov.nih.nci.bento.service;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import gov.nih.nci.bento.model.ConfigurationDAO;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.mockito.Mockito;
import org.opensearch.client.Response;
import org.opensearch.client.RestClient;

import java.io.InputStream;
import java.lang.reflect.Field;
import java.nio.charset.StandardCharsets;

/**
 * Shared setup for {@link ESService} unit tests (local OpenSearch client, no network calls).
 */
final class ESServiceTestSupport {

    static final Gson GSON = new GsonBuilder().serializeNulls().create();

    private ESServiceTestSupport() {
    }

    static ESService newService() {
        ConfigurationDAO config = Mockito.mock(ConfigurationDAO.class);
        Mockito.when(config.isEsSignRequests()).thenReturn(false);
        Mockito.when(config.getEsHost()).thenReturn("localhost");
        Mockito.when(config.getEsPort()).thenReturn(9200);
        Mockito.when(config.getEsScheme()).thenReturn("http");
        return new ESService(config);
    }

    static void closeClient(ESService service) throws Exception {
        Field clientField = ESService.class.getDeclaredField("client");
        clientField.setAccessible(true);
        RestClient client = (RestClient) clientField.get(service);
        if (client != null) {
            client.close();
        }
    }

    static void setClientForTest(ESService service, RestClient client) throws Exception {
        Field clientField = ESService.class.getDeclaredField("client");
        clientField.setAccessible(true);
        RestClient existing = (RestClient) clientField.get(service);
        if (existing != null) {
            existing.close();
        }
        clientField.set(service, client);
    }

    static JsonObject loadResponseFixture(String fileName) {
        String path = "/opensearch/responses/" + fileName;
        try (InputStream in = ESServiceTestSupport.class.getResourceAsStream(path)) {
            if (in == null) {
                throw new IllegalArgumentException("Missing classpath resource: " + path);
            }
            String text = new String(in.readAllBytes(), StandardCharsets.UTF_8);
            return JsonParser.parseString(text).getAsJsonObject();
        } catch (Exception e) {
            throw new IllegalStateException("Failed to load response fixture: " + path, e);
        }
    }

    static void assertJsonRoundTrip(Object body) {
        String json = GSON.toJson(body);
        JsonParser.parseString(json);
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
}
