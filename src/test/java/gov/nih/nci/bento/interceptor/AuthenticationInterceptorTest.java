package gov.nih.nci.bento.interceptor;

import com.google.gson.JsonParser;
import gov.nih.nci.bento.model.ConfigurationDAO;
import jakarta.servlet.http.Cookie;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.lang.reflect.Field;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;

import com.sun.net.httpserver.HttpServer;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Phase 7: unit tests for {@link AuthenticationInterceptor}.
 */
@ExtendWith(MockitoExtension.class)
class AuthenticationInterceptorTest {

    private ConfigurationDAO config;
    private AuthenticationInterceptor interceptor;
    private HttpServletRequest request;
    private HttpServletResponse response;
    private StringWriter responseBody;
    private HttpServer authServer;

    @BeforeEach
    void setUp() throws Exception {
        config = mock(ConfigurationDAO.class);
        interceptor = new AuthenticationInterceptor();
        injectConfig(interceptor, config);

        request = mock(HttpServletRequest.class);
        response = mock(HttpServletResponse.class);
        responseBody = new StringWriter();
        lenient().when(response.getWriter()).thenReturn(new PrintWriter(responseBody, true));
    }

    @AfterEach
    void tearDown() {
        if (authServer != null) {
            authServer.stop(0);
        }
    }

    @Test
    void preHandle_authDisabled_allowsRequest() throws Exception {
        when(config.isAuthEnabled()).thenReturn(false);

        assertTrue(interceptor.preHandle(request, response, new Object()));
    }

    @Test
    void preHandle_nonPrivateEndpoint_allowsRequest() throws Exception {
        when(config.isAuthEnabled()).thenReturn(true);
        when(request.getServletPath()).thenReturn("/version");

        assertTrue(interceptor.preHandle(request, response, new Object()));
    }

    @Test
    void preHandle_privateEndpointWithoutCookies_rejectsRequest() throws Exception {
        when(config.isAuthEnabled()).thenReturn(true);
        when(request.getServletPath()).thenReturn("/v1/graphql/");
        when(request.getCookies()).thenReturn(null);

        assertFalse(interceptor.preHandle(request, response, new Object()));
        assertTrue(responseBody.toString().contains("You must be logged in to use this API"));
    }

    @Test
    void preHandle_validSessionCookie_allowsRequest() throws Exception {
        startAuthServer(200, "{\"status\":true}");
        when(config.isAuthEnabled()).thenReturn(true);
        when(config.getAuthEndpoint()).thenReturn("http://localhost:" + authServer.getAddress().getPort());
        when(request.getServletPath()).thenReturn("/v1/graphql/");
        when(request.getCookies()).thenReturn(new Cookie[] {new Cookie("session", "valid-token")});

        assertTrue(interceptor.preHandle(request, response, new Object()));
    }

    @Test
    void preHandle_authenticationServiceReturnsFalse_rejectsRequest() throws Exception {
        startAuthServer(200, "{\"status\":false}");
        when(config.isAuthEnabled()).thenReturn(true);
        when(config.getAuthEndpoint()).thenReturn("http://localhost:" + authServer.getAddress().getPort());
        when(request.getServletPath()).thenReturn("/v1/graphql/");
        when(request.getCookies()).thenReturn(new Cookie[] {new Cookie("session", "invalid-token")});

        assertFalse(interceptor.preHandle(request, response, new Object()));
        assertTrue(responseBody.toString().contains("You must be logged in to use this API"));
    }

    @Test
    void preHandle_authenticationServiceNonOk_rejectsRequest() throws Exception {
        startAuthServer(500, "server error");
        when(config.isAuthEnabled()).thenReturn(true);
        when(config.getAuthEndpoint()).thenReturn("http://localhost:" + authServer.getAddress().getPort());
        when(request.getServletPath()).thenReturn("/v1/graphql/");
        when(request.getCookies()).thenReturn(new Cookie[] {new Cookie("session", "token")});

        assertFalse(interceptor.preHandle(request, response, new Object()));
        assertTrue(responseBody.toString().contains("You must be logged in to use this API"));
    }

    @Test
    void preHandle_authenticationServiceInvalidJson_rejectsWithParseError() throws Exception {
        startAuthServer(200, "not-json");
        when(config.isAuthEnabled()).thenReturn(true);
        when(config.getAuthEndpoint()).thenReturn("http://localhost:" + authServer.getAddress().getPort());
        when(request.getServletPath()).thenReturn("/v1/graphql/");
        when(request.getCookies()).thenReturn(new Cookie[] {new Cookie("session", "token")});

        assertFalse(interceptor.preHandle(request, response, new Object()));
        var body = JsonParser.parseString(responseBody.toString()).getAsJsonObject();
        assertTrue(body.getAsJsonArray("errors").get(0).getAsJsonObject().get("message").getAsString()
                .contains("parsing the response"));
    }

    private void startAuthServer(int statusCode, String body) throws Exception {
        authServer = HttpServer.create(new InetSocketAddress("localhost", 0), 0);
        authServer.createContext(
                "/authenticated",
                exchange -> {
                    byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
                    exchange.sendResponseHeaders(statusCode, bytes.length);
                    exchange.getResponseBody().write(bytes);
                    exchange.close();
                });
        authServer.start();
    }

    private static void injectConfig(AuthenticationInterceptor target, ConfigurationDAO configurationDAO)
            throws Exception {
        Field configField = AuthenticationInterceptor.class.getDeclaredField("config");
        configField.setAccessible(true);
        configField.set(target, configurationDAO);
    }
}
