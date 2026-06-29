package gov.nih.nci.bento.controller;

import com.google.gson.JsonParser;
import gov.nih.nci.bento.graphql.BentoGraphQL;
import gov.nih.nci.bento.model.ConfigurationDAO;
import gov.nih.nci.bento.support.GraphQLTestSupport;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.MvcResult;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;

import java.util.Collections;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.content;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

/**
 * Phase 7: unit tests for {@link GraphQLController} with mocked GraphQL engines.
 */
@ExtendWith(MockitoExtension.class)
class GraphQLControllerTest {

    private MockMvc mockMvc;
    private ConfigurationDAO config;
    private BentoGraphQL bentoGraphQL;

    @BeforeEach
    void setUp() {
        config = mock(ConfigurationDAO.class);
        bentoGraphQL = mock(BentoGraphQL.class);
        GraphQLController controller = new GraphQLController(config, bentoGraphQL);
        mockMvc = MockMvcBuilders.standaloneSetup(controller).build();
    }

    private void stubPrivateGraphQL() throws Exception {
        when(config.isAllowGraphQLQuery()).thenReturn(true);
        when(bentoGraphQL.getPrivateGraphQL()).thenReturn(GraphQLTestSupport.graphQLWithQueryFields(
                Collections.singletonMap("esVersion", env -> "private-1.0")));
    }

    private void stubPublicGraphQL() throws Exception {
        when(config.isAllowGraphQLQuery()).thenReturn(true);
        when(bentoGraphQL.getPublicGraphQL()).thenReturn(GraphQLTestSupport.graphQLWithQueryFields(
                Collections.singletonMap("esVersion", env -> "public-2.0")));
    }

    private void stubMutationDisabled() {
        lenient().when(config.isAllowGraphQLQuery()).thenReturn(true);
        when(config.isAllowGraphQLMutation()).thenReturn(false);
    }

    @Test
    void getVersion_returnsConfiguredApiVersion() throws Exception {
        when(config.getBentoApiVersion()).thenReturn("9.9.9");

        mockMvc.perform(get("/version").accept(MediaType.APPLICATION_JSON))
                .andExpect(status().isOk())
                .andExpect(content().json("{\"version\":\"9.9.9\"}"));
    }

    @Test
    void getPrivateGraphQLResponseByGET_returnsMethodNotAllowed() throws Exception {
        MvcResult result = mockMvc.perform(get("/v1/graphql/").accept(MediaType.APPLICATION_JSON))
                .andExpect(status().isMethodNotAllowed())
                .andReturn();

        var body = JsonParser.parseString(result.getResponse().getContentAsString()).getAsJsonObject();
        assertEquals(
                "API will only accept POST requests",
                body.getAsJsonArray("errors").get(0).getAsJsonObject().get("message").getAsString());
    }

    @Test
    void getPrivateGraphQLResponse_invalidJson_returnsBadRequest() throws Exception {
        mockMvc.perform(post("/v1/graphql/")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"variables\":{}}"))
                .andExpect(status().isBadRequest());
    }

    @Test
    void getPrivateGraphQLResponse_validQuery_returnsGraphQLResult() throws Exception {
        stubPrivateGraphQL();
        String body = "{\"query\":\"{ esVersion }\",\"variables\":{}}";

        MvcResult result = mockMvc.perform(post("/v1/graphql/")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(body))
                .andExpect(status().isOk())
                .andReturn();

        var json = JsonParser.parseString(result.getResponse().getContentAsString()).getAsJsonObject();
        assertEquals("private-1.0", json.getAsJsonObject("data").get("esVersion").getAsString());
    }

    @Test
    void getPublicGraphQLResponse_validQuery_returnsGraphQLResult() throws Exception {
        stubPublicGraphQL();
        String body = "{\"query\":\"{ esVersion }\",\"variables\":{}}";

        MvcResult result = mockMvc.perform(post("/v1/public-graphql/")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(body))
                .andExpect(status().isOk())
                .andReturn();

        var json = JsonParser.parseString(result.getResponse().getContentAsString()).getAsJsonObject();
        assertEquals("public-2.0", json.getAsJsonObject("data").get("esVersion").getAsString());
    }

    @Test
    void getPrivateGraphQLResponse_mutationWhenDisabled_returnsForbidden() throws Exception {
        stubMutationDisabled();
        String body = "{\"query\":\"mutation { noop }\",\"variables\":{}}";

        MvcResult result = mockMvc.perform(post("/v1/graphql/")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(body))
                .andExpect(status().isForbidden())
                .andReturn();

        var json = JsonParser.parseString(result.getResponse().getContentAsString()).getAsJsonObject();
        assertEquals(
                "mutation operations have been disabled in the application configuration.",
                json.getAsJsonArray("errors").get(0).getAsJsonObject().get("message").getAsString());
    }

    @Test
    void getOpenSearchVersion_delegatesToPublicGraphQL() throws Exception {
        when(bentoGraphQL.getPublicGraphQL()).thenReturn(GraphQLTestSupport.graphQLWithQueryFields(
                Collections.singletonMap("esVersion", env -> "public-2.0")));
        when(config.isAllowGraphQLQuery()).thenReturn(true);

        mockMvc.perform(get("/opensearch-version").accept(MediaType.APPLICATION_JSON))
                .andExpect(status().isOk())
                .andExpect(content().json("{\"version\":\"public-2.0\"}"));
    }
}
