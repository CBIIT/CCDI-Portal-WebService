package gov.nih.nci.bento;

import gov.nih.nci.bento.controller.IndexController;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.http.MediaType;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.test.context.junit.jupiter.SpringExtension;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.MvcResult;
import org.springframework.test.web.servlet.request.MockMvcRequestBuilders;
import org.springframework.test.web.servlet.result.MockMvcResultMatchers;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;

import static org.junit.jupiter.api.Assertions.assertNotNull;

/**
 * Unit test for IndexController
 * This is a lightweight unit test that doesn't load the full Spring context
 */
@ExtendWith(MockitoExtension.class)
public class IndexControllerTest {

    private MockMvc mockMvc;

    @BeforeEach
    public void setup() {
        // Create a standalone MockMvc without loading the full Spring context
        IndexController controller = new IndexController();
        this.mockMvc = MockMvcBuilders.standaloneSetup(controller).build();
    }

    /**
     * Confirm that the "/ping" endpoint does NOT accept POST requests and verify the following within the response:
     *     Http Status Code is 405 (METHOD NOT ALLOWED)
     *
     * @throws Exception
     */
    @Test
    public void pingEndpointTestPOST() throws Exception {
        MvcResult result = this.mockMvc.perform(MockMvcRequestBuilders.post("/ping"))
                .andExpect(MockMvcResultMatchers.status().isMethodNotAllowed())
                .andReturn();
        //assert method to satisfy codacy requirement, this statement will not be reached if the test fails
        assertNotNull(result);
    }

    /**
     * Test that the "/ping" endpoint accepts GET requests
     *
     * @throws Exception
     */
    @Test
    public void pingEndpointTestGET() throws Exception {
        MvcResult result = this.mockMvc.perform(MockMvcRequestBuilders.get("/ping")
                        .accept(MediaType.APPLICATION_JSON))
                .andExpect(MockMvcResultMatchers.status().isOk())
                .andReturn();
        assertNotNull(result);
    }

}
