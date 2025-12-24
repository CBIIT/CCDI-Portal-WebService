package gov.nih.nci.bento;

import gov.nih.nci.bento.controller.IndexController;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.test.context.junit4.SpringRunner;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.MvcResult;
import org.springframework.test.web.servlet.request.MockMvcRequestBuilders;
import org.springframework.test.web.servlet.result.MockMvcResultMatchers;

import static org.junit.Assert.assertNotNull;

@RunWith(SpringRunner.class)
@WebMvcTest(IndexController.class)
public class IndexControllerTest {

    @Autowired
    private MockMvc mockMvc;


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

}
