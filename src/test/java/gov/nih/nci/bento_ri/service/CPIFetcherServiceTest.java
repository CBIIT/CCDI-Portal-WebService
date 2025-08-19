package gov.nih.nci.bento_ri.service;

import org.junit.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.TestPropertySource;

/**
 * Example test for CPIFetcherService
 * Note: This test requires actual OAuth2 credentials to run successfully
 */
@SpringBootTest
@TestPropertySource(properties = {
    "oauth2.client.id=${OAUTH2_CLIENT_ID:test_client_id}",
    "oauth2.client.secret=${OAUTH2_CLIENT_SECRET:test_client_secret}",
    "oauth2.token.uri=${OAUTH2_TOKEN_URI:https://example.com/token}",
    "api.url=https://participantindex.ccdi.cancer.gov/v1/associated_participant_ids",
    "oauth2.scope=custom"
})
public class CPIFetcherServiceTest {

    /**
     * Example test showing how to use the CPI Fetcher Service
     * This test is disabled by default as it requires actual credentials
     */
    @Test
    public void testFetchAssociatedParticipantIds_Example() {
        // Example usage - uncomment and provide real credentials to test
        /*
        import gov.nih.nci.bento_ri.model.ParticipantRequest;
        import java.util.Arrays;
        import java.util.List;
        import java.util.Map;
        
        CPIFetcherService service = new CPIFetcherService();
        
        List<ParticipantRequest> requests = Arrays.asList(
            new ParticipantRequest("COG_PAMUPE", "pcdc"),
            new ParticipantRequest("PARTICIPANT_123", "study_abc")
        );
        
        try {
            Map<String, Object> response = service.fetchAssociatedParticipantIds(requests);
            System.out.println("API Response: " + response);
        } catch (Exception e) {
            System.err.println("Error: " + e.getMessage());
        }
        */
    }
}
