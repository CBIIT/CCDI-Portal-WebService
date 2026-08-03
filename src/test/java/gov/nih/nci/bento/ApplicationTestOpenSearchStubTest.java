package gov.nih.nci.bento;

import org.junit.jupiter.api.Test;

import java.io.InputStream;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

/**
 * Unit test for the {@code test} profile OpenSearch/Elasticsearch settings in
 * {@code application-test.properties}. That file is the webservice's stand-in for a real search
 * cluster during {@code mvn test}: filtering must stay off so GraphQL/ES paths do not require a
 * live OpenSearch (contrast with {@code application-integration.properties} and
 * {@link gov.nih.nci.integration.OpenSearchIntegrationTest}).
 */
class ApplicationTestOpenSearchStubTest {

    @Test
    void applicationTestPropertiesDefineLocalStubOpenSearch() throws Exception {
        Properties p = new Properties();
        try (InputStream in = getClass().getResourceAsStream("/application-test.properties")) {
            assertNotNull(in, "classpath:application-test.properties must exist");
            p.load(in);
        }

        assertEquals("localhost", p.getProperty("es.host"), "Unit tests should target localhost, not deployed OpenSearch");
        assertEquals("9200", p.getProperty("es.port"));
        assertEquals("http", p.getProperty("es.scheme"));
        assertEquals("false", p.getProperty("es.filter.enabled"),
                "es.filter.enabled must be false so unit tests do not require OpenSearch");
        assertEquals("false", p.getProperty("es.sign.requests"));
    }
}
