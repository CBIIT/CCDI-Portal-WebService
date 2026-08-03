package gov.nih.nci.bento_ri.service;

import gov.nih.nci.bento_ri.model.FormattedCPIResponse;
import gov.nih.nci.bento_ri.model.ParticipantRequest;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;

import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Phase 3: unit tests for {@link CPIFetcherService} with mocked HTTP (no external CPI).
 */
@ExtendWith(MockitoExtension.class)
class CPIFetcherServiceTest {

    private CacheService cacheService;
    private HttpClient httpClient;

    @BeforeEach
    void setUp() {
        cacheService = new CacheService();
        httpClient = mock(HttpClient.class);
    }

    @Test
    void fetchAssociatedParticipantIds_emptyInput_returnsEmptyList() throws Exception {
        CPIFetcherService service = CPIFetcherServiceTestSupport.newServiceWithHttpClient(httpClient, cacheService);

        List<FormattedCPIResponse> result = service.fetchAssociatedParticipantIds(List.of());

        assertTrue(result.isEmpty());
    }

    @Test
    void fetchAssociatedParticipantIds_formatsAssociatedIds() throws Exception {
        when(httpClient.send(any(HttpRequest.class), any())).thenAnswer(invocation -> {
            HttpRequest request = invocation.getArgument(0);
            String uri = request.uri().toString();
            HttpResponse<String> response = mock(HttpResponse.class);
            if (uri.contains("oauth/token")) {
                when(response.statusCode()).thenReturn(200);
                when(response.body()).thenReturn(CPIFetcherServiceTestSupport.loadFixture("token_response.json"));
            } else if (uri.contains("/domains")) {
                when(response.statusCode()).thenReturn(200);
                when(response.body()).thenReturn(CPIFetcherServiceTestSupport.loadFixture("domains_response.json"));
            } else {
                when(response.statusCode()).thenReturn(200);
                when(response.body()).thenReturn(CPIFetcherServiceTestSupport.loadFixture("associated_participants_response.json"));
            }
            return response;
        });

        CPIFetcherService service = CPIFetcherServiceTestSupport.newServiceWithHttpClient(httpClient, cacheService);
        ParticipantRequest request = new ParticipantRequest("ccdi-int-p001", "CCDI-STUDY");

        List<FormattedCPIResponse> responses = service.fetchAssociatedParticipantIds(List.of(request));

        assertEquals(1, responses.size());
        FormattedCPIResponse formatted = responses.get(0);
        assertEquals("ccdi-int-p001", formatted.getParticipantId());
        assertEquals("CCDI-STUDY", formatted.getStudyId());
        assertEquals(1, formatted.getCpiData().size());
        assertEquals("assoc-p001", formatted.getCpiData().get(0).getAssociatedId());
        assertEquals("CCDI Study Domain", formatted.getCpiData().get(0).getDomainDescription());
    }

    @Test
    void fetchAssociatedParticipantIds_usesCachedDomainsOnSecondCall() throws Exception {
        when(httpClient.send(any(HttpRequest.class), any())).thenAnswer(invocation -> {
            HttpRequest request = invocation.getArgument(0);
            String uri = request.uri().toString();
            HttpResponse<String> response = mock(HttpResponse.class);
            when(response.statusCode()).thenReturn(200);
            if (uri.contains("oauth/token")) {
                when(response.body()).thenReturn(CPIFetcherServiceTestSupport.loadFixture("token_response.json"));
            } else if (uri.contains("/domains")) {
                when(response.body()).thenReturn(CPIFetcherServiceTestSupport.loadFixture("domains_response.json"));
            } else {
                when(response.body()).thenReturn(CPIFetcherServiceTestSupport.loadFixture("associated_participants_response.json"));
            }
            return response;
        });

        CPIFetcherService service = CPIFetcherServiceTestSupport.newServiceWithHttpClient(httpClient, cacheService);
        ParticipantRequest request = new ParticipantRequest("ccdi-int-p001", "CCDI-STUDY");

        service.fetchAssociatedParticipantIds(List.of(request));
        service.fetchAssociatedParticipantIds(List.of(request));

        long domainsCalls = org.mockito.Mockito.mockingDetails(httpClient).getInvocations().stream()
                .filter(inv -> inv.getArgument(0, HttpRequest.class).uri().toString().contains("/domains"))
                .count();
        assertEquals(1, domainsCalls);
    }

    @Test
    void clearDomainsCache_causesDomainsToBeFetchedAgain() throws Exception {
        when(httpClient.send(any(HttpRequest.class), any())).thenAnswer(invocation -> {
            HttpRequest request = invocation.getArgument(0);
            String uri = request.uri().toString();
            HttpResponse<String> response = mock(HttpResponse.class);
            when(response.statusCode()).thenReturn(200);
            if (uri.contains("oauth/token")) {
                when(response.body()).thenReturn(CPIFetcherServiceTestSupport.loadFixture("token_response.json"));
            } else if (uri.contains("/domains")) {
                when(response.body()).thenReturn(CPIFetcherServiceTestSupport.loadFixture("domains_response.json"));
            } else {
                when(response.body()).thenReturn(CPIFetcherServiceTestSupport.loadFixture("associated_participants_response.json"));
            }
            return response;
        });

        CPIFetcherService service = CPIFetcherServiceTestSupport.newServiceWithHttpClient(httpClient, cacheService);
        ParticipantRequest request = new ParticipantRequest("ccdi-int-p001", "CCDI-STUDY");
        service.fetchAssociatedParticipantIds(List.of(request));
        service.clearDomainsCache();
        service.fetchAssociatedParticipantIds(List.of(request));

        long domainsCalls = org.mockito.Mockito.mockingDetails(httpClient).getInvocations().stream()
                .filter(inv -> inv.getArgument(0, HttpRequest.class).uri().toString().contains("/domains"))
                .count();
        assertEquals(2, domainsCalls);
    }
}
