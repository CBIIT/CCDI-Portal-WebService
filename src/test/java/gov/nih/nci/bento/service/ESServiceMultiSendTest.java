package gov.nih.nci.bento.service;

import gov.nih.nci.bento.model.search.MultipleRequests;
import gov.nih.nci.bento.model.search.mapper.TypeMapper;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.action.search.MultiSearchResponse;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.client.RequestOptions;
import org.opensearch.client.RestHighLevelClient;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Phase 5: unit tests for {@link ESService#elasticMultiSend} with mocked high-level client.
 */
@ExtendWith(MockitoExtension.class)
class ESServiceMultiSendTest {

    private ESService service;
    private RestHighLevelClient mockHighLevelClient;

    @BeforeEach
    void setUp() throws Exception {
        service = ESServiceTestSupport.newService();
        mockHighLevelClient = mock(RestHighLevelClient.class);
        ESServiceTestSupport.setRestHighLevelClientForTest(service, mockHighLevelClient);
    }

    @AfterEach
    void tearDown() throws Exception {
        ESServiceTestSupport.closeClient(service);
    }

    @Test
    void elasticMultiSend_mapsEachNamedResponseThroughTypeMapper() throws IOException {
        MultiSearchResponse multiSearchResponse = mock(MultiSearchResponse.class);
        MultiSearchResponse.Item item = mock(MultiSearchResponse.Item.class);
        SearchResponse searchResponse = mock(SearchResponse.class);
        when(item.getResponse()).thenReturn(searchResponse);
        when(multiSearchResponse.getResponses()).thenReturn(new MultiSearchResponse.Item[] {item});
        when(mockHighLevelClient.msearch(any(), eq(RequestOptions.DEFAULT))).thenReturn(multiSearchResponse);

        TypeMapper typeMapper = mock(TypeMapper.class);
        when(typeMapper.get(searchResponse)).thenReturn(List.of("bucket-a", "bucket-b"));

        MultipleRequests<List<String>> request = MultipleRequests.<List<String>>builder()
                .name("programs")
                .request(new SearchRequest("programs"))
                .typeMapper(typeMapper)
                .build();

        Map<String, List<String>> result = service.elasticMultiSend(List.of(request));

        assertEquals(List.of("bucket-a", "bucket-b"), result.get("programs"));
    }
}
