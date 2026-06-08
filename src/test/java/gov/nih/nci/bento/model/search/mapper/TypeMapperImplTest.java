package gov.nih.nci.bento.model.search.mapper;

import gov.nih.nci.bento.constants.Const;
import org.apache.lucene.search.TotalHits;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.search.SearchHit;
import org.opensearch.search.SearchHits;
import org.opensearch.search.aggregations.Aggregations;
import org.opensearch.search.aggregations.bucket.terms.Terms;
import org.opensearch.search.aggregations.metrics.ParsedMax;
import org.opensearch.search.aggregations.metrics.ParsedMin;
import org.opensearch.search.aggregations.metrics.Sum;
import org.opensearch.search.fetch.subphase.highlight.HighlightField;
import org.opensearch.core.common.text.Text;

import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Phase 3: unit tests for {@link TypeMapperImpl} OpenSearch response mappers.
 */
class TypeMapperImplTest {

    private TypeMapperImpl mapper;

    @BeforeEach
    void setUp() {
        mapper = new TypeMapperImpl();
    }

    @Test
    void getList_mapsRequestedSourceFields() {
        SearchResponse response = mock(SearchResponse.class);
        SearchHits hits = mock(SearchHits.class);
        SearchHit hit = mock(SearchHit.class);
        when(response.getHits()).thenReturn(hits);
        when(hits.getHits()).thenReturn(new SearchHit[] {hit});
        when(hit.getSourceAsMap()).thenReturn(Map.of(
                "file_name", "sample.bam",
                "file_id", "f001"));

        List<Map<String, Object>> rows = mapper.getList(Set.of("file_name", "file_id")).get(response);

        assertEquals(1, rows.size());
        assertEquals("sample.bam", rows.get(0).get("file_name"));
        assertEquals("f001", rows.get(0).get("file_id"));
    }

    @Test
    void getStrList_extractsFieldFromHits() {
        SearchResponse response = mock(SearchResponse.class);
        SearchHits hits = mock(SearchHits.class);
        SearchHit hit = mock(SearchHit.class);
        when(response.getHits()).thenReturn(hits);
        when(hits.getHits()).thenReturn(new SearchHit[] {hit});
        when(hit.getSourceAsMap()).thenReturn(Map.of("programs", "CCDI"));

        List<String> values = mapper.getStrList("programs").get(response);

        assertEquals(List.of("CCDI"), values);
    }

    @Test
    void getStrList_missingField_throws() {
        SearchResponse response = mock(SearchResponse.class);
        SearchHits hits = mock(SearchHits.class);
        SearchHit hit = mock(SearchHit.class);
        when(response.getHits()).thenReturn(hits);
        when(hits.getHits()).thenReturn(new SearchHit[] {hit});
        when(hit.getSourceAsMap()).thenReturn(Map.of());

        assertThrows(IllegalArgumentException.class,
                () -> mapper.getStrList("programs").get(response));
    }

    @Test
    void getIntTotal_returnsHitCount() {
        SearchResponse response = mock(SearchResponse.class);
        SearchHits hits = mock(SearchHits.class);
        when(response.getHits()).thenReturn(hits);
        when(hits.getTotalHits()).thenReturn(new TotalHits(99, TotalHits.Relation.EQUAL_TO));

        Long total = mapper.getIntTotal().get(response);

        assertEquals(99L, total);
    }

    @Test
    void getAggregate_mapsTermsBucketsToGroupCounts() {
        SearchResponse response = mock(SearchResponse.class);
        Terms terms = mock(Terms.class);
        Terms.Bucket bucket = mock(Terms.Bucket.class);
        Aggregations aggregations = mock(Aggregations.class);
        when(response.getAggregations()).thenReturn(aggregations);
        when(aggregations.get(Const.ES_PARAMS.TERMS_AGGS)).thenReturn(terms);
        doReturn(bucketList(bucket)).when(terms).getBuckets();
        when(bucket.getKey()).thenReturn("Program A");
        when(bucket.getDocCount()).thenReturn(12L);

        List<Map<String, Object>> groups = mapper.getAggregate().get(response);

        assertEquals(1, groups.size());
        assertEquals("Program A", groups.get(0).get(Const.BENTO_FIELDS.GROUP));
        assertEquals(12L, groups.get(0).get(Const.BENTO_FIELDS.SUBJECTS));
    }

    @Test
    void getAggregateTotalCnt_includesOtherDocCounts() {
        SearchResponse response = mock(SearchResponse.class);
        Terms terms = mock(Terms.class);
        Terms.Bucket bucket = mock(Terms.Bucket.class);
        Aggregations aggregations = mock(Aggregations.class);
        when(response.getAggregations()).thenReturn(aggregations);
        when(aggregations.get(Const.ES_PARAMS.TERMS_AGGS)).thenReturn(terms);
        doReturn(bucketList(bucket)).when(terms).getBuckets();
        when(terms.getSumOfOtherDocCounts()).thenReturn(3L);

        Integer count = mapper.getAggregateTotalCnt().get(response);

        assertEquals(4, count);
    }

    @Test
    void getRange_returnsMinMaxWhenHitsExist() {
        SearchResponse response = mock(SearchResponse.class);
        SearchHits hits = mock(SearchHits.class);
        Aggregations aggregations = mock(Aggregations.class);
        ParsedMin min = mock(ParsedMin.class);
        ParsedMax max = mock(ParsedMax.class);
        when(response.getHits()).thenReturn(hits);
        when(hits.getTotalHits()).thenReturn(new TotalHits(5, TotalHits.Relation.EQUAL_TO));
        when(response.getAggregations()).thenReturn(aggregations);
        when(aggregations.getAsMap()).thenReturn(Map.of("min", min, "max", max));
        when(min.getValue()).thenReturn(2.0);
        when(max.getValue()).thenReturn(18.0);

        Map<String, Object> range = mapper.getRange().get(response);

        assertEquals(2.0f, range.get(Const.YAML_QUERY.RESULT_TYPE.RANGE_PARAMS.LOWER_BOUND));
        assertEquals(18.0f, range.get(Const.YAML_QUERY.RESULT_TYPE.RANGE_PARAMS.UPPER_BOUND));
        assertEquals(5L, range.get("subjects"));
    }

    @Test
    void getRange_noHits_returnsZeroBounds() {
        SearchResponse response = mock(SearchResponse.class);
        SearchHits hits = mock(SearchHits.class);
        Aggregations aggregations = mock(Aggregations.class);
        ParsedMin min = mock(ParsedMin.class);
        ParsedMax max = mock(ParsedMax.class);
        when(response.getHits()).thenReturn(hits);
        when(hits.getTotalHits()).thenReturn(new TotalHits(0, TotalHits.Relation.EQUAL_TO));
        when(response.getAggregations()).thenReturn(aggregations);
        when(aggregations.getAsMap()).thenReturn(Map.of("min", min, "max", max));

        Map<String, Object> range = mapper.getRange().get(response);

        assertEquals(0.0f, range.get(Const.YAML_QUERY.RESULT_TYPE.RANGE_PARAMS.LOWER_BOUND));
        assertEquals(0.0f, range.get(Const.YAML_QUERY.RESULT_TYPE.RANGE_PARAMS.UPPER_BOUND));
    }

    @Test
    void getSumAggregate_returnsFloatValue() {
        SearchResponse response = mock(SearchResponse.class);
        Sum sum = mock(Sum.class);
        Aggregations aggregations = mock(Aggregations.class);
        when(response.getAggregations()).thenReturn(aggregations);
        when(aggregations.get(Const.ES_PARAMS.TERMS_AGGS)).thenReturn(sum);
        when(sum.getValue()).thenReturn(42.5);

        Float total = mapper.getSumAggregate().get(response);

        assertEquals(42.5f, total);
    }

    @Test
    void getMapWithHighlightedFields_addsHighlightToReturnMap() {
        SearchResponse response = mock(SearchResponse.class);
        SearchHits hits = mock(SearchHits.class);
        SearchHit hit = mock(SearchHit.class);
        HighlightField highlightField = mock(HighlightField.class);
        when(response.getHits()).thenReturn(hits);
        when(hits.getHits()).thenReturn(new SearchHit[] {hit});
        when(hit.getSourceAsMap()).thenReturn(Map.of("title", "About CCDI"));
        when(hit.getHighlightFields()).thenReturn(Map.of("title", highlightField));
        when(highlightField.getFragments()).thenReturn(new Text[] {new Text("About <em>CCDI</em>")});

        List<Map<String, Object>> rows = mapper.getMapWithHighlightedFields(Set.of("title")).get(response);

        assertEquals(1, rows.size());
        assertEquals("About CCDI", rows.get(0).get("title"));
        assertTrue(rows.get(0).get(Const.BENTO_FIELDS.HIGHLIGHT).toString().contains("CCDI"));
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private static List bucketList(Terms.Bucket bucket) {
        return List.of(bucket);
    }
}
