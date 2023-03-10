package gov.nih.nci.bento.model.search.filter;

import gov.nih.nci.bento.constants.Const;
import gov.nih.nci.bento.model.search.query.QueryFactory;
import org.opensearch.index.query.BoolQueryBuilder;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.search.aggregations.AggregationBuilders;
import org.opensearch.search.builder.SearchSourceBuilder;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class NestedFilter extends AbstractFilter {

    public NestedFilter(FilterParam param) {
        super(param);
    }

    @Override
    SearchSourceBuilder getFilter(FilterParam param, QueryFactory bentoParam) {
        return new SearchSourceBuilder()
                .size(0)
                .query(bentoParam.getQuery())
                .aggregation(AggregationBuilders
                        .nested(Const.ES_PARAMS.NESTED_SEARCH, param.getNestedPath())
                        .subAggregation(
                                AggregationBuilders
                                        .filter(Const.ES_PARAMS.NESTED_FILTER, getNestedQuery(param))
                                        .subAggregation(
                                                AggregationBuilders.terms(Const.ES_PARAMS.TERMS_AGGS)
                                                        .size(Const.ES_PARAMS.AGGS_SIZE)
                                                        .field(param.getNestedPath() + "." + param.getSelectedField()))));
    }

    @SuppressWarnings("unchecked")
    private QueryBuilder getNestedQuery(FilterParam filterParam) {
        BoolQueryBuilder boolQueryBuilder = new BoolQueryBuilder();
        // Multiple nested fields
        // for the purpose of total number of aggregation & filter inner fields
        Set<String> nestedParameters = new HashSet<>(filterParam.getNestedParameters());
        removeFilterField(filterParam, nestedParameters);
        filterParam.getArgs().forEach((k,v)->{
            if (nestedParameters.contains(k)) {
                List<String> list = filterParam.getArgs().containsKey(k) ? (List<String>) filterParam.getArgs().get(k) : new ArrayList<>();
                if (list.size() > 0) {
                    list.forEach(l->
                        boolQueryBuilder.should(QueryBuilders.termQuery(filterParam.getNestedPath() + "." + k, l))
                    );
                }
            }
        });
        return boolQueryBuilder;
    }

    private void removeFilterField(FilterParam filterParam, Set<String> nestedParameters) {
        if (filterParam.isIgnoreSelectedField() && nestedParameters.contains(filterParam.getSelectedField())) {
            nestedParameters.remove(filterParam.getSelectedField());
        }
    }
}
