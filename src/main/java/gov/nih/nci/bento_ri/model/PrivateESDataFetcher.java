package gov.nih.nci.bento_ri.model;

import gov.nih.nci.bento.constants.Const;
import gov.nih.nci.bento.model.AbstractPrivateESDataFetcher;
import gov.nih.nci.bento.model.search.mapper.TypeMapperImpl;
import gov.nih.nci.bento.model.search.mapper.TypeMapperService;
import gov.nih.nci.bento.model.search.yaml.YamlQueryFactory;
import gov.nih.nci.bento.service.ESService;
import gov.nih.nci.bento_ri.service.InventoryESService;
import graphql.schema.idl.RuntimeWiring;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.client.Request;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.github.benmanes.caffeine.cache.Cache;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;

import java.io.File;
import java.io.IOException;
import java.util.*;

import static graphql.schema.idl.TypeRuntimeWiring.newTypeWiring;

@Component
public class PrivateESDataFetcher extends AbstractPrivateESDataFetcher {
    private static final Logger logger = LogManager.getLogger(PrivateESDataFetcher.class);
    private final YamlQueryFactory yamlQueryFactory;
    private InventoryESService inventoryESService;
    @Autowired
    private Cache<String, Object> caffeineCache;

    // parameters used in queries
    final String PAGE_SIZE = "first";
    final String OFFSET = "offset";
    final String ORDER_BY = "order_by";
    final String SORT_DIRECTION = "sort_direction";

    final String STUDIES_FACET_END_POINT = "/study_participants/_search";
    final String PARTICIPANTS_END_POINT = "/participants/_search";
    final String DIAGNOSIS_END_POINT = "/diagnosis/_search";
    final String STUDIES_END_POINT = "/studies/_search";
    final String SAMPLES_END_POINT = "/samples/_search";
    final String FILES_END_POINT = "/files/_search";

    final String PARTICIPANTS_COUNT_END_POINT = "/participants/_count";
    final String DIAGNOSIS_COUNT_END_POINT = "/diagnosis/_count";
    final String STUDIES_COUNT_END_POINT = "/studies/_count";
    final String SAMPLES_COUNT_END_POINT = "/samples/_count";
    final String FILES_COUNT_END_POINT = "/files/_count";

    final String ADDITIONAL_UPDATE = "additional_update";

    final Set<String> RANGE_PARAMS = Set.of("age_at_diagnosis", "participant_age_at_collection");

    final Set<String> BOOLEAN_PARAMS = Set.of("assay_method");

    final Set<String> ARRAY_PARAMS = Set.of("file_type");

    final Set<String> INCLUDE_PARAMS  = Set.of("race");

    final Set<String> REGULAR_PARAMS = Set.of("study_id", "participant_id", "race", "sex_at_birth", "diagnosis", "disease_phase", "diagnosis_classification_system", "diagnosis_basis", "tumor_grade_source", "tumor_stage_source", "diagnosis_anatomic_site", "age_at_diagnosis", "last_known_survival_status", "sample_anatomic_site", "participant_age_at_collection", "sample_tumor_status", "tumor_classification", "assay_method", "file_type", "dbgap_accession", "study_acronym", "study_short_title", "library_selection", "library_source_material", "library_source_molecule", "library_strategy");
    final Set<String> PARTICIPANT_REGULAR_PARAMS = Set.of("participant_id", "race", "sex_at_birth", "diagnosis", "disease_phase", "diagnosis_classification_system", "diagnosis_basis", "tumor_grade_source", "tumor_stage_source", "diagnosis_anatomic_site", "age_at_diagnosis", "last_known_survival_status", "sample_anatomic_site", "participant_age_at_collection", "sample_tumor_status", "tumor_classification", "assay_method", "file_type", "dbgap_accession", "study_acronym", "study_short_title", "library_selection", "library_source_material", "library_source_molecule", "library_strategy");
    final Set<String> DIAGNOSIS_REGULAR_PARAMS = Set.of("participant_id", "sample_id", "race", "sex_at_birth", "dbgap_accession", "study_acronym", "study_name", "diagnosis", "disease_phase", "diagnosis_classification_system", "diagnosis_basis", "tumor_grade_source", "tumor_stage_source", "diagnosis_anatomic_site", "age_at_diagnosis");
    final Set<String> SAMPLE_REGULAR_PARAMS = Set.of("participant_id", "race", "sex_at_birth", "dbgap_accession", "study_acronym", "study_name", "sample_anatomic_site", "participant_age_at_collection", "sample_tumor_status", "tumor_classification");
    final Set<String> STUDY_REGULAR_PARAMS = Set.of("study_id", "dbgap_accession", "study_acronym", "study_name");
    final Set<String> FILE_REGULAR_PARAMS = Set.of("file_category", "dbgap_accession", "study_acronym", "study_name", "file_type", "library_selection", "library_source_material", "library_source_molecule", "library_strategy");

    public PrivateESDataFetcher(InventoryESService esService) {
        super(esService);
        inventoryESService = esService;
        yamlQueryFactory = new YamlQueryFactory(esService);
    }

    @Override
    public RuntimeWiring buildRuntimeWiring() throws IOException {
        return RuntimeWiring.newRuntimeWiring()
                .type(newTypeWiring("QueryType")
                        .dataFetchers(yamlQueryFactory.createYamlQueries(Const.ES_ACCESS_TYPE.PRIVATE))
                        .dataFetcher("idsLists", env -> idsLists())
                        .dataFetcher("searchParticipants", env -> {
                            Map<String, Object> args = env.getArguments();
                            return searchParticipants(args);
                        })
                        .dataFetcher("participantOverview", env -> {
                            Map<String, Object> args = env.getArguments();
                            return participantOverview(args);
                        })
                        .dataFetcher("diagnosisOverview", env -> {
                            Map<String, Object> args = env.getArguments();
                            return diagnosisOverview(args);
                        })
                        .dataFetcher("studyOverview", env -> {
                            Map<String, Object> args = env.getArguments();
                            return studyOverview(args);
                        })
                        .dataFetcher("sampleOverview", env -> {
                            Map<String, Object> args = env.getArguments();
                            return sampleOverview(args);
                        })
                        .dataFetcher("fileOverview", env -> {
                            Map<String, Object> args = env.getArguments();
                            return fileOverview(args);
                        })
                        .dataFetcher("fileIDsFromList", env -> {
                            Map<String, Object> args = env.getArguments();
                            return fileIDsFromList(args);
                        })
                        .dataFetcher("numberOfMCICount", env -> {
                            Map<String, Object> args = env.getArguments();
                            return getParticipantsCount();
                        })
                        .dataFetcher("findParticipantIdsInList", env -> {
                            Map<String, Object> args = env.getArguments();
                            return findParticipantIdsInList(args);
                        })
                )
                .build();
    }

    private List<Map<String, Object>> subjectCountBy(String category, Map<String, Object> params, String endpoint, String cardinalityAggName, String indexType) throws IOException {
        return subjectCountBy(category, params, endpoint, Map.of(), cardinalityAggName, indexType);
    }

    private List<Map<String, Object>> subjectCountBy(String category, Map<String, Object> params, String endpoint, Map<String, Object> additionalParams, String cardinalityAggName, String indexType) throws IOException {
        Map<String, Object> query = inventoryESService.buildFacetFilterQuery(params, RANGE_PARAMS, Set.of(PAGE_SIZE), REGULAR_PARAMS, "nested_filters", indexType);
        List<String> only_includes;
        List<String> valueSet = INCLUDE_PARAMS.contains(category) ? (List<String>)params.get(category) : List.of();
        if (valueSet.size() > 0 && !(valueSet.size() == 1 && valueSet.get(0).equals(""))){
            only_includes = valueSet;
        } else {
            only_includes = List.of();
        }
        return getGroupCount(category, query, endpoint, cardinalityAggName, only_includes);
    }

    private List<Map<String, Object>> subjectCountByRange(String category, Map<String, Object> params, String endpoint, String cardinalityAggName, String indexType) throws IOException {
        return subjectCountByRange(category, params, endpoint, Map.of(), cardinalityAggName, indexType);
    }

    private List<Map<String, Object>> subjectCountByRange(String category, Map<String, Object> params, String endpoint, Map<String, Object> additionalParams, String cardinalityAggName, String indexType) throws IOException {
        Map<String, Object> query = inventoryESService.buildFacetFilterQuery(params, RANGE_PARAMS, Set.of(PAGE_SIZE), REGULAR_PARAMS, "nested_filters", indexType);
        return getGroupCountByRange(category, query, endpoint, cardinalityAggName);
    }

    private List<Map<String, Object>> filterSubjectCountBy(String category, Map<String, Object> params, String endpoint, String cardinalityAggName, String indexType) throws IOException {
        return filterSubjectCountBy(category, params, endpoint, Map.of(), cardinalityAggName, indexType);
    }

    private List<Map<String, Object>> filterSubjectCountBy(String category, Map<String, Object> params, String endpoint, Map<String, Object> additionalParams, String cardinalityAggName, String indexType) throws IOException {
        Map<String, Object> query = inventoryESService.buildFacetFilterQuery(params, RANGE_PARAMS, Set.of(PAGE_SIZE, category), REGULAR_PARAMS, "nested_filters", indexType);
        return getGroupCount(category, query, endpoint, cardinalityAggName, List.of());
    }

    private JsonArray getNodeCount(String category, Map<String, Object> query, String endpoint) throws IOException {
        query = inventoryESService.addNodeCountAggregations(query, category);
        Request request = new Request("GET", endpoint);
        request.setJsonEntity(gson.toJson(query));
        JsonObject jsonObject = inventoryESService.send(request);
        Map<String, JsonArray> aggs = inventoryESService.collectNodeCountAggs(jsonObject, category);
        JsonArray buckets = aggs.get(category);

        return buckets;
    }

    private List<Map<String, Object>> getGroupCountByRange(String category, Map<String, Object> query, String endpoint, String cardinalityAggName) throws IOException {
        query = inventoryESService.addRangeCountAggregations(query, category, cardinalityAggName);
        Request request = new Request("GET", endpoint);
        // System.out.println(gson.toJson(query));
        request.setJsonEntity(gson.toJson(query));
        JsonObject jsonObject = inventoryESService.send(request);
        Map<String, JsonArray> aggs = inventoryESService.collectRangCountAggs(jsonObject, category);
        JsonArray buckets = aggs.get(category);

        return getGroupCountHelper(buckets, cardinalityAggName);
    }

    private List<Map<String, Object>> getGroupCount(String category, Map<String, Object> query, String endpoint, String cardinalityAggName, List<String> only_includes) throws IOException {
        if (RANGE_PARAMS.contains(category)) {
            query = inventoryESService.addRangeAggregations(query, category, only_includes);
            Request request = new Request("GET", endpoint);
            // System.out.println(gson.toJson(query));
            request.setJsonEntity(gson.toJson(query));
            JsonObject jsonObject = inventoryESService.send(request);
            Map<String, JsonObject> aggs = inventoryESService.collectRangAggs(jsonObject, category);
            JsonObject ranges = aggs.get(category);

            return getRangeGroupCountHelper(ranges);
        } else {
            String[] AGG_NAMES = new String[] {category};
            query = inventoryESService.addAggregations(query, AGG_NAMES, cardinalityAggName, only_includes);
            Request request = new Request("GET", endpoint);
            request.setJsonEntity(gson.toJson(query));
            // System.out.println(gson.toJson(query));
            JsonObject jsonObject = inventoryESService.send(request);
            Map<String, JsonArray> aggs = inventoryESService.collectTermAggs(jsonObject, AGG_NAMES);
            JsonArray buckets = aggs.get(category);

            return getGroupCountHelper(buckets, cardinalityAggName);
        }
        
    }

    private List<Map<String, Object>> getRangeGroupCountHelper(JsonObject ranges) throws IOException {
        List<Map<String, Object>> data = new ArrayList<>();
        if (ranges.get("count").getAsInt() == 0) {
            data.add(Map.of("lowerBound", 0,
                    "subjects", 0,
                    "upperBound", 0
            ));
        } else {
            data.add(Map.of("lowerBound", ranges.get("min").getAsInt(),
                    "subjects", ranges.get("count").getAsInt(),
                    "upperBound", ranges.get("max").getAsInt()
            ));
        }
        return data;
    }

    private List<Map<String, Object>> getBooleanGroupCountHelper(JsonObject filters) throws IOException {
        List<Map<String, Object>> data = new ArrayList<>();
        for (Map.Entry<String, JsonElement> group: filters.entrySet()) {
            int count = group.getValue().getAsJsonObject().get("parent").getAsJsonObject().get("doc_count").getAsInt();
            if (count > 0) {
                data.add(Map.of("group", group.getKey(),
                    "subjects", count
                ));
            }
        }
        return data;
    }

    private List<Map<String, Object>> getGroupCountHelper(JsonArray buckets, String cardinalityAggName) throws IOException {
        List<Map<String, Object>> data = new ArrayList<>();
        for (JsonElement group: buckets) {
            if(!group.getAsJsonObject().get("key").getAsString().equals("")){
                data.add(Map.of("group", group.getAsJsonObject().get("key").getAsString(),
                        "subjects", !(cardinalityAggName == null) ? group.getAsJsonObject().get("cardinality_count").getAsJsonObject().get("value").getAsInt() : group.getAsJsonObject().get("doc_count").getAsInt()
                ));
            }
        }
        return data;
    }

    private Map<String, String[]> idsLists() throws IOException {
        Map<String, String[][]> indexProperties = Map.of(
            PARTICIPANTS_END_POINT, new String[][]{
                    new String[]{"participantIds", "participant_id"}
            }
        );
        //Generic Query
        Map<String, Object> query = esService.buildListQuery();
        //Results Map
        Map<String, String[]> results = new HashMap<>();
        //Iterate through each index properties map and make a request to each endpoint then format the results as
        // String arrays
        String cacheKey = "participantIDs";
        Map<String, String[]> data = (Map<String, String[]>)caffeineCache.asMap().get(cacheKey);
        if (data != null) {
            logger.info("hit cache!");
            return data;
        } else {
            for (String endpoint: indexProperties.keySet()){
                Request request = new Request("GET", endpoint);
                String[][] properties = indexProperties.get(endpoint);
                List<String> fields = new ArrayList<>();
                for (String[] prop: properties) {
                    fields.add(prop[1]);
                }
                query.put("_source", fields);
                
                List<Map<String, Object>> result = esService.collectPage(request, query, properties, ESService.MAX_ES_SIZE,
                        0);
                Map<String, List<String>> indexResults = new HashMap<>();
                Arrays.asList(properties).forEach(x -> indexResults.put(x[0], new ArrayList<>()));
                for(Map<String, Object> resultElement: result){
                    for(String key: indexResults.keySet()){
                        List<String> tmp = indexResults.get(key);
                        String v = (String) resultElement.get(key);
                        if (!tmp.contains(v)) {
                            tmp.add(v);
                        }
                    }
                }
                for(String key: indexResults.keySet()){
                    results.put(key, indexResults.get(key).toArray(new String[indexResults.size()]));
                }
            }
            caffeineCache.put(cacheKey, results);
        }
        
        return results;
    }

    private Integer getParticipantsCount() throws IOException {

        Map<String, Object> params = new HashMap<>();
        List study_ids=new ArrayList();
        study_ids.add("phs002790");
        params.put("study_id",study_ids);


        Map<String, Object> query_participants = inventoryESService.buildFacetFilterQuery(params, RANGE_PARAMS, Set.of(), REGULAR_PARAMS, "nested_filters", "participants");

        Request participantsCountRequest = new Request("GET", PARTICIPANTS_END_POINT);

        participantsCountRequest.setJsonEntity(gson.toJson(query_participants));
        JsonObject participantsCountResult = inventoryESService.send(participantsCountRequest);
        int numberOfParticipants = participantsCountResult.getAsJsonObject("hits").getAsJsonObject("total").get("value").getAsInt();

        return numberOfParticipants;

    }

    private Map<String, Object> searchParticipants(Map<String, Object> params) throws IOException {
        String cacheKey = generateCacheKey(params);
        Map<String, Object> data = (Map<String, Object>)caffeineCache.asMap().get(cacheKey);
        if (data != null) {
            logger.info("hit cache!");
            return data;
        } else {
            // logger.info("cache miss... querying for data.");
            data = new HashMap<>();

            final String CARDINALITY_AGG_NAME = "cardinality_agg_name";
            final String AGG_NAME = "agg_name";
            final String AGG_ENDPOINT = "agg_endpoint";
            final String WIDGET_QUERY = "widgetQueryName";
            final String FILTER_COUNT_QUERY = "filterCountQueryName";
            // Query related values
            final List<Map<String, Object>> PARTICIPANT_TERM_AGGS = new ArrayList<>();
            PARTICIPANT_TERM_AGGS.add(Map.of(
                    CARDINALITY_AGG_NAME, "pid",
                    AGG_NAME, "study_acronym",
                    WIDGET_QUERY, "participantCountByStudy",
                    FILTER_COUNT_QUERY, "filterParticipantCountByAcronym",
                    AGG_ENDPOINT, STUDIES_FACET_END_POINT
            ));
            PARTICIPANT_TERM_AGGS.add(Map.of(
                    CARDINALITY_AGG_NAME, "pid",
                    AGG_NAME, "diagnosis",
                    WIDGET_QUERY, "participantCountByDiagnosis",
                    FILTER_COUNT_QUERY, "filterParticipantCountByDiagnosis",
                    AGG_ENDPOINT, DIAGNOSIS_END_POINT
            ));
            PARTICIPANT_TERM_AGGS.add(Map.of(
                    CARDINALITY_AGG_NAME, "pid",
                    AGG_NAME, "age_at_diagnosis",
                    WIDGET_QUERY, "participantCountByDiagnosisAge",
                    FILTER_COUNT_QUERY, "filterParticipantCountByDiagnosisAge",
                    AGG_ENDPOINT, DIAGNOSIS_END_POINT
            ));
            PARTICIPANT_TERM_AGGS.add(Map.of(
                    AGG_NAME, "sex_at_birth",
                    WIDGET_QUERY,"participantCountBySexAtBirth",
                    FILTER_COUNT_QUERY, "filterParticipantCountBySexAtBirth",
                    AGG_ENDPOINT, PARTICIPANTS_END_POINT
            ));
            PARTICIPANT_TERM_AGGS.add(Map.of(
                    AGG_NAME, "race",
                    WIDGET_QUERY, "participantCountByRace",
                    FILTER_COUNT_QUERY, "filterParticipantCountByRace",
                    AGG_ENDPOINT, PARTICIPANTS_END_POINT
            ));
            PARTICIPANT_TERM_AGGS.add(Map.of(
                    CARDINALITY_AGG_NAME, "pid",
                    AGG_NAME, "dbgap_accession",
                    FILTER_COUNT_QUERY, "filterParticipantCountByDBGAPAccession",
                    AGG_ENDPOINT, STUDIES_FACET_END_POINT
            ));

            PARTICIPANT_TERM_AGGS.add(Map.of(
                    CARDINALITY_AGG_NAME, "pid",
                    AGG_NAME, "diagnosis_anatomic_site",
                    FILTER_COUNT_QUERY, "filterParticipantCountByDiagnosisAnatomicSite",
                    AGG_ENDPOINT, DIAGNOSIS_END_POINT
            ));
            PARTICIPANT_TERM_AGGS.add(Map.of(
                    CARDINALITY_AGG_NAME, "pid",
                    AGG_NAME, "disease_phase",
                    FILTER_COUNT_QUERY, "filterParticipantCountByDiseasePhase",
                    ADDITIONAL_UPDATE, Map.of("Not Reported", 3500),
                    AGG_ENDPOINT, DIAGNOSIS_END_POINT
            ));
            PARTICIPANT_TERM_AGGS.add(Map.of(
                    CARDINALITY_AGG_NAME, "pid",
                    AGG_NAME, "diagnosis_classification_system",
                    FILTER_COUNT_QUERY, "filterParticipantCountByDiagnosisClassificationSystem",
                    ADDITIONAL_UPDATE, Map.of("ICD-O-3.2", 5000),
                    AGG_ENDPOINT, DIAGNOSIS_END_POINT
            ));
            PARTICIPANT_TERM_AGGS.add(Map.of(
                    CARDINALITY_AGG_NAME, "pid",
                    AGG_NAME, "diagnosis_basis",
                    FILTER_COUNT_QUERY, "filterParticipantCountByDiagnosisBasis",
                    ADDITIONAL_UPDATE, Map.of("Clinical", 3500),
                    AGG_ENDPOINT, DIAGNOSIS_END_POINT
            ));
            PARTICIPANT_TERM_AGGS.add(Map.of(
                    AGG_NAME, "last_known_survival_status",
                    FILTER_COUNT_QUERY, "filterParticipantCountBySurvivalStatus",
                    AGG_ENDPOINT, PARTICIPANTS_END_POINT
            ));
            PARTICIPANT_TERM_AGGS.add(Map.of(
                    CARDINALITY_AGG_NAME, "pid",
                    AGG_NAME, "sample_anatomic_site",
                    FILTER_COUNT_QUERY, "filterParticipantCountBySampleAnatomicSite",
                    AGG_ENDPOINT, SAMPLES_END_POINT
            ));
            PARTICIPANT_TERM_AGGS.add(Map.of(
                    CARDINALITY_AGG_NAME, "pid",
                    AGG_NAME, "tumor_grade_source",
                    FILTER_COUNT_QUERY, "filterParticipantCountByTumorGradeSource",
                    AGG_ENDPOINT, DIAGNOSIS_END_POINT
            ));
            PARTICIPANT_TERM_AGGS.add(Map.of(
                    CARDINALITY_AGG_NAME, "pid",
                    AGG_NAME, "tumor_stage_source",
                    FILTER_COUNT_QUERY, "filterParticipantCountByTumorStageSource",
                    AGG_ENDPOINT, DIAGNOSIS_END_POINT
            ));
            PARTICIPANT_TERM_AGGS.add(Map.of(
                    CARDINALITY_AGG_NAME, "pid",
                    AGG_NAME, "participant_age_at_collection",
                    FILTER_COUNT_QUERY, "filterParticipantCountBySampleAge",
                    AGG_ENDPOINT, SAMPLES_END_POINT
            ));
            PARTICIPANT_TERM_AGGS.add(Map.of(
                    CARDINALITY_AGG_NAME, "pid",
                    AGG_NAME, "sample_tumor_status",
                    FILTER_COUNT_QUERY, "filterParticipantCountByTumorStatus",
                    ADDITIONAL_UPDATE, Map.of("Normal", 4000, "Tumor", 4500),
                    AGG_ENDPOINT, SAMPLES_END_POINT
            ));
            PARTICIPANT_TERM_AGGS.add(Map.of(
                    CARDINALITY_AGG_NAME, "pid",
                    AGG_NAME, "tumor_classification",
                    FILTER_COUNT_QUERY, "filterParticipantCountByTumorClassification",
                    ADDITIONAL_UPDATE, Map.of("Not Applicable", 4000),
                    AGG_ENDPOINT, SAMPLES_END_POINT
            ));
            //assay_method mapped to file_category
            PARTICIPANT_TERM_AGGS.add(Map.of(
                    CARDINALITY_AGG_NAME, "pid",
                    AGG_NAME, "file_category",
                    WIDGET_QUERY, "participantCountByAssayMethod",
                    FILTER_COUNT_QUERY, "filterParticipantCountByAssayMethod",
                    ADDITIONAL_UPDATE, Map.of("Sequencing", 500),
                    AGG_ENDPOINT, FILES_END_POINT
            ));
            PARTICIPANT_TERM_AGGS.add(Map.of(
                    CARDINALITY_AGG_NAME, "pid",
                    AGG_NAME, "file_type",
                    FILTER_COUNT_QUERY, "filterParticipantCountByFileType",
                    ADDITIONAL_UPDATE, Map.of("bam", 3500, "crai", 3600, "cram", 4000, "html", 3000, "pdf", 3000, "txt", 3500, "vcf" , 3500),
                    AGG_ENDPOINT, FILES_END_POINT
            ));
            PARTICIPANT_TERM_AGGS.add(Map.of(
                    CARDINALITY_AGG_NAME, "pid",
                    AGG_NAME, "study_name",
                    FILTER_COUNT_QUERY, "filterParticipantCountByStudyTitle",
                    AGG_ENDPOINT, STUDIES_FACET_END_POINT
            ));
            PARTICIPANT_TERM_AGGS.add(Map.of(
                    CARDINALITY_AGG_NAME, "pid",
                    AGG_NAME, "library_selection",
                    FILTER_COUNT_QUERY, "filterParticipantCountByLibrarySelection",
                    ADDITIONAL_UPDATE, Map.of("Hybrid Selection", 4500),
                    AGG_ENDPOINT, FILES_END_POINT
            ));
            PARTICIPANT_TERM_AGGS.add(Map.of(
                    CARDINALITY_AGG_NAME, "pid",
                    AGG_NAME, "library_source_material",
                    FILTER_COUNT_QUERY, "filterParticipantCountByLibrarySourceMaterial",
                    ADDITIONAL_UPDATE, Map.of("Bulk Cells", 3000),
                    AGG_ENDPOINT, FILES_END_POINT
            ));
            PARTICIPANT_TERM_AGGS.add(Map.of(
                    CARDINALITY_AGG_NAME, "pid",
                    AGG_NAME, "library_source_molecule",
                    FILTER_COUNT_QUERY, "filterParticipantCountByLibrarySourceMolecule",
                    ADDITIONAL_UPDATE, Map.of("Genomic", 5000, "Transcriptomic", 3500),
                    AGG_ENDPOINT, FILES_END_POINT
            ));
            PARTICIPANT_TERM_AGGS.add(Map.of(
                    CARDINALITY_AGG_NAME, "pid",
                    AGG_NAME, "library_strategy",
                    FILTER_COUNT_QUERY, "filterParticipantCountByLibraryStrategy",
                    AGG_ENDPOINT, FILES_END_POINT
            ));
            
            Map<String, Object> query_participants = inventoryESService.buildFacetFilterQuery(params, RANGE_PARAMS, Set.of(), REGULAR_PARAMS, "nested_filters", "participants");
            // System.out.println(gson.toJson(query_participants));
            Map<String, Object> newQuery_participants = new HashMap<>(query_participants);
            newQuery_participants.put("size", 0);
            newQuery_participants.put("track_total_hits", 10000000);
            Map<String, Object> fields = new HashMap<String, Object>();
            fields.put("file_count", Map.of("sum", Map.of("field", "file_count")));
            newQuery_participants.put("aggs", fields);
            Request participantsCountRequest = new Request("GET", PARTICIPANTS_END_POINT);
            // System.out.println(gson.toJson(newQuery_participants));
            participantsCountRequest.setJsonEntity(gson.toJson(newQuery_participants));
            JsonObject participantsCountResult = inventoryESService.send(participantsCountRequest);
            int numberOfParticipants = participantsCountResult.getAsJsonObject("hits").getAsJsonObject("total").get("value").getAsInt();
            int participants_file_count = participantsCountResult.getAsJsonObject("aggregations").getAsJsonObject("file_count").get("value").getAsInt();

            Map<String, Object> query_diagnosis = inventoryESService.buildFacetFilterQuery(params, RANGE_PARAMS, Set.of(), REGULAR_PARAMS, "nested_filters", "diagnosis");
            Request diagnosisCountRequest = new Request("GET", DIAGNOSIS_COUNT_END_POINT);
            // System.out.println(gson.toJson(query_diagnosis));
            diagnosisCountRequest.setJsonEntity(gson.toJson(query_diagnosis));
            JsonObject diagnosisCountResult = inventoryESService.send(diagnosisCountRequest);
            int numberOfDiagnosis = diagnosisCountResult.get("count").getAsInt();

            Map<String, Object> query_samples = inventoryESService.buildFacetFilterQuery(params, RANGE_PARAMS, Set.of(), REGULAR_PARAMS, "nested_filters", "samples");
            Map<String, Object> newQuery_samples = new HashMap<>(query_samples);
            newQuery_samples.put("size", 0);
            newQuery_samples.put("track_total_hits", 10000000);
            Map<String, Object> fields_sample = new HashMap<String, Object>();
            fields_sample.put("file_count", Map.of("sum", Map.of("field", "direct_file_count")));
            newQuery_samples.put("aggs", fields_sample);
            Request samplesCountRequest = new Request("GET", SAMPLES_END_POINT);
            //System.out.println(gson.toJson(newQuery_samples));
            samplesCountRequest.setJsonEntity(gson.toJson(newQuery_samples));
            JsonObject samplesCountResult = inventoryESService.send(samplesCountRequest);
            int numberOfSamples = samplesCountResult.getAsJsonObject("hits").getAsJsonObject("total").get("value").getAsInt();
            int samples_file_count = samplesCountResult.getAsJsonObject("aggregations").getAsJsonObject("file_count").get("value").getAsInt();

            Map<String, Object> query_files_all_records = inventoryESService.buildFacetFilterQuery(params, RANGE_PARAMS, Set.of(), REGULAR_PARAMS, "nested_filters", "files_overall");
            int numberOfStudies = getNodeCount("study_id", query_files_all_records, FILES_END_POINT).size();

            Request filesCountRequest = new Request("GET", FILES_COUNT_END_POINT);
            Map<String, Object> query_files_valid_records = inventoryESService.buildFacetFilterQuery(params, RANGE_PARAMS, Set.of(), REGULAR_PARAMS, "nested_filters", "files");
            filesCountRequest.setJsonEntity(gson.toJson(query_files_valid_records));
            JsonObject filesCountResult = inventoryESService.send(filesCountRequest);
            int numberOfFiles = filesCountResult.get("count").getAsInt();

            data.put("numberOfStudies", numberOfStudies);
            data.put("numberOfDiagnosis", numberOfDiagnosis);
            data.put("numberOfParticipants", numberOfParticipants);
            data.put("numberOfSamples", numberOfSamples);
            data.put("numberOfFiles", numberOfFiles);
            data.put("participantsFileCount", participants_file_count);
            data.put("diagnosisFileCount", participants_file_count);
            data.put("samplesFileCount", samples_file_count);
            data.put("studiesFileCount", numberOfFiles);
            data.put("filesFileCount", numberOfFiles);

            // widgets data and facet filter counts for projects
            for (var agg: PARTICIPANT_TERM_AGGS) {
                String field = (String)agg.get(AGG_NAME);
                String widgetQueryName = (String)agg.get(WIDGET_QUERY);
                Map<String, Integer> additionalUpdate = (Map<String, Integer>)agg.get(ADDITIONAL_UPDATE);
                String filterCountQueryName = (String)agg.get(FILTER_COUNT_QUERY);
                String endpoint = (String)agg.get(AGG_ENDPOINT);
                String indexType = endpoint.replace("/", "").replace("_search", "");
                String cardinalityAggName = (String)agg.get(CARDINALITY_AGG_NAME);
                // System.out.println(cardinalityAggName);
                List<Map<String, Object>> filterCount = filterSubjectCountBy(field, params, endpoint, cardinalityAggName, indexType);
                if(RANGE_PARAMS.contains(field)) {
                    data.put(filterCountQueryName, filterCount.get(0));
                } else {
                    data.put(filterCountQueryName, filterCount);
                }

                if (widgetQueryName != null) {
                    if (RANGE_PARAMS.contains(field)) {
                        List<Map<String, Object>> subjectCount = subjectCountByRange(field, params, endpoint, cardinalityAggName, indexType);
                        data.put(widgetQueryName, subjectCount);
                    } else {
                        if (params.containsKey(field) && ((List<String>)params.get(field)).size() > 0) {
                            List<Map<String, Object>> subjectCount = subjectCountBy(field, params, endpoint, cardinalityAggName, indexType);
                            data.put(widgetQueryName, subjectCount);
                        } else {
                            data.put(widgetQueryName, filterCount);
                        }
                    }

                }

                if (additionalUpdate != null) {
                    List<Map<String, Object>> filterCount_2_update = (List<Map<String, Object>>)data.get(filterCountQueryName);
                    List<Map<String, Object>> widgetCount_2_update = (List<Map<String, Object>>)data.get(widgetQueryName);
                    List<String> facetValues_need_update = new ArrayList<String>();
                    //check if the count for each of the group within the filterCount is smaller than the marked number
                    for (Map<String, Object> map : filterCount_2_update) {
                        String group = (String)map.get("group");
                        if (additionalUpdate.containsKey(group)) {
                            int count = (Integer)map.get("subjects");
                            int marked = (Integer)additionalUpdate.get(group);
                            if (count > marked) {
                                //need to perform query
                                facetValues_need_update.add(group);
                            }
                        }
                    }
                    //if any facet value is above the number, perform the query
                    if (facetValues_need_update.size() > 0) {
                        Map<String, Object> query_4_update = inventoryESService.buildFacetFilterQuery(params, RANGE_PARAMS, Set.of(field), REGULAR_PARAMS, "nested_filters", "participants");
                        String prop = field.equals("file_category") ? "assay_method" : field;
                        query_4_update = inventoryESService.addCustomAggregations(query_4_update, "facetAgg", prop, "sample_diagnosis_file_filters");
                        Request request = new Request("GET", PARTICIPANTS_END_POINT);
                        request.setJsonEntity(gson.toJson(query_4_update));
                        JsonObject jsonObject = inventoryESService.send(request);
                        Map<String, Integer> updated_values = inventoryESService.collectCustomTerms(jsonObject, "facetAgg");
                        //update the facet value one more time
                        List<Map<String, Object>> filterCount_new = new ArrayList<Map<String, Object>>();
                        for (Map<String, Object> map : filterCount_2_update) {
                            String group = (String)map.get("group");
                            int count = (Integer)map.get("subjects");
                            // System.out.println(count);
                            if (facetValues_need_update.indexOf(group) >= 0) {
                                count = updated_values.get(group);
                                // System.out.println("-->"+ count);
                            }
                            filterCount_new.add(Map.of("group", group, "subjects", count));
                        }
                        data.put(filterCountQueryName, filterCount_new);
                        //update the widget facet value if widget exists
                        if (widgetCount_2_update != null) {
                            List<Map<String, Object>> widgetCount_new = new ArrayList<Map<String, Object>>();
                            for (Map<String, Object> map : widgetCount_2_update) {
                                String group = (String)map.get("group");
                                int count = (Integer)map.get("subjects");
                                if (facetValues_need_update.indexOf(group) >= 0) {
                                    count = updated_values.get(group);
                                }
                                widgetCount_new.add(Map.of("group", group, "subjects", count));
                            }
                            data.put(widgetQueryName, widgetCount_new);
                        }
                    }
                }
            }

            caffeineCache.put(cacheKey, data);

            return data;
        }

    }

    private List<Map<String, Object>> participantOverview(Map<String, Object> params) throws IOException {
        final String[][] PROPERTIES = new String[][]{
            new String[]{"id", "id"},
            new String[]{"participant_id", "participant_id"},
            new String[]{"dbgap_accession", "dbgap_accession"},
                new String[]{"study_id", "study_id"},
            new String[]{"race", "race_str"},
            new String[]{"sex_at_birth", "sex_at_birth"},
            new String[]{"synonym_id", "alternate_participant_id"},
            new String[]{"files", "files"}
        };

        String defaultSort = "participant_id"; // Default sort order

        Map<String, String> mapping = Map.ofEntries(
                Map.entry("participant_id", "participant_id"),
                Map.entry("dbgap_accession", "dbgap_accession"),
                Map.entry("study_id", "study_id"),
                Map.entry("race", "race_str"),
                Map.entry("sex_at_birth", "sex_at_birth"),

                Map.entry("synonym_id", "alternate_participant_id")
        );

        return overview(PARTICIPANTS_END_POINT, params, PROPERTIES, defaultSort, mapping, REGULAR_PARAMS, "nested_filters", "participants");
    }

    private List<Map<String, Object>> diagnosisOverview(Map<String, Object> params) throws IOException {
        final String[][] PROPERTIES = new String[][]{
            new String[]{"id", "id"},
            new String[]{"diagnosis_id", "diagnosis_id"},
            new String[]{"participant_id", "participant_id"},
            new String[]{"sample_id", "sample_id"},
            new String[]{"dbgap_accession", "dbgap_accession"},
                new String[]{"study_id", "study_id"},
            new String[]{"diagnosis", "diagnosis"},
            new String[]{"anatomic_site", "diagnosis_anatomic_site_str"},
            new String[]{"disease_phase", "disease_phase"},
                new String[]{"diagnosis_classification_system", "diagnosis_classification_system"},
                new String[]{"diagnosis_basis", "diagnosis_basis"},
            new String[]{"age_at_diagnosis", "age_at_diagnosis"},
            new String[]{"tumor_grade_source", "tumor_grade_source"},
            new String[]{"tumor_stage_source", "tumor_stage_source"},
            new String[]{"last_known_survival_status", "last_known_survival_status"},
            new String[]{"files", "files"}
        };

        String defaultSort = "diagnosis_id"; // Default sort order

        Map<String, String> mapping = Map.ofEntries(
                Map.entry("diagnosis_id", "diagnosis_id"),
                Map.entry("participant_id", "participant_id"),
                Map.entry("sample_id", "sample_id"),
                Map.entry("dbgap_accession", "dbgap_accession"),
                Map.entry("study_id", "study_id"),
                Map.entry("diagnosis", "diagnosis"),
                Map.entry("anatomic_site", "diagnosis_anatomic_site_str"),
                Map.entry("disease_phase", "disease_phase"),
                Map.entry("diagnosis_basis", "diagnosis_basis"),
                Map.entry("diagnosis_classification_system", "diagnosis_classification_system"),
                Map.entry("age_at_diagnosis", "age_at_diagnosis"),
                Map.entry("tumor_grade_source", "tumor_grade_source"),
                Map.entry("tumor_stage_source", "tumor_stage_source"),
                Map.entry("last_known_survival_status", "last_known_survival_status")
        );

        return overview(DIAGNOSIS_END_POINT, params, PROPERTIES, defaultSort, mapping, REGULAR_PARAMS, "nested_filters", "diagnosis");
    }

    private List<Map<String, Object>> studyOverview(Map<String, Object> params) throws IOException {
        final String[][] PROPERTIES = new String[][]{
            new String[]{"id", "id"},
            new String[]{"study_id", "study_id"},
            new String[]{"grant_id", "grant_id"},
            new String[]{"dbgap_accession", "dbgap_accession"},
            new String[]{"study_name", "study_name"},
            new String[]{"personnel_name", "PIs"},
            new String[]{"num_of_participants", "num_of_participants"},
            new String[]{"diagnosis", "diagnosis_cancer"},
            new String[]{"num_of_samples", "num_of_samples"},
            new String[]{"anatomic_site", "diagnosis_anatomic_site"},
            new String[]{"num_of_files", "num_of_files"},
            new String[]{"file_type", "file_types"},
            new String[]{"pubmed_id", "pubmed_ids"},
            new String[]{"files", "files"}
        };

        String defaultSort = "study_id"; // Default sort order

        Map<String, String> mapping = Map.ofEntries(
                Map.entry("study_id", "study_id"),
                Map.entry("pubmed_id", "pubmed_ids"),
                Map.entry("grant_id", "grant_id"),
                Map.entry("dbgap_accession", "dbgap_accession"),
                Map.entry("study_name", "study_name"),
                Map.entry("personnel_name", "PIs"),
                Map.entry("num_of_participants", "num_of_participants"),
                Map.entry("num_of_samples", "num_of_samples"),
                Map.entry("num_of_files", "num_of_files")
        );

        Request request = new Request("GET", FILES_END_POINT);
        Map<String, Object> query = inventoryESService.buildFacetFilterQuery(params, RANGE_PARAMS, Set.of(PAGE_SIZE, OFFSET, ORDER_BY, SORT_DIRECTION), REGULAR_PARAMS, "nested_filters", "files");
        String[] AGG_NAMES = new String[] {"study_id"};
        query = inventoryESService.addAggregations(query, AGG_NAMES);
        // System.out.println(gson.toJson(query));
        request.setJsonEntity(gson.toJson(query));
        JsonObject jsonObject = inventoryESService.send(request);
        Map<String, JsonArray> aggs = inventoryESService.collectTermAggs(jsonObject, AGG_NAMES);
        JsonArray buckets = aggs.get("study_id");
        List<String> data = new ArrayList<>();
        for (var bucket: buckets) {
            data.add(bucket.getAsJsonObject().get("key").getAsString());
        }

        String order_by = (String)params.get(ORDER_BY);
        String direction = ((String)params.get(SORT_DIRECTION));
        int pageSize = (int) params.get(PAGE_SIZE);
        int offset = (int) params.get(OFFSET);
        
        Map<String, Object> study_params = new HashMap<>();
        if (data.size() == 0) {
            data.add("-1");
        }
        study_params.put("study_id", data);
        study_params.put(ORDER_BY, order_by);
        study_params.put(SORT_DIRECTION, direction);
        study_params.put(PAGE_SIZE, pageSize);
        study_params.put(OFFSET, offset);
        
        return overview(STUDIES_END_POINT, study_params, PROPERTIES, defaultSort, mapping, REGULAR_PARAMS, "nested_filters", "studies");
    }

    private List<Map<String, Object>> sampleOverview(Map<String, Object> params) throws IOException {
        final String[][] PROPERTIES = new String[][]{
            new String[]{"id", "id"},
            new String[]{"sample_id", "sample_id"},
            new String[]{"participant_id", "participant_id"},
            new String[]{"study_id", "study_id"},
            new String[]{"anatomic_site", "sample_anatomic_site_str"},
            new String[]{"participant_age_at_collection", "participant_age_at_collection"},
            new String[]{"sample_tumor_status", "sample_tumor_status"},
            new String[]{"tumor_classification", "tumor_classification"},
            new String[]{"files", "files"}
        };

        String defaultSort = "sample_id"; // Default sort order

        Map<String, String> mapping = Map.ofEntries(
                Map.entry("sample_id", "sample_id"),
                Map.entry("participant_id", "participant_id"),
                Map.entry("study_id", "study_id"),
                Map.entry("anatomic_site", "sample_anatomic_site_str"),
                Map.entry("participant_age_at_collection", "participant_age_at_collection"),
                Map.entry("sample_tumor_status", "sample_tumor_status"),
                Map.entry("tumor_classification", "tumor_classification")
        );

        return overview(SAMPLES_END_POINT, params, PROPERTIES, defaultSort, mapping, REGULAR_PARAMS, "nested_filters", "samples");
    }

    private List<Map<String, Object>> fileOverview(Map<String, Object> params) throws IOException {
        final String[][] PROPERTIES = new String[][]{
            new String[]{"id", "id"},
            new String[]{"file_id", "file_id"},
            new String[]{"guid", "guid"},
            new String[]{"file_name", "file_name"},
            new String[]{"file_category", "file_category"},
            new String[]{"file_description", "file_description"},
            new String[]{"file_type", "file_type"},
            new String[]{"file_size", "file_size"},
                new String[]{"library_selection", "library_selection"},
                new String[]{"library_source_material", "library_source_material"},
                new String[]{"library_source_molecule", "library_source_molecule"},
                new String[]{"library_strategy", "library_strategy"},
            new String[]{"study_id", "study_id"},
            new String[]{"participant_id", "participant_id"},
            new String[]{"sample_id", "sample_id"},
            new String[]{"md5sum", "md5sum"},
            new String[]{"files", "files"}
        };

        String defaultSort = "file_id"; // Default sort order

        Map<String, String> mapping = Map.ofEntries(
                Map.entry("file_id", "file_id"),
                Map.entry("guid", "guid"),
                Map.entry("file_name", "file_name"),
                Map.entry("file_category", "file_category"),
                Map.entry("file_description", "file_description"),
                Map.entry("file_type", "file_type"),
                Map.entry("file_size", "file_size"),
                Map.entry("study_id", "study_id"),
                Map.entry("library_selection", "library_selection.sort"),
                Map.entry("library_source_material", "library_source_material.sort"),
                Map.entry("library_source_molecule", "library_source_molecule.sort"),
                Map.entry("library_strategy", "library_strategy.sort"),
                Map.entry("participant_id", "participant_id"),
                Map.entry("sample_id", "sample_id"),
                Map.entry("md5sum", "md5sum")
        );

        return overview(FILES_END_POINT, params, PROPERTIES, defaultSort, mapping, REGULAR_PARAMS, "nested_filters", "files");
    }

    // if the nestedProperty is set, this will filter based upon the params against the nested property for the endpoint's index.
    // otherwise, this will filter based upon the params against the top level properties for the index
    private List<Map<String, Object>> overview(String endpoint, Map<String, Object> params, String[][] properties, String defaultSort, Map<String, String> mapping, Set<String> regular_fields, String nestedProperty, String overviewType) throws IOException {

        Request request = new Request("GET", endpoint);
        Map<String, Object> query = inventoryESService.buildFacetFilterQuery(params, RANGE_PARAMS, Set.of(PAGE_SIZE, OFFSET, ORDER_BY, SORT_DIRECTION), regular_fields, nestedProperty, overviewType);
        String order_by = (String)params.get(ORDER_BY);
        String direction = ((String)params.get(SORT_DIRECTION)).toLowerCase();
        query.put("sort", mapSortOrder(order_by, direction, defaultSort, mapping));
        // "_source": {"exclude": [ "sample_diagnosis_file_filters"]}
        if (overviewType.equals("participants")) {
            query.put("_source", Map.of("exclude", Set.of("sample_diagnosis_file_filters")));
        }
        int pageSize = (int) params.get(PAGE_SIZE);
        int offset = (int) params.get(OFFSET);
        List<Map<String, Object>> page = inventoryESService.collectPage(request, query, properties, pageSize, offset);
        return page;
    }

    private List<Map<String, Object>> findParticipantIdsInList(Map<String, Object> params) throws IOException {
        final String[][] properties = new String[][]{
                new String[]{"participant_id", "participant_id"},
                new String[]{"study_id", "study_id"}
        };

        Map<String, Object> query = esService.buildListQuery(params, Set.of(), false);
        Request request = new Request("GET",PARTICIPANTS_END_POINT);

        return esService.collectPage(request, query, properties, ESService.MAX_ES_SIZE, 0);
    }

    private Map<String, String> mapSortOrder(String order_by, String direction, String defaultSort, Map<String, String> mapping) {
        String sortDirection = direction;
        if (!sortDirection.equalsIgnoreCase("asc") && !sortDirection.equalsIgnoreCase("desc")) {
            sortDirection = "asc";
        }

        String sortOrder = defaultSort; // Default sort order
        if (mapping.containsKey(order_by)) {
            sortOrder = mapping.get(order_by);
        } else {
            logger.info("Order: \"" + order_by + "\" not recognized, use default order");
        }
        return Map.of(sortOrder, sortDirection);
    }

    private List<String> fileIDsFromList(Map<String, Object> params) throws IOException {
        List<String> participantIDsSet = (List<String>) params.get("participant_ids");
        List<String> diagnosisIDsSet = (List<String>) params.get("diagnosis_ids");
        List<String> studyIDsSet = (List<String>) params.get("study_ids");
        List<String> sampleIDsSet = (List<String>) params.get("sample_ids");
        List<String> fileIDsSet = (List<String>) params.get("file_ids");
        
        if (participantIDsSet.size() > 0 && !(participantIDsSet.size() == 1 && participantIDsSet.get(0).equals(""))) {
            Map<String, Object> query = inventoryESService.buildGetFileIDsQuery(participantIDsSet);
            Request request = new Request("GET", PARTICIPANTS_END_POINT);
            // System.out.println(gson.toJson(query));
            request.setJsonEntity(gson.toJson(query));
            JsonObject jsonObject = inventoryESService.send(request);
            List<String> result = inventoryESService.collectFileIDs(jsonObject);
            return result;
        }

        if (diagnosisIDsSet.size() > 0 && !(diagnosisIDsSet.size() == 1 && diagnosisIDsSet.get(0).equals(""))) {
            Map<String, Object> query = inventoryESService.buildGetFileIDsQuery(diagnosisIDsSet);
            Request request = new Request("GET", DIAGNOSIS_END_POINT);
            // System.out.println(gson.toJson(query));
            request.setJsonEntity(gson.toJson(query));
            JsonObject jsonObject = inventoryESService.send(request);
            List<String> result = inventoryESService.collectFileIDs(jsonObject);
            return result;
        }

        if (studyIDsSet.size() > 0 && !(studyIDsSet.size() == 1 && studyIDsSet.get(0).equals(""))) {
            Map<String, Object> query = inventoryESService.buildGetFileIDsQuery(studyIDsSet);
            Request request = new Request("GET", STUDIES_END_POINT);
            // System.out.println(gson.toJson(query));
            request.setJsonEntity(gson.toJson(query));
            JsonObject jsonObject = inventoryESService.send(request);
            List<String> result = inventoryESService.collectFileIDs(jsonObject);
            return result;
        }

        if (sampleIDsSet.size() > 0 && !(sampleIDsSet.size() == 1 && sampleIDsSet.get(0).equals(""))) {
            Map<String, Object> query = inventoryESService.buildGetFileIDsQuery(sampleIDsSet);
            Request request = new Request("GET", SAMPLES_END_POINT);
            // System.out.println(gson.toJson(query));
            request.setJsonEntity(gson.toJson(query));
            JsonObject jsonObject = inventoryESService.send(request);
            List<String> result = inventoryESService.collectFileIDs(jsonObject);
            return result;
        }

        if (fileIDsSet.size() > 0 && !(fileIDsSet.size() == 1 && fileIDsSet.get(0).equals(""))) {
            //return with the same file ids
            
            return fileIDsSet;
            // Map<String, Object> query = inventoryESService.buildGetFileIDsQuery(fileIDsSet, "file_id");
            // Request request = new Request("GET", FILES_END_POINT);
            // System.out.println(gson.toJson(query));
            // request.setJsonEntity(gson.toJson(query));
            // JsonObject jsonObject = inventoryESService.send(request);
            // List<String> result = inventoryESService.collectFileIDs(jsonObject, "file_id");
            // return result;
        }

        return new ArrayList<>();
    }

    private String generateCacheKey(Map<String, Object> params) throws IOException {
        List<String> keys = new ArrayList<>();
        for (String key: params.keySet()) {
            if (RANGE_PARAMS.contains(key)) {
                // Range parameters, should contain two doubles, first lower bound, then upper bound
                // Any other values after those two will be ignored
                List<Integer> bounds = (List<Integer>) params.get(key);
                if (bounds.size() >= 2) {
                    Integer lower = bounds.get(0);
                    Integer higher = bounds.get(1);
                    if (lower == null && higher == null) {
                        throw new IOException("Lower bound and Upper bound can't be both null!");
                    }
                    keys.add(key.concat(lower.toString()).concat(higher.toString()));
                }
            } else {
                List<String> valueSet = (List<String>) params.get(key);
                // list with only one empty string [""] means return all records
                if (valueSet.size() > 0 && !(valueSet.size() == 1 && valueSet.get(0).equals(""))) {
                    keys.add(key.concat(valueSet.toString()));
                }
            }
        }
        if (keys.size() == 0){
            // System.out.println("all");
            return "all";
        } else {
            // System.out.println(keys.toString());
            return keys.toString();
        }
    }
}
