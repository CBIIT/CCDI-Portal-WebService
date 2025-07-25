package gov.nih.nci.bento_ri.service;

import com.google.gson.*;
import gov.nih.nci.bento.model.ConfigurationDAO;
import gov.nih.nci.bento.service.ESService;
import gov.nih.nci.bento.service.RedisService;
import gov.nih.nci.bento.service.connector.AWSClient;
import gov.nih.nci.bento.service.connector.AbstractClient;
import gov.nih.nci.bento.service.connector.DefaultClient;

import org.apache.http.util.EntityUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.client.*;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import javax.annotation.PreDestroy;
import java.io.IOException;
import java.util.*;

@Service("InventoryESService")
public class InventoryESService extends ESService {
    public static final String SCROLL_ENDPOINT = "/_search/scroll";
    public static final String JSON_OBJECT = "jsonObject";
    public static final String AGGS = "aggs";
    public static final int MAX_ES_SIZE = 10000;
    final Set<String> PARTICIPANT_PARAMS = Set.of("race", "sex_at_birth");
    final Set<String> SURVIVAL_PARAMS = Set.of("last_known_survival_status", "age_at_last_known_survival_status", "first_event");
    final Set<String> TREATMENT_PARAMS = Set.of("treatment_type", "treatment_agent", "age_at_treatment_start");
    final Set<String> TREATMENT_RESPONSE_PARAMS = Set.of("response_category", "age_at_response");
    final Set<String> DIAGNOSIS_PARAMS = Set.of( "diagnosis", "disease_phase", "diagnosis_classification_system", "diagnosis_basis", "tumor_grade_source", "tumor_stage_source", "diagnosis_anatomic_site", "age_at_diagnosis");
    final Set<String> SAMPLE_PARAMS = Set.of("sample_anatomic_site", "participant_age_at_collection", "sample_tumor_status", "tumor_classification");
    final Set<String> FILE_PARAMS = Set.of("data_category", "file_type", "library_selection", "library_source_material", "library_source_molecule", "library_strategy", "file_mapping_level");
    final Set<String> SAMPLE_FILE_PARAMS = Set.of("sample_anatomic_site", "participant_age_at_collection", "sample_tumor_status", "tumor_classification", "data_category", "file_type", "library_selection", "library_source_material", "library_source_molecule", "library_strategy", "file_mapping_level");
    

    static final AWSCredentialsProvider credentialsProvider = new DefaultAWSCredentialsProviderChain();

    private static final Logger logger = LogManager.getLogger(RedisService.class);

    @Autowired
    private ConfigurationDAO config;

    private RestClient client;

    private Gson gson = new GsonBuilder().serializeNulls().create();

    private InventoryESService(ConfigurationDAO config) {
        super(config);
        this.gson = new GsonBuilder().serializeNulls().create();
        logger.info("Initializing Elasticsearch client");
        // Base on host name to use signed request (AWS) or not (local)
        AbstractClient abstractClient = config.isEsSignRequests() ? new AWSClient(config) : new DefaultClient(config);
        client = abstractClient.getLowLevelElasticClient();
    }

    @PreDestroy
    private void close() throws IOException {
        client.close();
    }

    public JsonObject send(Request request) throws IOException{
        Response response = client.performRequest(request);
        int statusCode = response.getStatusLine().getStatusCode();
        if (statusCode != 200) {
            String msg = "Elasticsearch returned code: " + statusCode;
            logger.error(msg);
            throw new IOException(msg);
        }
        return getJSonFromResponse(response);
    }

    public JsonObject getJSonFromResponse(Response response) throws IOException {
        String responseBody = EntityUtils.toString(response.getEntity());
        JsonObject jsonObject = gson.fromJson(responseBody, JsonObject.class);
        return jsonObject;
    }

    // This function build queries with following rules:
    //  - If a list is empty, query will return empty dataset
    //  - If a list has only one element which is empty string, query will return all data available
    //  - If a list is null, query will return all data available
    public Map<String, Object> buildListQuery(Map<String, Object> params, Set<String> excludedParams) {
        return buildListQuery(params, excludedParams, false);
    }

    public Map<String, Object> buildListQuery(Map<String, Object> params, Set<String> excludedParams, boolean ignoreCase) {
        Map<String, Object> result = new HashMap<>();

        List<Object> filter = new ArrayList<>();
        for (var key: params.keySet()) {
            if (excludedParams.contains(key)) {
                continue;
            }
            Object obj = params.get(key);
            List<String> valueSet;
            if (obj instanceof List) {
                valueSet = (List<String>) obj;
            }
            else if (obj instanceof Integer) {
                String value = String.valueOf(obj);
                valueSet = List.of(value);
            } else {
                String value = (String)obj;
                valueSet = List.of(value);
            }

            if (ignoreCase) {
                List<String> lowerCaseValueSet = new ArrayList<>();
                for (String value: valueSet) {
                    lowerCaseValueSet.add(value.toLowerCase());
                }
                valueSet = lowerCaseValueSet;
            }
            // list with only one empty string [""] means return all records
            if (valueSet.size() == 1) {
                if (valueSet.get(0).equals("")) {
                    continue;
                }
            }
            filter.add(Map.of(
                "terms", Map.of( key, valueSet)
            ));
        }

        result.put("query", Map.of("bool", Map.of("filter", filter)));
        return result;
    }

    public Map<String, Object> buildFacetFilterQuery(Map<String, Object> params, Set<String> rangeParams, Set<String> excludedParams, Set<String> regular_fields, String nestedProperty, String indexType) throws IOException {
        Map<String, Object> result = new HashMap<>();

        if (indexType.startsWith("files")) {
            //regular files query
            List<Object> filter_1 = new ArrayList<>();
            //query for files directly linked to study
            List<Object> filter_2 = new ArrayList<>();
            List<Object> participant_filters = new ArrayList<>();
            List<Object> combined_participant_filters = new ArrayList<>();
            List<Object> sample_diagnosis_filters = new ArrayList<>();
            List<Object> combined_filters = new ArrayList<>();
            List<Object> combined_survival_filters = new ArrayList<>();
            List<Object> combined_treatment_filters = new ArrayList<>();
            List<Object> combined_treatment_response_filters = new ArrayList<>();
            List<Object> survival_filters = new ArrayList<>();
            List<Object> treatment_filters = new ArrayList<>();
            List<Object> treatment_response_filters = new ArrayList<>();
            
            for (String key: params.keySet()) { 
                String finalKey = key;
                if (key.equals("data_category")) {
                        finalKey = "data_category";
                }
                if (excludedParams.contains(finalKey)) {
                    continue;
                }

                if (rangeParams.contains(key)) {
                    // Range parameters, should contain two doubles, first lower bound, then upper bound
                    // Any other values after those two will be ignored
                    List<Integer> bounds = (List<Integer>) params.get(key);
                    if (bounds.size() >= 2) {
                        Integer lower = bounds.get(0);
                        Integer higher = bounds.get(1);
                        if (lower == null && higher == null) {
                            throw new IOException("Lower bound and Upper bound can't be both null!");
                        }
                        Map<String, Integer> range = new HashMap<>();
                        if (lower != null) {
                            range.put("gte", lower);
                        }
                        if (higher != null) {
                            range.put("lte", higher);
                        }
                        if (key.equals("age_at_diagnosis")) {
                            sample_diagnosis_filters.add(Map.of(
                                "range", Map.of("sample_diagnosis_filters."+key, range)
                            ));
                            combined_filters.add(Map.of(
                                "range", Map.of("combined_filters.sample_diagnosis_filters."+key, range)
                            ));
                        } else if (key.equals("participant_age_at_collection")) {
                            sample_diagnosis_filters.add(Map.of(
                                "range", Map.of("sample_diagnosis_filters."+key, range)
                            ));
                            combined_filters.add(Map.of(
                                "range", Map.of("combined_filters.sample_diagnosis_filters."+key, range)
                            ));
                        
                        } else if (key.equals("age_at_treatment_start")) {
                            treatment_filters.add(Map.of(
                                "range", Map.of("treatment_filters."+key, range)
                            ));
                            combined_filters.add(Map.of(
                                "range", Map.of("combined_filters.treatment_filters."+key, range)
                            ));
                        } else if (key.equals("age_at_response")) {
                            treatment_response_filters.add(Map.of(
                                "range", Map.of("treatment_response_filters."+key, range)
                            ));
                            combined_filters.add(Map.of(
                                "range", Map.of("combined_filters.treatment_response_filters."+key, range)
                            ));
                        } else if (key.equals("age_at_last_known_survival_status")) {
                            survival_filters.add(Map.of(
                                "range", Map.of("survival_filters."+key, range)
                            ));
                            combined_filters.add(Map.of(
                                "range", Map.of("combined_filters.survival_filters."+key, range)
                            ));
                        
                        } else {
                            filter_1.add(Map.of(
                                "range", Map.of(key, range)
                            ));
                            filter_2.add(Map.of(
                                "range", Map.of(key, range)
                            ));
                        }
                    }
                } else {
                    // Term parameters (default)
                    List<String> valueSet = (List<String>) params.get(key);
                    
                    if (key.equals("participant_ids")) {
                        key = "participant_id";
                    }
                    // list with only one empty string [""] means return all records
                    if (valueSet.size() > 0 && !(valueSet.size() == 1 && valueSet.get(0).equals(""))) {
                        if (PARTICIPANT_PARAMS.contains(key)) {
                            participant_filters.add(Map.of(
                                "terms", Map.of("participant_filters."+key, valueSet)
                            ));
                            combined_participant_filters.add(Map.of(
                                "terms", Map.of("combined_filters."+key, valueSet)
                            ));
                        } else if (SURVIVAL_PARAMS.contains(key)) {
                            survival_filters.add(Map.of(
                                "terms", Map.of("survival_filters."+key, valueSet)
                            ));
                            combined_survival_filters.add(Map.of(
                                "terms", Map.of("combined_filters.survival_filters."+key, valueSet)
                            ));
                        } else if (TREATMENT_PARAMS.contains(key)) {
                            treatment_filters.add(Map.of(
                                "terms", Map.of("treatment_filters."+key, valueSet)
                            ));
                            combined_treatment_filters.add(Map.of(
                                "terms", Map.of("combined_filters.treatment_filters."+key, valueSet)
                            ));
                        }  else if (TREATMENT_RESPONSE_PARAMS.contains(key)) {
                            treatment_response_filters.add(Map.of(
                                "terms", Map.of("treatment_response_filters."+key, valueSet)
                            ));
                            combined_treatment_response_filters.add(Map.of(
                                "terms", Map.of("combined_filters.treatment_response_filters."+key, valueSet)
                            ));
                        } else if (DIAGNOSIS_PARAMS.contains(key)) {
                            sample_diagnosis_filters.add(Map.of(
                                "terms", Map.of("sample_diagnosis_filters."+key, valueSet)
                            ));
                            combined_filters.add(Map.of(
                                "terms", Map.of("combined_filters.sample_diagnosis_filters."+key, valueSet)
                            ));
                        } else if (SAMPLE_PARAMS.contains(key)) {
                            sample_diagnosis_filters.add(Map.of(
                                "terms", Map.of("sample_diagnosis_filters."+key, valueSet)
                            ));
                            combined_filters.add(Map.of(
                                "terms", Map.of("combined_filters.sample_diagnosis_filters."+key, valueSet)
                            ));
                        } else {
                            filter_1.add(Map.of(
                                "terms", Map.of(key, valueSet)
                            ));
                            if (key.equals("participant_id")) {
                                combined_participant_filters.add(Map.of(
                                    "terms", Map.of("combined_filters."+key, valueSet)
                                ));
                            } else {
                                filter_2.add(Map.of(
                                    "terms", Map.of(key, valueSet)
                                ));
                            }
                        }
                    }
                }
            }

            int filterLen = filter_1.size();
            int participantFilterLen = participant_filters.size();
            int survivalFilterLen = survival_filters.size();
            int treatmentFilterLen = treatment_filters.size();
            int treatmentResponseFilterLen = treatment_response_filters.size();
            int sampleDiagnosisFilterLen = sample_diagnosis_filters.size();
            int combinedParticipantFilterLen = combined_participant_filters.size();
            int combinedFilterLen = combined_filters.size();
            int combinedSurvivalFilterLen = combined_survival_filters.size();
            int combinedTreatmentFilterLen = combined_treatment_filters.size();
            int combinedTreatmentResponseFilterLen = combined_treatment_response_filters.size();

            if (filterLen + participantFilterLen + survivalFilterLen + treatmentFilterLen + treatmentResponseFilterLen + combinedParticipantFilterLen + sampleDiagnosisFilterLen + combinedFilterLen + combinedSurvivalFilterLen + combinedTreatmentFilterLen + combinedTreatmentResponseFilterLen == 0) {
                if (indexType.equals("files_overall")) {
                    result.put("query", Map.of("match_all", Map.of()));
                } else {
                    result.put("query", Map.of("bool", Map.of("must", Map.of("exists", Map.of("field", "file_id")))));
                }
            } else {
                if (participantFilterLen > 0) {
                    filter_1.add(Map.of("nested", Map.of("path", "participant_filters", "query", Map.of("bool", Map.of("filter", participant_filters)))));
                }
                if (sampleDiagnosisFilterLen > 0) {
                    filter_1.add(Map.of("nested", Map.of("path", "sample_diagnosis_filters", "query", Map.of("bool", Map.of("filter", sample_diagnosis_filters)))));
                }
                if (survivalFilterLen > 0) {
                    filter_1.add(Map.of("nested", Map.of("path", "survival_filters", "query", Map.of("bool", Map.of("filter", survival_filters)))));
                }
                if (treatmentFilterLen > 0) {
                    filter_1.add(Map.of("nested", Map.of("path", "treatment_filters", "query", Map.of("bool", Map.of("filter", treatment_filters)))));
                }
                if (treatmentResponseFilterLen > 0) {
                    filter_1.add(Map.of("nested", Map.of("path", "treatment_response_filters", "query", Map.of("bool", Map.of("filter", treatment_response_filters)))));
                }
                if (combinedFilterLen > 0) {
                    combined_participant_filters.add(Map.of("nested", Map.of("path", "combined_filters.sample_diagnosis_filters", "query", Map.of("bool", Map.of("filter", combined_filters)))));
                }
                if (combinedSurvivalFilterLen > 0) {
                    combined_participant_filters.add(Map.of("nested", Map.of("path", "combined_filters.survival_filters", "query", Map.of("bool", Map.of("filter", combined_survival_filters)))));
                }
                if (combinedTreatmentFilterLen > 0) {
                    combined_participant_filters.add(Map.of("nested", Map.of("path", "combined_filters.treatment_filters", "query", Map.of("bool", Map.of("filter", combined_treatment_filters)))));
                }
                if (combinedTreatmentResponseFilterLen > 0) {
                    combined_participant_filters.add(Map.of("nested", Map.of("path", "combined_filters.treatment_response_filters", "query", Map.of("bool", Map.of("filter", combined_treatment_response_filters)))));
                }
                // System.out.println(filter_1);
                filter_2.add(Map.of("nested", Map.of("path", "combined_filters", "query", Map.of("bool", Map.of("filter", combined_participant_filters)))));
                List<Object> overall_filter = new ArrayList<>();
                List<Object> should_filter = new ArrayList<>();
                should_filter.add(Map.of("exists", Map.of("field", "pid")));
                should_filter.add(Map.of("exists", Map.of("field", "sample_id")));
                if (indexType.equals("files_overall")) {
                    overall_filter.add(Map.of("bool", Map.of("should", should_filter, "filter", filter_1)));
                } else {
                    overall_filter.add(Map.of("bool", Map.of("must", Map.of("exists", Map.of("field", "file_id")), "should", should_filter, "filter", filter_1))); 
                }
                List<Object> must_not_filter = new ArrayList<>();
                must_not_filter.add(Map.of("exists", Map.of("field", "pid")));
                must_not_filter.add(Map.of("exists", Map.of("field", "sample_id")));
                if (indexType.equals("files_overall")) {
                    overall_filter.add(Map.of("bool", Map.of("must_not", must_not_filter, "filter", filter_2)));
                } else {
                        overall_filter.add(Map.of("bool", Map.of("must", Map.of("exists", Map.of("field", "file_id")), "must_not", must_not_filter, "filter", filter_2)));
                }
                result.put("query", Map.of("bool", Map.of("should", overall_filter)));
            }
        } else {
            List<Object> filter = new ArrayList<>();
            List<Object> diagnosis_filters = new ArrayList<>();
            List<Object> survival_filters = new ArrayList<>();
            List<Object> treatment_filters = new ArrayList<>();
            List<Object> treatment_response_filters = new ArrayList<>();
            List<Object> file_filters = new ArrayList<>();
            List<Object> sample_file_filters = new ArrayList<>();
            List<Object> sample_diagnosis_file_filters = new ArrayList<>();
            
            for (String key: params.keySet()) {
                if (excludedParams.contains(key)) {
                    continue;
                }

                if (rangeParams.contains(key)) {
                    // Range parameters, should contain two doubles, first lower bound, then upper bound
                    // Any other values after those two will be ignored
                    List<Integer> bounds = (List<Integer>) params.get(key);
                    if (bounds.size() >= 2) {
                        Integer lower = bounds.get(0);
                        Integer higher = bounds.get(1);
                        if (lower == null && higher == null) {
                            throw new IOException("Lower bound and Upper bound can't be both null!");
                        }
                        Map<String, Integer> range = new HashMap<>();
                        if (lower != null) {
                            range.put("gte", lower);
                        }
                        if (higher != null) {
                            range.put("lte", higher);
                        }
                        if(indexType.endsWith("participants") && (key.equals("age_at_diagnosis") || key.equals("participant_age_at_collection"))){
                            sample_diagnosis_file_filters.add(Map.of(
                                "range", Map.of("sample_diagnosis_file_filters."+key, range)
                            ));
                        } else if (indexType.equals("samples") && key.equals("age_at_diagnosis")) {
                            diagnosis_filters.add(Map.of(
                                "range", Map.of("diagnosis_filters."+key, range)
                            ));
                        } else if (indexType.equals("diagnosis") && key.equals("participant_age_at_collection")) {
                            sample_file_filters.add(Map.of(
                                "range", Map.of("sample_file_filters."+key, range)
                            ));
                        } else if (!indexType.equals("treatments") && key.equals("age_at_treatment_start")) {
                            treatment_filters.add(Map.of(
                                "range", Map.of("treatment_filters."+key, range)
                            ));
                        } else if (!indexType.equals("treatment_responses") && key.equals("age_at_response")) {
                            treatment_response_filters.add(Map.of(
                                "range", Map.of("treatment_response_filters."+key, range)
                            ));
                        } else if (!indexType.equals("survivals") && key.equals("age_at_last_known_survival_status")) {
                            survival_filters.add(Map.of(
                                "range", Map.of("survival_filters."+key, range)
                            ));
                        } else {
                            filter.add(Map.of(
                                "range", Map.of(key, range)
                            ));
                        }
                    }
                } else {
                    // Term parameters (default)
                    List<String> valueSet = (List<String>) params.get(key);
                    
                    if (key.equals("participant_ids")) {
                        key = "participant_id";
                    }
                    // list with only one empty string [""] means return all records
                    if (valueSet.size() > 0 && !(valueSet.size() == 1 && valueSet.get(0).equals(""))) {
                        if ((DIAGNOSIS_PARAMS.contains(key) || SAMPLE_FILE_PARAMS.contains(key) || FILE_PARAMS.contains(key)) && (!indexType.equals("diagnosis") && !indexType.equals("samples"))) {
                            sample_diagnosis_file_filters.add(Map.of(
                                "terms", Map.of("sample_diagnosis_file_filters."+key, valueSet)
                            ));
                        } else if (SURVIVAL_PARAMS.contains(key) && !indexType.equals("survivals"))  {
                            survival_filters.add(Map.of(
                                "terms", Map.of("survival_filters."+key, valueSet)
                            ));
                        } else if (TREATMENT_PARAMS.contains(key) && !indexType.equals("treatments"))  {
                            treatment_filters.add(Map.of(
                                "terms", Map.of("treatment_filters."+key, valueSet)
                            ));
                        } else if (TREATMENT_RESPONSE_PARAMS.contains(key) && !indexType.equals("treatment_responses"))  {
                            treatment_response_filters.add(Map.of(
                                "terms", Map.of("treatment_response_filters."+key, valueSet)
                            ));
                        } else if (DIAGNOSIS_PARAMS.contains(key) && indexType.equals("samples")) {
                            diagnosis_filters.add(Map.of(
                                "terms", Map.of("diagnosis_filters."+key, valueSet)
                            ));
                        } else if (SAMPLE_FILE_PARAMS.contains(key) && !indexType.equals("samples")) {
                            sample_file_filters.add(Map.of(
                                "terms", Map.of("sample_file_filters."+key, valueSet)
                            ));
                        } else if (FILE_PARAMS.contains(key) && indexType.equals("samples")) {
                            file_filters.add(Map.of(
                                "terms", Map.of("file_filters."+key, valueSet)
                            ));
                        } else {
                            filter.add(Map.of(
                                "terms", Map.of(key, valueSet)
                            ));
                        }
                    }
                }
            }

            int FilterLen = filter.size();
            int survivalFilterLen = survival_filters.size();
            int treatmentFilterLen = treatment_filters.size();
            int treatmentResponseFilterLen = treatment_response_filters.size();
            int diagnosisFilterLen = diagnosis_filters.size();
            int sampleFileFilterLen = sample_file_filters.size();
            int fileFilterLen = file_filters.size();
            int sampleDiagnosisFileFilterLen = sample_diagnosis_file_filters.size();

            if (FilterLen + survivalFilterLen + treatmentFilterLen + treatmentResponseFilterLen+ diagnosisFilterLen + sampleFileFilterLen + fileFilterLen + sampleDiagnosisFileFilterLen == 0) {
                result.put("query", Map.of("match_all", Map.of()));
            } else {
                if (diagnosisFilterLen > 0) {
                    filter.add(Map.of("nested", Map.of("path", "diagnosis_filters", "query", Map.of("bool", Map.of("filter", diagnosis_filters)))));
                }
                if (survivalFilterLen > 0) {
                    filter.add(Map.of("nested", Map.of("path", "survival_filters", "query", Map.of("bool", Map.of("filter", survival_filters)))));
                }
                if (treatmentFilterLen > 0) {
                    filter.add(Map.of("nested", Map.of("path", "treatment_filters", "query", Map.of("bool", Map.of("filter", treatment_filters)))));
                }
                if (treatmentResponseFilterLen > 0) {
                    filter.add(Map.of("nested", Map.of("path", "treatment_response_filters", "query", Map.of("bool", Map.of("filter", treatment_response_filters)))));
                }
                if (sampleFileFilterLen > 0) {
                    filter.add(Map.of("nested", Map.of("path", "sample_file_filters", "query", Map.of("bool", Map.of("filter", sample_file_filters)))));
                }
                if (fileFilterLen > 0) {
                    filter.add(Map.of("nested", Map.of("path", "file_filters", "query", Map.of("bool", Map.of("filter", file_filters)))));
                }
                if (sampleDiagnosisFileFilterLen > 0) {
                    filter.add(Map.of("nested", Map.of("path", "sample_diagnosis_file_filters", "query", Map.of("bool", Map.of("filter", sample_diagnosis_file_filters)))));
                }
                result.put("query", Map.of("bool", Map.of("filter", filter)));
            }
        }
  
        return result;
    }

    public Map<String, Object> buildGetFileIDsQuery(List<String> ids) throws IOException {
        Map<String, Object> result = new HashMap<>();
        result.put("_source", Set.of("id", "files"));
        result.put("query", Map.of("terms", Map.of("id", ids)));
        result.put("size", ids.size());
        result.put("from", 0);
        return result;
    }

    public Map<String, Object> addAggregations(Map<String, Object> query, String[] termAggNames, String cardinalityAggName, List<String> only_includes) {
        return addAggregations(query, termAggNames, cardinalityAggName, new String[]{}, only_includes);
    }

    public Map<String, Object> addNodeCountAggregations(Map<String, Object> query, String nodeName) {
        Map<String, Object> newQuery = new HashMap<>(query);
        newQuery.put("size", 0);

        // "aggs" : {
        //     "langs" : {
        //         "terms" : { "field" : "language",  "size" : 10000 }
        //     }
        // }

        Map<String, Object> fields = new HashMap<String, Object>();
        fields.put(nodeName, Map.of("terms", Map.of("field", nodeName, "size", 10000)));
        newQuery.put("aggs", fields);
        
        return newQuery;
    }

    public Map<String, Object> addRangeCountAggregations(Map<String, Object> query, String rangeAggName, String cardinalityAggName) {
        Map<String, Object> newQuery = new HashMap<>(query);
        newQuery.put("size", 0);

            //   "aggs": {
            //     "age_at_diagnosis": {
            //        "range": {
            //           "field": "age_at_diagnosis",
            //           "ranges": [
            //              {
            //                 "from": 0,
            //                 "to": 1000
            //              },
            //              {
            //                 "from": 1000,
            //                 "to": 10000
            //              },
            //              {
            //                 "from": 10000,
            //                 "to": 25000
            //              },
            //              {
            //                 "from": 25000
            //              }
            //           ]
            //        },
            //        "aggs": {
            //           "unique_count": {
            //            "cardinality": {
            //                "field": "participant_id",
            //                "precision_threshold": 40000
            //            }
            //          }
            //        }
            //      }
            //    }

        Map<String, Object> fields = new HashMap<String, Object>();
        Map<String, Object> subField = new HashMap<String, Object>();
        Map<String, Object> subField_ranges = new HashMap<String, Object>();
        subField_ranges.put("field", rangeAggName);
        subField_ranges.put("ranges", Set.of(Map.of("key", "0 to 4", "from", 0, "to", 4 * 365), Map.of("key", "5 to 9", "from", 4 * 365, "to", 9 * 365), Map.of("key", "10 to 14", "from", 9 * 365, "to", 14 * 365), Map.of("key", "15 to 19", "from", 14 * 365, "to", 19 * 365), Map.of("key", "20 to 29", "from", 19 * 365, "to", 29 * 365), Map.of("key", ">29", "from", 29 * 365)));
        
        subField.put("range", subField_ranges);
        if (! (cardinalityAggName == null)) {
            subField.put("aggs", Map.of("cardinality_count", Map.of("cardinality", Map.of("field", cardinalityAggName, "precision_threshold", 40000))));
        }
        fields.put(rangeAggName, subField);
        newQuery.put("aggs", fields);
        
        return newQuery;
    }

    public Map<String, Object> addRangeAggregations(Map<String, Object> query, String rangeAggName, List<String> only_includes) {
        Map<String, Object> newQuery = new HashMap<>(query);
        newQuery.put("size", 0);

        //       "aggs": {
        //         "inner": {
        //           "filter":{  
        //             "range":{  
        //              "age_at_diagnosis":{  
        //               "gt":0
        //              }
        //             }
        //            },
        //            "aggs": {
        //              "age_stats": { 
        //               "stats": { 
        //                 "field": "age_at_diagnosis"
        //               }
        //             }
        //           }

        Map<String, Object> fields = new HashMap<String, Object>();
        Map<String, Object> subField = new HashMap<String, Object>();
        subField.put("filter", Map.of("range", Map.of(rangeAggName, Map.of("gt", -1))));
        subField.put("aggs", Map.of("range_stats", Map.of("stats", Map.of("field", rangeAggName))));
        fields.put("inner", subField);
        newQuery.put("aggs", fields);
        
        return newQuery;
    }

    public Map<String, Object> addAggregations(Map<String, Object> query, String[] termAggNames, String subCardinalityAggName, String[] rangeAggNames, List<String> only_includes) {
        Map<String, Object> newQuery = new HashMap<>(query);
        newQuery.put("size", 0);
        Map<String, Object> fields = new HashMap<String, Object>();
        for (String field: termAggNames) {
            Map<String, Object> subField = new HashMap<String, Object>();
            subField.put("field", field);
            subField.put("size", 10000);
            if (only_includes.size() > 0) {
                subField.put("include", only_includes);
            }
            if (! (subCardinalityAggName == null)) {
                fields.put(field, Map.of("terms", subField, "aggs", addCardinalityHelper(subCardinalityAggName)));
            } else {
                fields.put(field, Map.of("terms", subField));
            }
        }
        newQuery.put("aggs", fields);
        return newQuery;
    }

    public Map<String, Object> addCustomAggregations(Map<String, Object> query, String aggName, String field, String nestedProperty) {
        // "aggs": {
        //     "customAgg": {
            //     "nested": {
            //         "path": "sample_diagnosis_file_filters"
            //     },
            //     "aggs": {
            //         "min_price": {
            //         "terms": {
            //             "field": "sample_diagnosis_file_filters.diagnosis_classification_system"
            //         },
            //         "aggs": {
            //             "top_reverse_nested": {
            //             "reverse_nested": {}
            //             }
            //         }
            //         }
            //     }
        //     }
        // }
        Map<String, Object> newQuery = new HashMap<>(query);
        newQuery.put("size", 0);
        Map<String, Object> aggSection = new HashMap<String, Object>();
        Map<String, Object> aggSubSection = new HashMap<String, Object>();
        aggSubSection.put("agg_buckets", Map.of("terms", Map.of("field", nestedProperty + "." + field, "size", 1000), "aggs", Map.of("top_reverse_nested", Map.of("reverse_nested", Map.of()))));
        aggSection.put(aggName, Map.of("nested", Map.of("path", nestedProperty), "aggs", aggSubSection));
        newQuery.put("aggs", aggSection);
        return newQuery;
    }

    public Map<String, Object> addCardinalityHelper(String cardinalityAggName) {
        return Map.of("cardinality_count", Map.of("cardinality", Map.of("field", cardinalityAggName, "precision_threshold", 40000)));
    }

    public Map<String, JsonArray> collectNodeCountAggs(JsonObject jsonObject, String nodeName) {
        Map<String, JsonArray> data = new HashMap<>();
        JsonObject aggs = jsonObject.getAsJsonObject("aggregations");
        data.put(nodeName, aggs.getAsJsonObject(nodeName).getAsJsonArray("buckets"));
        
        return data;
    }

    public Map<String, JsonArray> collectRangCountAggs(JsonObject jsonObject, String rangeAggName) {
        Map<String, JsonArray> data = new HashMap<>();
        JsonObject aggs = jsonObject.getAsJsonObject("aggregations");
        data.put(rangeAggName, aggs.getAsJsonObject(rangeAggName).getAsJsonArray("buckets"));
        
        return data;
    }

    public Map<String, JsonObject> collectRangAggs(JsonObject jsonObject, String rangeAggName) {
        Map<String, JsonObject> data = new HashMap<>();
        JsonObject aggs = jsonObject.getAsJsonObject("aggregations");
        data.put(rangeAggName, aggs.getAsJsonObject("inner").getAsJsonObject("range_stats"));
        
        return data;
    }

    public List<String> collectFileIDs(JsonObject jsonObject) {
        List<String> data = new ArrayList<>();
        JsonArray searchHits = jsonObject.getAsJsonObject("hits").getAsJsonArray("hits");
        for (var hit: searchHits) {
            JsonObject obj = hit.getAsJsonObject().get("_source").getAsJsonObject();
            JsonArray arr = obj.get("files").getAsJsonArray();
            for (int i = 0; i < arr.size(); i++) {
                data.add(arr.get(i).getAsString());
            }
        }
        return data;
    }

    public List<String> collectTerms(JsonObject jsonObject, String aggName) {
        List<String> data = new ArrayList<>();
        JsonObject aggs = jsonObject.getAsJsonObject("aggregations");
        JsonArray buckets = aggs.getAsJsonObject(aggName).getAsJsonArray("buckets");
        for (var bucket: buckets) {
            data.add(bucket.getAsJsonObject().get("key").getAsString());
        }
        return data;
    }

    public Map<String, Integer> collectCustomTerms(JsonObject jsonObject, String aggName) {
        Map<String, Integer> data = new HashMap<>();
        JsonObject aggs = jsonObject.getAsJsonObject("aggregations").getAsJsonObject(aggName);
        JsonArray buckets = aggs.getAsJsonObject("agg_buckets").getAsJsonArray("buckets");
        for (var bucket: buckets) {
            data.put(bucket.getAsJsonObject().get("key").getAsString(), bucket.getAsJsonObject().getAsJsonObject("top_reverse_nested").get("doc_count").getAsInt());
        }
        return data;
    }

    public Map<String, JsonObject> collectRangeAggs(JsonObject jsonObject, String[] rangeAggNames) {
        Map<String, JsonObject> data = new HashMap<>();
        JsonObject aggs = jsonObject.getAsJsonObject("aggregations");
        for (String aggName: rangeAggNames) {
            // Range/stats
            data.put(aggName, aggs.getAsJsonObject(aggName));
        }
        return data;
    }

    // Collect a page of data, result will be of pageSize or less if not enough data remains
    public List<Map<String, Object>> collectPage(JsonObject jsonObject, String[][] properties, int pageSize) throws IOException {
        return collectPage(jsonObject, properties, pageSize, 0);
    }

    private List<Map<String, Object>> collectPage(JsonObject jsonObject, String[][] properties, int pageSize, int offset) throws IOException {
        return collectPage(jsonObject, properties, null, pageSize, offset);
    }

    public List<Map<String, Object>> collectPage(JsonObject jsonObject, String[][] properties, String[][] highlights, int pageSize, int offset) throws IOException {
        List<Map<String, Object>> data = new ArrayList<>();
        JsonArray searchHits = jsonObject.getAsJsonObject("hits").getAsJsonArray("hits");
        for (int i = 0; i < searchHits.size(); i++) {
            // skip offset number of documents
            if (i + 1 <= offset) {
                continue;
            }
            Map<String, Object> row = new HashMap<>();
            for (String[] prop: properties) {
                String propName = prop[0];
                String dataField = prop[1];
                JsonElement element = searchHits.get(i).getAsJsonObject().get("_source").getAsJsonObject().get(dataField);
                row.put(propName, getValue(element));
            }
            data.add(row);
            if (data.size() >= pageSize) {
                break;
            }
        }
        return data;
    }

    // Convert JsonElement into Java collections and primitives
    private Object getValue(JsonElement element) {
        Object value = null;
        if (element == null || element.isJsonNull()) {
            return null;
        } else if (element.isJsonObject()) {
            value = new HashMap<String, Object>();
            JsonObject object = element.getAsJsonObject();
            for (String key: object.keySet()) {
                ((Map<String, Object>) value).put(key, getValue(object.get(key)));
            }
        } else if (element.isJsonArray()) {
            value = new ArrayList<>();
            for (JsonElement entry: element.getAsJsonArray()) {
                ((List<Object>)value).add(getValue(entry));
            }
        } else {
            value = element.getAsString();
        }
        return value;
    }

}