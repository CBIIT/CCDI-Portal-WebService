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
    final Set<String> PARTICIPANT_PARAMS = Set.of("race", "sex_at_birth", "participant_id");
    final Set<String> SURVIVAL_PARAMS = Set.of("last_known_survival_status", "age_at_last_known_survival_status", "first_event");
    final Set<String> TREATMENT_PARAMS = Set.of("treatment_type", "treatment_agent", "age_at_treatment_start");
    final Set<String> TREATMENT_RESPONSE_PARAMS = Set.of("response_category", "age_at_response");
    final Set<String> DIAGNOSIS_PARAMS = Set.of( "diagnosis", "disease_phase", "diagnosis_classification_system", "diagnosis_basis", "tumor_grade_source", "tumor_stage_source", "diagnosis_anatomic_site", "age_at_diagnosis", "diagnosis_category");
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

    /**
     * Queries the /_count Opensearch endpoint and returns the number of hits
     * @param query Opensearch query
     * @param index Name of the index to query
     * @return
     * @throws IOException
     */
    public int getCount(Map<String, Object> query, String index) throws IOException {
        Request request = new Request("GET", String.format("/%s/_count", index));
        String queryJson = gson.toJson(query);
        JsonObject recountResult;
        int newCount;

        request.setJsonEntity(queryJson);
        recountResult = send(request);
        newCount = recountResult.get("count").getAsInt();

        return newCount;
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
        
        // Add unknownAges parameters to excluded parameters to prevent them from being processed as regular filters
        Set<String> localExcludedParams = new HashSet<>(excludedParams);
        for (String key : params.keySet()) {
            if (key.endsWith("_unknownAges")) {
                localExcludedParams.add(key);
            }
        }

        if (indexType.startsWith("files")) {
            //regular files query
            List<Object> filter_1 = new ArrayList<>();
            List<Object> combined_filters = new ArrayList<>();
            List<Object> combined_participant_filters = new ArrayList<>();
            List<Object> combined_sample_diagnosis_filters = new ArrayList<>();
            List<Object> combined_survival_filters = new ArrayList<>();
            List<Object> combined_treatment_filters = new ArrayList<>();
            List<Object> combined_treatment_response_filters = new ArrayList<>();
            
            for (String key: params.keySet()) { 
                String finalKey = key;
                if (key.equals("data_category")) {
                        finalKey = "data_category";
                }
                if (localExcludedParams.contains(finalKey)) {
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
                            // Check if unknownAges parameter exists to determine if we should include unknown values
                            String unknownAgesKey = key + "_unknownAges";
                            boolean includeUnknown = true; // Default to including unknown values
                            if (params.containsKey(unknownAgesKey)) {
                                List<String> unknownAgesValues = (List<String>) params.get(unknownAgesKey);
                                // Only consider unknownAges parameter if it has a meaningful value
                                if (unknownAgesValues != null && !unknownAgesValues.isEmpty() && !unknownAgesValues.get(0).equals("")) {
                                    includeUnknown = false; // Use normal range filtering when unknownAges parameter is specified
                                }
                            }
                            
                            if (includeUnknown) {
                                // Include unknown values (-999) by default when no unknownAges parameter is specified
                                combined_sample_diagnosis_filters.add(Map.of(
                                    "bool", Map.of("should", List.of(
                                        Map.of("range", Map.of("combined_filters.sample_diagnosis_filters."+key, range)),
                                        Map.of("term", Map.of("combined_filters.sample_diagnosis_filters."+key, -999))
                                    ))
                                ));
                            } else {
                                // Use normal range filter when unknownAges parameter is specified
                                combined_sample_diagnosis_filters.add(Map.of(
                                    "range", Map.of("combined_filters.sample_diagnosis_filters."+key, range)
                                ));
                            }
                        } else if (key.equals("participant_age_at_collection")) {
                            // Check if unknownAges parameter exists to determine if we should include unknown values
                            String unknownAgesKey = key + "_unknownAges";
                            boolean includeUnknown = true; // Default to including unknown values
                            if (params.containsKey(unknownAgesKey)) {
                                List<String> unknownAgesValues = (List<String>) params.get(unknownAgesKey);
                                // Only consider unknownAges parameter if it has a meaningful value
                                if (unknownAgesValues != null && !unknownAgesValues.isEmpty() && !unknownAgesValues.get(0).equals("")) {
                                    includeUnknown = false; // Use normal range filtering when unknownAges parameter is specified
                                }
                            }
                            
                            if (includeUnknown) {
                                // Include unknown values (-999) by default when no unknownAges parameter is specified
                                combined_sample_diagnosis_filters.add(Map.of(
                                    "bool", Map.of("should", List.of(
                                        Map.of("range", Map.of("combined_filters.sample_diagnosis_filters."+key, range)),
                                        Map.of("term", Map.of("combined_filters.sample_diagnosis_filters."+key, -999))
                                    ))
                                ));
                            } else {
                                // Use normal range filter when unknownAges parameter is specified
                                combined_sample_diagnosis_filters.add(Map.of(
                                    "range", Map.of("combined_filters.sample_diagnosis_filters."+key, range)
                                ));
                            }
                        
                        } else if (key.equals("age_at_treatment_start")) {
                            // Check if unknownAges parameter exists to determine if we should include unknown values
                            String unknownAgesKey = key + "_unknownAges";
                            boolean includeUnknown = true; // Default to including unknown values
                            if (params.containsKey(unknownAgesKey)) {
                                List<String> unknownAgesValues = (List<String>) params.get(unknownAgesKey);
                                // Only consider unknownAges parameter if it has a meaningful value
                                if (unknownAgesValues != null && !unknownAgesValues.isEmpty() && !unknownAgesValues.get(0).equals("")) {
                                    includeUnknown = false; // Use normal range filtering when unknownAges parameter is specified
                                }
                            }
                            
                            if (includeUnknown) {
                                // Include unknown values (-999) by default when no unknownAges parameter is specified
                                combined_treatment_filters.add(Map.of(
                                    "bool", Map.of("should", List.of(
                                        Map.of("range", Map.of("combined_filters.treatment_filters."+key, range)),
                                        Map.of("term", Map.of("combined_filters.treatment_filters."+key, -999))
                                    ))
                                ));
                            } else {
                                // Use normal range filter when unknownAges parameter is specified
                                combined_treatment_filters.add(Map.of(
                                    "range", Map.of("combined_filters.treatment_filters."+key, range)
                                ));
                            }
                        } else if (key.equals("age_at_response")) {
                            // Check if unknownAges parameter exists to determine if we should include unknown values
                            String unknownAgesKey = key + "_unknownAges";
                            boolean includeUnknown = true; // Default to including unknown values
                            if (params.containsKey(unknownAgesKey)) {
                                List<String> unknownAgesValues = (List<String>) params.get(unknownAgesKey);
                                // Only consider unknownAges parameter if it has a meaningful value
                                if (unknownAgesValues != null && !unknownAgesValues.isEmpty() && !unknownAgesValues.get(0).equals("")) {
                                    includeUnknown = false; // Use normal range filtering when unknownAges parameter is specified
                                }
                            }
                            
                            if (includeUnknown) {
                                // Include unknown values (-999) by default when no unknownAges parameter is specified
                                combined_treatment_response_filters.add(Map.of(
                                    "bool", Map.of("should", List.of(
                                        Map.of("range", Map.of("combined_filters.treatment_response_filters."+key, range)),
                                        Map.of("term", Map.of("combined_filters.treatment_response_filters."+key, -999))
                                    ))
                                ));
                            } else {
                                // Use normal range filter when unknownAges parameter is specified
                                combined_treatment_response_filters.add(Map.of(
                                    "range", Map.of("combined_filters.treatment_response_filters."+key, range)
                                ));
                            }
                        } else if (key.equals("age_at_last_known_survival_status")) {
                            // Check if unknownAges parameter exists to determine if we should include unknown values
                            String unknownAgesKey = key + "_unknownAges";
                            boolean includeUnknown = true; // Default to including unknown values
                            if (params.containsKey(unknownAgesKey)) {
                                List<String> unknownAgesValues = (List<String>) params.get(unknownAgesKey);
                                // Only consider unknownAges parameter if it has a meaningful value
                                if (unknownAgesValues != null && !unknownAgesValues.isEmpty() && !unknownAgesValues.get(0).equals("")) {
                                    includeUnknown = false; // Use normal range filtering when unknownAges parameter is specified
                                }
                            }
                            
                            if (includeUnknown) {
                                // Include unknown values (-999) by default when no unknownAges parameter is specified
                                combined_survival_filters.add(Map.of(
                                    "bool", Map.of("should", List.of(
                                        Map.of("range", Map.of("combined_filters.survival_filters."+key, range)),
                                        Map.of("term", Map.of("combined_filters.survival_filters."+key, -999))
                                    ))
                                ));
                            } else {
                                // Use normal range filter when unknownAges parameter is specified
                                combined_survival_filters.add(Map.of(
                                    "range", Map.of("combined_filters.survival_filters."+key, range)
                                ));
                            }
                        
                        } else {
                            filter_1.add(Map.of(
                                "range", Map.of(key, range)
                            ));
                        }
                    }
                    
                    // Handle unknownAges parameter for age fields
                    String unknownAgesKey = key + "_unknownAges";
                    if (params.containsKey(unknownAgesKey) && (key.equals("age_at_diagnosis") || key.equals("participant_age_at_collection") || key.equals("age_at_treatment_start") || key.equals("age_at_response") || key.equals("age_at_last_known_survival_status"))) {
                        List<String> unknownAgesValues = (List<String>) params.get(unknownAgesKey);
                        if (unknownAgesValues != null && !unknownAgesValues.isEmpty() && !unknownAgesValues.get(0).equals("")) {
                            String unknownAgesValue = unknownAgesValues.get(0);
                            if ("exclude".equals(unknownAgesValue)) {
                                // Exclude records with unknown values (-999) and null values by checking field exists
                                if (key.equals("age_at_diagnosis")) {
                                    combined_sample_diagnosis_filters.add(Map.of(
                                        "bool", Map.of(
                                            "must", List.of(Map.of("exists", Map.of("field", "combined_filters.sample_diagnosis_filters."+key))),
                                            "must_not", List.of(Map.of("term", Map.of("combined_filters.sample_diagnosis_filters."+key, -999)))
                                        )
                                    ));
                                } else if (key.equals("participant_age_at_collection")) {
                                    combined_sample_diagnosis_filters.add(Map.of(
                                        "bool", Map.of(
                                            "must", List.of(Map.of("exists", Map.of("field", "combined_filters.sample_diagnosis_filters."+key))),
                                            "must_not", List.of(Map.of("term", Map.of("combined_filters.sample_diagnosis_filters."+key, -999)))
                                        )
                                    ));
                                } else if (key.equals("age_at_treatment_start")) {
                                    combined_treatment_filters.add(Map.of(
                                        "bool", Map.of(
                                            "must", List.of(Map.of("exists", Map.of("field", "combined_filters.treatment_filters."+key))),
                                            "must_not", List.of(Map.of("term", Map.of("combined_filters.treatment_filters."+key, -999)))
                                        )
                                    ));
                                } else if (key.equals("age_at_response")) {
                                    combined_treatment_response_filters.add(Map.of(
                                        "bool", Map.of(
                                            "must", List.of(Map.of("exists", Map.of("field", "combined_filters.treatment_response_filters."+key))),
                                            "must_not", List.of(Map.of("term", Map.of("combined_filters.treatment_response_filters."+key, -999)))
                                        )
                                    ));
                                } else if (key.equals("age_at_last_known_survival_status")) {
                                    combined_survival_filters.add(Map.of(
                                        "bool", Map.of(
                                            "must", List.of(Map.of("exists", Map.of("field", "combined_filters.survival_filters."+key))),
                                            "must_not", List.of(Map.of("term", Map.of("combined_filters.survival_filters."+key, -999)))
                                        )
                                    ));
                                }
                            } else if ("only".equals(unknownAgesValue)) {
                                // Only include records with unknown values (-999)
                                if (key.equals("age_at_diagnosis")) {
                                    combined_sample_diagnosis_filters.add(Map.of(
                                        "terms", Map.of("combined_filters.sample_diagnosis_filters."+key, List.of(-999))
                                    ));
                                } else if (key.equals("participant_age_at_collection")) {
                                    combined_sample_diagnosis_filters.add(Map.of(
                                        "terms", Map.of("combined_filters.sample_diagnosis_filters."+key, List.of(-999))
                                    ));
                                } else if (key.equals("age_at_treatment_start")) {
                                    combined_treatment_filters.add(Map.of(
                                        "terms", Map.of("combined_filters.treatment_filters."+key, List.of(-999))
                                    ));
                                } else if (key.equals("age_at_response")) {
                                    combined_treatment_response_filters.add(Map.of(
                                        "terms", Map.of("combined_filters.treatment_response_filters."+key, List.of(-999))
                                    ));
                                } else if (key.equals("age_at_last_known_survival_status")) {
                                    combined_survival_filters.add(Map.of(
                                        "terms", Map.of("combined_filters.survival_filters."+key, List.of(-999))
                                    ));
                                }
                            }
                            // "include" is default behavior, no changes needed
                        }
                    }
                } else {
                    // Term parameters (default)
                    List<String> valueSet = (List<String>) params.get(key);

                    if (key.equals("import_data")) {
                        if (valueSet.size() > 0 && !(valueSet.size() == 1 && valueSet.get(0).equals(""))) {
                            List<Object> shouldClauses = new ArrayList<>();
                            for (String obj : valueSet) {
                                try {
                                    JsonObject json = JsonParser.parseString(obj).getAsJsonObject();
                                    String study = json.get("study_id").getAsString();
                                    JsonArray participants = json.getAsJsonArray("participant_id");
                                    List<String> participantList = new ArrayList<>();
                                    for (JsonElement p : participants) {
                                        participantList.add(p.getAsString());
                                    }
                                    shouldClauses.add(
                                        Map.of(
                                            "bool", Map.of(
                                                "filter", List.of(
                                                    Map.of("term", Map.of("study_id", study)),
                                                    Map.of("terms", Map.of("participant_id", participantList))
                                                )
                                            )
                                        )
                                    );
                                } catch (Exception e) {
                                    // Handle parse error if needed
                                }
                            }
                            filter_1.add(
                                Map.of(
                                    "bool", Map.of(
                                        "should", shouldClauses
                                    )
                                )
                            );
                        }
                        continue;
                    }
                    
                    if (key.equals("participant_ids")) {
                        key = "participant_id";
                    }
                    // list with only one empty string [""] means return all records
                    if (valueSet.size() > 0 && !(valueSet.size() == 1 && valueSet.get(0).equals(""))) {
                        if (PARTICIPANT_PARAMS.contains(key)) {
                            combined_participant_filters.add(Map.of(
                                "terms", Map.of("combined_filters."+key, valueSet)
                            ));
                        } else if (SURVIVAL_PARAMS.contains(key)) {
                            combined_survival_filters.add(Map.of(
                                "terms", Map.of("combined_filters.survival_filters."+key, valueSet)
                            ));
                        } else if (TREATMENT_PARAMS.contains(key)) {
                            combined_treatment_filters.add(Map.of(
                                "terms", Map.of("combined_filters.treatment_filters."+key, valueSet)
                            ));
                        }  else if (TREATMENT_RESPONSE_PARAMS.contains(key)) {
                            combined_treatment_response_filters.add(Map.of(
                                "terms", Map.of("combined_filters.treatment_response_filters."+key, valueSet)
                            ));
                        } else if (DIAGNOSIS_PARAMS.contains(key)) {
                            combined_sample_diagnosis_filters.add(Map.of(
                                "terms", Map.of("combined_filters.sample_diagnosis_filters."+key, valueSet)
                            ));
                        } else if (SAMPLE_PARAMS.contains(key)) {
                            combined_sample_diagnosis_filters.add(Map.of(
                                "terms", Map.of("combined_filters.sample_diagnosis_filters."+key, valueSet)
                            ));
                        } else {
                            filter_1.add(Map.of(
                                "terms", Map.of(key, valueSet)
                            ));
                        }
                    }
                }
            }

            int filterLen = filter_1.size();
            int combinedParticipantFilterLen = combined_participant_filters.size();
            int combinedSampleDiagnosisFilterLen = combined_sample_diagnosis_filters.size();
            int combinedSurvivalFilterLen = combined_survival_filters.size();
            int combinedTreatmentFilterLen = combined_treatment_filters.size();
            int combinedTreatmentResponseFilterLen = combined_treatment_response_filters.size();

            if (filterLen + combinedParticipantFilterLen + combinedSampleDiagnosisFilterLen + combinedSurvivalFilterLen + combinedTreatmentFilterLen + combinedTreatmentResponseFilterLen == 0) {
                if (indexType.equals("files_overall")) {
                    result.put("query", Map.of("match_all", Map.of()));
                } else {
                    result.put("query", Map.of("bool", Map.of("must", Map.of("exists", Map.of("field", "file_id")))));
                }
            } else {
                if (combinedParticipantFilterLen > 0) {
                    //add each element of combined_participant_filters to combined_filters
                    for (Object filter : combined_participant_filters) {
                        combined_filters.add(filter);
                    }
                }
                if (combinedSampleDiagnosisFilterLen > 0) {
                    combined_filters.add(Map.of("nested", Map.of("path", "combined_filters.sample_diagnosis_filters", "query", Map.of("bool", Map.of("filter", combined_sample_diagnosis_filters)))));
                }
                if (combinedSurvivalFilterLen > 0) {
                    combined_filters.add(Map.of("nested", Map.of("path", "combined_filters.survival_filters", "query", Map.of("bool", Map.of("filter", combined_survival_filters)))));
                }
                if (combinedTreatmentFilterLen > 0) {
                    combined_filters.add(Map.of("nested", Map.of("path", "combined_filters.treatment_filters", "query", Map.of("bool", Map.of("filter", combined_treatment_filters)))));
                }
                if (combinedTreatmentResponseFilterLen > 0) {
                    combined_filters.add(Map.of("nested", Map.of("path", "combined_filters.treatment_response_filters", "query", Map.of("bool", Map.of("filter", combined_treatment_response_filters)))));
                }
                // System.out.println(filter_1);
                filter_1.add(Map.of("nested", Map.of("path", "combined_filters", "query", Map.of("bool", Map.of("filter", combined_filters)))));
                List<Object> overall_filter = new ArrayList<>();
                if (indexType.equals("files_overall")) {
                    overall_filter.add(Map.of("bool", Map.of("filter", filter_1)));
                } else {
                    overall_filter.add(Map.of("bool", Map.of("must", Map.of("exists", Map.of("field", "file_id")), "filter", filter_1))); 
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
                if (localExcludedParams.contains(key)) {
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
                            // Check if unknownAges parameter exists to determine if we should include unknown values
                            String unknownAgesKey = key + "_unknownAges";
                            boolean includeUnknown = true; // Default to including unknown values
                            if (params.containsKey(unknownAgesKey)) {
                                List<String> unknownAgesValues = (List<String>) params.get(unknownAgesKey);
                                // Only consider unknownAges parameter if it has a meaningful value
                                if (unknownAgesValues != null && !unknownAgesValues.isEmpty() && !unknownAgesValues.get(0).equals("")) {
                                    includeUnknown = false; // Use normal range filtering when unknownAges parameter is specified
                                }
                            }
                            
                            if (includeUnknown) {
                                // Include unknown values (-999) by default when no unknownAges parameter is specified
                                sample_diagnosis_file_filters.add(Map.of(
                                    "bool", Map.of("should", List.of(
                                        Map.of("range", Map.of("sample_diagnosis_file_filters."+key, range)),
                                        Map.of("term", Map.of("sample_diagnosis_file_filters."+key, -999))
                                    ))
                                ));
                            } else {
                                // Use normal range filter when unknownAges parameter is specified
                                sample_diagnosis_file_filters.add(Map.of(
                                    "range", Map.of("sample_diagnosis_file_filters."+key, range)
                                ));
                            }
                        } else if (indexType.equals("samples") && key.equals("age_at_diagnosis")) {
                            // Check if unknownAges parameter exists to determine if we should include unknown values
                            String unknownAgesKey = key + "_unknownAges";
                            boolean includeUnknown = true; // Default to including unknown values
                            if (params.containsKey(unknownAgesKey)) {
                                List<String> unknownAgesValues = (List<String>) params.get(unknownAgesKey);
                                // Only consider unknownAges parameter if it has a meaningful value
                                if (unknownAgesValues != null && !unknownAgesValues.isEmpty() && !unknownAgesValues.get(0).equals("")) {
                                    includeUnknown = false; // Use normal range filtering when unknownAges parameter is specified
                                }
                            }
                            
                            if (includeUnknown) {
                                // Include unknown values (-999) by default when no unknownAges parameter is specified
                                diagnosis_filters.add(Map.of(
                                    "bool", Map.of("should", List.of(
                                        Map.of("range", Map.of("diagnosis_filters."+key, range)),
                                        Map.of("term", Map.of("diagnosis_filters."+key, -999))
                                    ))
                                ));
                            } else {
                                // Use normal range filter when unknownAges parameter is specified
                                diagnosis_filters.add(Map.of(
                                    "range", Map.of("diagnosis_filters."+key, range)
                                ));
                            }
                        } else if (indexType.equals("diagnosis") && key.equals("participant_age_at_collection")) {
                            // Check if unknownAges parameter exists to determine if we should include unknown values
                            String unknownAgesKey = key + "_unknownAges";
                            boolean includeUnknown = true; // Default to including unknown values
                            if (params.containsKey(unknownAgesKey)) {
                                List<String> unknownAgesValues = (List<String>) params.get(unknownAgesKey);
                                // Only consider unknownAges parameter if it has a meaningful value
                                if (unknownAgesValues != null && !unknownAgesValues.isEmpty() && !unknownAgesValues.get(0).equals("")) {
                                    includeUnknown = false; // Use normal range filtering when unknownAges parameter is specified
                                }
                            }
                            
                            if (includeUnknown) {
                                // Include unknown values (-999) by default when no unknownAges parameter is specified
                                sample_file_filters.add(Map.of(
                                    "bool", Map.of("should", List.of(
                                        Map.of("range", Map.of("sample_file_filters."+key, range)),
                                        Map.of("term", Map.of("sample_file_filters."+key, -999))
                                    ))
                                ));
                            } else {
                                // Use normal range filter when unknownAges parameter is specified
                                sample_file_filters.add(Map.of(
                                    "range", Map.of("sample_file_filters."+key, range)
                                ));
                            }
                        } else if (!indexType.equals("treatments") && key.equals("age_at_treatment_start")) {
                            // Check if unknownAges parameter exists to determine if we should include unknown values
                            String unknownAgesKey = key + "_unknownAges";
                            boolean includeUnknown = true; // Default to including unknown values
                            if (params.containsKey(unknownAgesKey)) {
                                List<String> unknownAgesValues = (List<String>) params.get(unknownAgesKey);
                                // Only consider unknownAges parameter if it has a meaningful value
                                if (unknownAgesValues != null && !unknownAgesValues.isEmpty() && !unknownAgesValues.get(0).equals("")) {
                                    includeUnknown = false; // Use normal range filtering when unknownAges parameter is specified
                                }
                            }
                            
                            if (includeUnknown) {
                                // Include unknown values (-999) by default when no unknownAges parameter is specified
                                treatment_filters.add(Map.of(
                                    "bool", Map.of("should", List.of(
                                        Map.of("range", Map.of("treatment_filters."+key, range)),
                                        Map.of("term", Map.of("treatment_filters."+key, -999))
                                    ))
                                ));
                            } else {
                                // Use normal range filter when unknownAges parameter is specified
                                treatment_filters.add(Map.of(
                                    "range", Map.of("treatment_filters."+key, range)
                                ));
                            }
                        } else if (!indexType.equals("treatment_responses") && key.equals("age_at_response")) {
                            // Check if unknownAges parameter exists to determine if we should include unknown values
                            String unknownAgesKey = key + "_unknownAges";
                            boolean includeUnknown = true; // Default to including unknown values
                            if (params.containsKey(unknownAgesKey)) {
                                List<String> unknownAgesValues = (List<String>) params.get(unknownAgesKey);
                                // Only consider unknownAges parameter if it has a meaningful value
                                if (unknownAgesValues != null && !unknownAgesValues.isEmpty() && !unknownAgesValues.get(0).equals("")) {
                                    includeUnknown = false; // Use normal range filtering when unknownAges parameter is specified
                                }
                            }
                            
                            if (includeUnknown) {
                                // Include unknown values (-999) by default when no unknownAges parameter is specified
                                treatment_response_filters.add(Map.of(
                                    "bool", Map.of("should", List.of(
                                        Map.of("range", Map.of("treatment_response_filters."+key, range)),
                                        Map.of("term", Map.of("treatment_response_filters."+key, -999))
                                    ))
                                ));
                            } else {
                                // Use normal range filter when unknownAges parameter is specified
                                treatment_response_filters.add(Map.of(
                                    "range", Map.of("treatment_response_filters."+key, range)
                                ));
                            }
                        } else if (!indexType.equals("survivals") && key.equals("age_at_last_known_survival_status")) {
                            // Check if unknownAges parameter exists to determine if we should include unknown values
                            String unknownAgesKey = key + "_unknownAges";
                            boolean includeUnknown = true; // Default to including unknown values
                            if (params.containsKey(unknownAgesKey)) {
                                List<String> unknownAgesValues = (List<String>) params.get(unknownAgesKey);
                                // Only consider unknownAges parameter if it has a meaningful value
                                if (unknownAgesValues != null && !unknownAgesValues.isEmpty() && !unknownAgesValues.get(0).equals("")) {
                                    includeUnknown = false; // Use normal range filtering when unknownAges parameter is specified
                                }
                            }
                            
                            if (includeUnknown) {
                                // Include unknown values (-999) by default when no unknownAges parameter is specified
                                survival_filters.add(Map.of(
                                    "bool", Map.of("should", List.of(
                                        Map.of("range", Map.of("survival_filters."+key, range)),
                                        Map.of("term", Map.of("survival_filters."+key, -999))
                                    ))
                                ));
                            } else {
                                // Use normal range filter when unknownAges parameter is specified
                                survival_filters.add(Map.of(
                                    "range", Map.of("survival_filters."+key, range)
                                ));
                            }
                        } else {
                            filter.add(Map.of(
                                "range", Map.of(key, range)
                            ));
                        }
                    }
                    
                    // Handle unknownAges parameter for age fields
                    String unknownAgesKey = key + "_unknownAges";
                    if (params.containsKey(unknownAgesKey) && (key.equals("age_at_diagnosis") || key.equals("participant_age_at_collection") || key.equals("age_at_treatment_start") || key.equals("age_at_response") || key.equals("age_at_last_known_survival_status"))) {
                        List<String> unknownAgesValues = (List<String>) params.get(unknownAgesKey);
                        if (unknownAgesValues != null && !unknownAgesValues.isEmpty() && !unknownAgesValues.get(0).equals("")) {
                            String unknownAgesValue = unknownAgesValues.get(0);
                            
                            if ("exclude".equals(unknownAgesValue)) {
                                // Exclude records with unknown values (-999) and null values by checking field exists
                                if(indexType.endsWith("participants") && (key.equals("age_at_diagnosis") || key.equals("participant_age_at_collection"))){
                                    sample_diagnosis_file_filters.add(Map.of(
                                        "bool", Map.of(
                                            "must", List.of(Map.of("exists", Map.of("field", "sample_diagnosis_file_filters."+key))),
                                            "must_not", List.of(Map.of("term", Map.of("sample_diagnosis_file_filters."+key, -999)))
                                        )
                                    ));
                                } else if (indexType.equals("samples") && key.equals("age_at_diagnosis")) {
                                    diagnosis_filters.add(Map.of(
                                        "bool", Map.of(
                                            "must", List.of(Map.of("exists", Map.of("field", "diagnosis_filters."+key))),
                                            "must_not", List.of(Map.of("term", Map.of("diagnosis_filters."+key, -999)))
                                        )
                                    ));
                                } else if (indexType.equals("diagnosis") && key.equals("participant_age_at_collection")) {
                                    sample_file_filters.add(Map.of(
                                        "bool", Map.of(
                                            "must", List.of(Map.of("exists", Map.of("field", "sample_file_filters."+key))),
                                            "must_not", List.of(Map.of("term", Map.of("sample_file_filters."+key, -999)))
                                        )
                                    ));
                                } else if (!indexType.equals("treatments") && key.equals("age_at_treatment_start")) {
                                    treatment_filters.add(Map.of(
                                        "bool", Map.of(
                                            "must", List.of(Map.of("exists", Map.of("field", "treatment_filters."+key))),
                                            "must_not", List.of(Map.of("term", Map.of("treatment_filters."+key, -999)))
                                        )
                                    ));
                                } else if (!indexType.equals("treatment_responses") && key.equals("age_at_response")) {
                                    treatment_response_filters.add(Map.of(
                                        "bool", Map.of(
                                            "must", List.of(Map.of("exists", Map.of("field", "treatment_response_filters."+key))),
                                            "must_not", List.of(Map.of("term", Map.of("treatment_response_filters."+key, -999)))
                                        )
                                    ));
                                } else if (!indexType.equals("survivals") && key.equals("age_at_last_known_survival_status")) {
                                    survival_filters.add(Map.of(
                                        "bool", Map.of(
                                            "must", List.of(Map.of("exists", Map.of("field", "survival_filters."+key))),
                                            "must_not", List.of(Map.of("term", Map.of("survival_filters."+key, -999)))
                                        )
                                    ));
                                } else {
                                    filter.add(Map.of(
                                        "bool", Map.of(
                                            "must", List.of(Map.of("exists", Map.of("field", key))),
                                            "must_not", List.of(Map.of("term", Map.of(key, -999)))
                                        )
                                    ));
                                }
                            } else if ("only".equals(unknownAgesValue)) {
                                // Only include records with unknown values (-999)
                                if(indexType.endsWith("participants") && (key.equals("age_at_diagnosis") || key.equals("participant_age_at_collection"))){
                                    sample_diagnosis_file_filters.add(Map.of(
                                        "terms", Map.of("sample_diagnosis_file_filters."+key, List.of(-999))
                                    ));
                                } else if (indexType.equals("samples") && key.equals("age_at_diagnosis")) {
                                    diagnosis_filters.add(Map.of(
                                        "terms", Map.of("diagnosis_filters."+key, List.of(-999))
                                    ));
                                } else if (indexType.equals("diagnosis") && key.equals("participant_age_at_collection")) {
                                    sample_file_filters.add(Map.of(
                                        "terms", Map.of("sample_file_filters."+key, List.of(-999))
                                    ));
                                } else if (!indexType.equals("treatments") && key.equals("age_at_treatment_start")) {
                                    treatment_filters.add(Map.of(
                                        "terms", Map.of("treatment_filters."+key, List.of(-999))
                                    ));
                                } else if (!indexType.equals("treatment_responses") && key.equals("age_at_response")) {
                                    treatment_response_filters.add(Map.of(
                                        "terms", Map.of("treatment_response_filters."+key, List.of(-999))
                                    ));
                                } else if (!indexType.equals("survivals") && key.equals("age_at_last_known_survival_status")) {
                                    survival_filters.add(Map.of(
                                        "terms", Map.of("survival_filters."+key, List.of(-999))
                                    ));
                                } else {
                                    filter.add(Map.of(
                                        "terms", Map.of(key, List.of(-999))
                                    ));
                                }
                            }
                            // "include" is default behavior, no changes needed
                        }
                    }
                } else {
                    // Term parameters (default)
                    List<String> valueSet = (List<String>) params.get(key);

                    if (key.equals("import_data")) {
                        if (valueSet.size() > 0 && !(valueSet.size() == 1 && valueSet.get(0).equals(""))) {
                            List<Object> shouldClauses = new ArrayList<>();
                            for (String obj : valueSet) {
                                try {
                                    JsonObject json = JsonParser.parseString(obj).getAsJsonObject();
                                    String study = json.get("study_id").getAsString();
                                    JsonArray participants = json.getAsJsonArray("participant_id");
                                    List<String> participantList = new ArrayList<>();
                                    for (JsonElement p : participants) {
                                        participantList.add(p.getAsString());
                                    }
                                    shouldClauses.add(
                                        Map.of(
                                            "bool", Map.of(
                                                "filter", List.of(
                                                    Map.of("term", Map.of("study_id", study)),
                                                    Map.of("terms", Map.of("participant_id", participantList))
                                                )
                                            )
                                        )
                                    );
                                } catch (Exception e) {
                                    // Handle parse error if needed
                                }
                            }
                            filter.add(
                                Map.of(
                                    "bool", Map.of(
                                        "should", shouldClauses
                                    )
                                )
                            );
                        }
                        continue;
                    }
                    
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
                // Opensearch ranges are [from, to)
        subField_ranges.put("ranges", Set.of(
            Map.of(
                "key", "0 - 4",
                "from", 0,
                "to", 5 * 365
            ), Map.of(
                "key", "5 - 9",
                "from", 5 * 365,
                "to", 10 * 365
            ), Map.of(
                "key", "10 - 14",
                "from", 10 * 365,
                "to", 15 * 365
            ), Map.of(
                "key", "15 - 19",
                "from", 15 * 365,
                "to", 20 * 365
            ), Map.of(
                "key", "20 - 29",
                "from", 20 * 365,
                "to", 30 * 365
            ), Map.of(
                "key", "> 29",
                "from", 30 * 365
            )
        ));
                
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

    /**
     * Retrieves all unique bucket names for a property aggregation
     * Used for cohort charts to determine which groups to display
     * @param property The property to aggregate on (e.g., "treatment_type", "race")
     * @param params Filter parameters (including participant IDs)
     * @param rangeParams Set of parameters that are ranges (not used in bucket names)
     * @param cardinalityAggName Field name for cardinality aggregation (e.g., "pid" for participant id)
     * @param index The index type (e.g., "participants", "treatments")
     * @param endpoint The OpenSearch endpoint to query
     * @return List of bucket names (unique values) for the property
     * @throws IOException
     */
    public List<String> getBucketNames(String property, Map<String, Object> params, Set<String> rangeParams, String cardinalityAggName, String index, String endpoint) throws IOException {
        List<String> bucketNames = new ArrayList<String>();
        Map<String, Object> query = buildFacetFilterQuery(params, rangeParams, Set.of(), Set.of(), "", index);

        // Add aggs clause to Opensearch query
        String[] aggNames = new String[] {property};
        query = addAggregations(query, aggNames, cardinalityAggName, List.of());

        // Send Opensearch request and retrieve list of buckets
        Request request = new Request("GET", endpoint);
        String jsonizedRequest = gson.toJson(query);
        request.setJsonEntity(jsonizedRequest);
        JsonObject jsonObject = send(request);
        Map<String, JsonArray> aggs = collectTermAggs(jsonObject, aggNames);
        JsonArray buckets = aggs.get(property);

        if (buckets != null) {
            for (JsonElement bucket : buckets) {
                JsonObject bucketObj = bucket.getAsJsonObject();
                if (bucketObj.has("key")) {
                    bucketNames.add(bucketObj.get("key").getAsString());
                }
            }
        }

        return bucketNames;
    }

}