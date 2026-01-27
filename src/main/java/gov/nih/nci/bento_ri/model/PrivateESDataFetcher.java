package gov.nih.nci.bento_ri.model;

import gov.nih.nci.bento.constants.Const;
import gov.nih.nci.bento.model.AbstractPrivateESDataFetcher;
import gov.nih.nci.bento.model.search.yaml.YamlQueryFactory;
import gov.nih.nci.bento.service.ESService;
import gov.nih.nci.bento_ri.service.InventoryESService;
import gov.nih.nci.bento_ri.service.CPIFetcherService;
import gov.nih.nci.bento_ri.model.FormattedCPIResponse;
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
import com.google.gson.Gson;

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
    private CPIFetcherService cpiFetcherService;
    @Autowired
    private Cache<String, Object> caffeineCache;

    final String CARDINALITY_AGG_NAME = "cardinality_agg_name";
    final String AGG_NAME = "agg_name";
    final String AGG_ENDPOINT = "agg_endpoint";
    final String WIDGET_QUERY = "widgetQueryName";
    final String FILTER_COUNT_QUERY = "filterCountQueryName";

    // parameters used in queries
    final String PAGE_SIZE = "first";
    final String OFFSET = "offset";
    final String ORDER_BY = "order_by";
    final String SORT_DIRECTION = "sort_direction";

    final String COHORTS_END_POINT = "/cohorts/_search";
    final String STUDIES_FACET_END_POINT = "/study_participants/_search";
    final String PARTICIPANTS_END_POINT = "/participants/_search";
    final String SURVIVALS_END_POINT = "/survivals/_search";
    final String TREATMENT_END_POINT = "/treatments/_search";
    final String TREATMENT_RESPONSE_END_POINT = "/treatment_responses/_search";
    final String DIAGNOSIS_END_POINT = "/diagnosis/_search";
    final String STUDIES_END_POINT = "/studies/_search";
    final String SAMPLES_END_POINT = "/samples/_search";
    final String FILES_END_POINT = "/files/_search";
    final String GS_ABOUT_END_POINT = "/ccdi_hub_static_pages/_search";
    final String NODES_END_POINT = "/model_nodes/_search";
    final String PROPERTIES_END_POINT = "/model_properties/_search";
    final String VALUES_END_POINT = "/model_values/_search";

    final String PARTICIPANTS_COUNT_END_POINT = "/participants/_count";
    final String DIAGNOSIS_COUNT_END_POINT = "/diagnosis/_count";
    final String STUDIES_COUNT_END_POINT = "/studies/_count";
    final String SAMPLES_COUNT_END_POINT = "/samples/_count";
    final String FILES_COUNT_END_POINT = "/files/_count";
    final String NODES_COUNT_END_POINT = "/model_nodes/_count";
    final String PROPERTIES_COUNT_END_POINT = "/model_properties/_count";
    final String VALUES_COUNT_END_POINT = "/model_values/_count";

    final String GS_END_POINT = "endpoint";
    final String GS_COUNT_ENDPOINT = "count_endpoint";
    final String GS_RESULT_FIELD = "result_field";
    final String GS_COUNT_RESULT_FIELD = "count_result_field";
    final String GS_SEARCH_FIELD = "search_field";
    final String GS_COLLECT_FIELDS = "collect_fields";
    final String GS_SORT_FIELD = "sort_field";
    final String GS_CATEGORY_TYPE = "category_type";
    final String GS_ABOUT = "about";
    final String GS_HIGHLIGHT_FIELDS = "highlight_fields";
    final String GS_HIGHLIGHT_DELIMITER = "$";
    
    final String ADDITIONAL_UPDATE = "additional_update";

    final Set<String> RANGE_PARAMS = Set.of("age_at_diagnosis", "participant_age_at_collection","age_at_treatment_start", "age_at_response", "age_at_last_known_survival_status");

    final Set<String> BOOLEAN_PARAMS = Set.of("data_category");

    final Set<String> ARRAY_PARAMS = Set.of("file_type");

    final Set<String> INCLUDE_PARAMS  = Set.of("race", "data_category");

    // Cohort Chart bucket limits
    final int COHORT_CHART_BUCKET_LIMIT_HIGH = 20;
    final int COHORT_CHART_BUCKET_LIMIT_LOW = 5;

    final Set<String> REGULAR_PARAMS = Set.of("study_id", "participant_id", "diagnosis_id", "race", "sex_at_birth", "diagnosis", "disease_phase", "diagnosis_classification_system", "diagnosis_category", "diagnosis_basis", "tumor_grade_source", "tumor_stage_source", "diagnosis_anatomic_site", "age_at_diagnosis", "last_known_survival_status", "age_at_last_known_survival_status", "first_event", "treatment_type", "treatment_agent", "age_at_treatment_start", "response_category", "age_at_response", "sample_anatomic_site", "participant_age_at_collection", "sample_tumor_status", "tumor_classification", "data_category", "file_type", "dbgap_accession", "study_acronym", "study_short_title", "library_selection", "library_source_material", "library_source_molecule", "library_strategy");
    final Set<String> PARTICIPANT_REGULAR_PARAMS = Set.of("participant_id", "race", "sex_at_birth", "diagnosis", "disease_phase", "diagnosis_classification_system", "diagnosis_basis", "tumor_grade_source", "tumor_stage_source", "diagnosis_anatomic_site", "diagnosis_category", "age_at_diagnosis", "last_known_survival_status", "age_at_last_known_survival_status","first_event", "treatment_type", "treatment_agent", "age_at_treatment_start", "response_category", "age_at_response", "sample_anatomic_site", "participant_age_at_collection", "sample_tumor_status", "tumor_classification", "data_category", "file_type", "dbgap_accession", "study_acronym", "study_short_title", "library_selection", "library_source_material", "library_source_molecule", "library_strategy");
    final Set<String> DIAGNOSIS_REGULAR_PARAMS = Set.of("participant_id", "sample_id", "race", "sex_at_birth", "dbgap_accession", "study_acronym", "study_name", "diagnosis", "disease_phase", "diagnosis_classification_system", "diagnosis_basis", "tumor_grade_source", "tumor_stage_source", "diagnosis_anatomic_site", "age_at_diagnosis");
    final Set<String> SAMPLE_REGULAR_PARAMS = Set.of("participant_id", "race", "sex_at_birth", "dbgap_accession", "study_acronym", "study_name", "sample_anatomic_site", "participant_age_at_collection", "sample_tumor_status", "tumor_classification", "diagnosis_category");
    final Set<String> STUDY_REGULAR_PARAMS = Set.of("study_id", "dbgap_accession", "study_acronym", "study_name", "study_status");
    final Set<String> FILE_REGULAR_PARAMS = Set.of("data_category", "dbgap_accession", "study_acronym", "study_name", "file_type", "library_selection", "library_source_material", "library_source_molecule", "library_strategy", "file_mapping_level");

    //default supporting data
    final Map<String, String> DEFAULT_IDC_DATA = Map.of(
        "phs002790", new Gson().toJson(Map.of("collection_id", "ccdi_mci",
        "cancer_type", "Various", 
        "date_updated", "2025-09-12", 
        "description", "The Molecular Characterization Initiative (MCI) is a component of the National Cancer Institute’s (NCI) Childhood Cancer Data Initiative (CCDI). It offers state-of-the-art molecular testing at no cost to newly diagnosed children, adolescents, and young adults (AYAs) with central nervous system (CNS) tumors, soft tissue sarcomas (STS), certain rare childhood cancers (RAR), and certain neuroblastomas (NBL) treated at a Children’s Oncology Group (COG)–affiliated hospital. The goal of MCI is to enhance the understanding of genetic factors in pediatric cancers and to provide timely, clinically relevant findings to doctors and families to aid in treatment decisions and determine eligibility for certain planned COG clinical trials.</p>\n<p>\nPlease see the <a target=\"_blank\" href=\"https://doi.org/10.5281/zenodo.11099086\" data-toggle=\"modal\" data-target=\"#external-web-warning\" class=\"external-link\" data-toggle=\"modal\" data-target=\"#external-web-warning\">DICOM converted whole slide hematoxylin and eosin stained images from the Molecular Characterization Initiative of the National Cancer Institute's Childhood Cancer Data Initiative\n <i class=\"fa-solid fa-external-link external-link-icon\" aria-hidden=\"true\"></i></a> information page to learn more about the images and any supporting metadata for this collection, and to learn about attribution/citation requirements.</p>\n", 
        "doi", "10.5281/zenodo.11099086", "image_types", "SM", "location", "Various", "species", "Human", "subject_count", "4055", "supporting_data", ""))
    );
    final Map<String, String> DEFAULT_TCIA_DATA = Map.of();
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
                        .dataFetcher("cohortManifest", env -> {
                            Map<String, Object> args = env.getArguments();
                            return cohortManifest(args);
                        })
                        .dataFetcher("cohortMetadata", env -> {
                            Map<String, Object> args = env.getArguments();
                            return cohortMetadata(args);
                        })
                        .dataFetcher("cohortCharts", env -> {
                            Map<String, Object> args = env.getArguments();
                            return cohortCharts(args);
                        })
                        .dataFetcher("studyDetails", env -> {
                            Map<String, Object> args = env.getArguments();
                            return studyDetails(args);
                        })
                        .dataFetcher("studiesListing", env -> {
                            Map<String, Object> args = env.getArguments();
                            return studiesListing(args);
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
                        .dataFetcher("numberOfStudies", env -> {
                            Map<String, Object> args = env.getArguments();
                            return numberOfStudies(args);
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
                        .dataFetcher("filesManifestInList", env -> {
                            Map<String, Object> args = env.getArguments();
                            return filesManifestInList(args);
                        })
                        .dataFetcher("globalSearch", env -> {
                            Map<String, Object> args = env.getArguments();
                            return globalSearch(args);
                        })
                        .dataFetcher("getFilenames", env -> {
                            Map<String, Object> args = env.getArguments();
                            return getFilenames(args);
                        })
                )
                .build();
    }

    private Map<String, Object> addHighlight(Map<String, Object> query, Map<String, Object> category) {
        Map<String, Object> result = new HashMap<>(query);
        List<String> searchFields = (List<String>)category.get(GS_SEARCH_FIELD);
        Map<String, Object> highlightClauses = new HashMap<>();
        for (String searchFieldName: searchFields) {
            highlightClauses.put(searchFieldName, Map.of());
        }

        result.put("highlight", Map.of(
                        "fields", highlightClauses,
                        "pre_tags", "",
                        "post_tags", "",
                        "fragment_size", 1
                )
        );
        return result;
    }

    private Map<String, Object> getGlobalSearchQuery(String input, Map<String, Object> category) {
        List<String> searchFields = (List<String>)category.get(GS_SEARCH_FIELD);
        List<Object> searchClauses = new ArrayList<>();
        for (String searchFieldName: searchFields) {
            searchClauses.add(Map.of("match_phrase_prefix", Map.of(searchFieldName, input)));
        }
        Map<String, Object> query = new HashMap<>();
        String indexType = (String)category.get(GS_CATEGORY_TYPE);
        if (indexType.equals("file")) {
            query.put("query", Map.of("bool", Map.of("must", Map.of("exists", Map.of("field", "file_id")), "should", searchClauses, "minimum_should_match", 1)));
        } else {
            query.put("query", Map.of("bool", Map.of("should", searchClauses)));
        }
        return query;
    }

    private List paginate(List org, int pageSize, int offset) {
        List<Object> result = new ArrayList<>();
        int size = org.size();
        if (offset <= size -1) {
            int end_index = offset + pageSize;
            if (end_index > size) {
                end_index = size;
            }
            result = org.subList(offset, end_index);
        }
        return result;
    }

    private List<Map<String, Object>> searchAboutPage(String input) throws IOException {
        final String ABOUT_CONTENT = "content.paragraph";
        Map<String, Object> query = Map.of(
                "query", Map.of("match", Map.of(ABOUT_CONTENT, input)),
                "highlight", Map.of(
                        "fields", Map.of(ABOUT_CONTENT, Map.of()),
                        "pre_tags", GS_HIGHLIGHT_DELIMITER,
                        "post_tags", GS_HIGHLIGHT_DELIMITER
                ),
                "size", 10000
        );
        Request request = new Request("GET", GS_ABOUT_END_POINT);
        request.setJsonEntity(gson.toJson(query));
        JsonObject jsonObject = esService.send(request);

        List<Map<String, Object>> result = new ArrayList<>();

        for (JsonElement hit: jsonObject.get("hits").getAsJsonObject().get("hits").getAsJsonArray()) {
            String page = hit.getAsJsonObject().get("_source").getAsJsonObject().get("page").getAsString();
            String title = hit.getAsJsonObject().get("_source").getAsJsonObject().get("title").getAsString();
            JsonArray arr = hit.getAsJsonObject().get("highlight").getAsJsonObject().get(ABOUT_CONTENT).getAsJsonArray();
            List<String> list = new ArrayList<String>();
            for (var element: arr) {
                list.add(element.getAsString());
            }
            result.add(Map.of(
                    GS_CATEGORY_TYPE, GS_ABOUT,
                    "page", page,
                    "title", title,
                    "text", list
            ));
        }

        return result;
    }

    private Map<String, Object> globalSearch(Map<String, Object> params) throws IOException {
        Map<String, Object> result = new HashMap<>();
        String input = (String) params.get("input");
        int size = (int) params.get("first");
        int offset = (int) params.get("offset");
        List<Map<String, Object>> searchCategories = new ArrayList<>();
        searchCategories.add(Map.of(
                GS_END_POINT, PARTICIPANTS_END_POINT,
                GS_COUNT_ENDPOINT, PARTICIPANTS_COUNT_END_POINT,
                GS_COUNT_RESULT_FIELD, "participant_count",
                GS_RESULT_FIELD, "participants",
                GS_SEARCH_FIELD, List.of("participant_id_gs","diagnosis_str_gs", "diagnosis_category_str_gs", "study_id_gs", "age_at_diagnosis_str_gs", "treatment_type_str_gs", "sex_at_birth_gs", "treatment_agent_str_gs", "race_str_gs", "last_known_survival_status_str_gs"),
                GS_SORT_FIELD, "participant_id",
                GS_COLLECT_FIELDS, new String[][]{
                        new String[]{"id", "id"},
                        new String[]{"participant_id", "participant_id"},
                        new String[]{"diagnosis_str", "diagnosis_str"},
                        new String[]{"diagnosis_category_str", "diagnosis_category_str"},
                        new String[]{"age_at_diagnosis_str", "age_at_diagnosis_str"},
                        new String[]{"treatment_agent_str", "treatment_agent_str"},
                        new String[]{"treatment_type_str", "treatment_type_str"},
                        new String[]{"study_id", "study_id"},
                        new String[]{"race_str", "race_str"},
                        new String[]{"sex_at_birth", "sex_at_birth"},
                        new String[]{"last_known_survival_status_str", "last_known_survival_status_str"}
                },
                GS_CATEGORY_TYPE, "subject"
        ));
        searchCategories.add(Map.of(
                GS_END_POINT, STUDIES_END_POINT,
                GS_COUNT_ENDPOINT, STUDIES_COUNT_END_POINT,
                GS_COUNT_RESULT_FIELD, "study_count",
                GS_RESULT_FIELD, "studies",
                GS_SEARCH_FIELD, List.of("study_id_gs", "study_name_gs", "study_status_gs"),
                GS_SORT_FIELD, "study_id",
                GS_COLLECT_FIELDS, new String[][]{
                        new String[]{"study_id", "study_id"},
                        new String[]{"study_name", "study_name"},
                        new String[]{"study_status", "study_status"},
                        new String[]{"num_of_participants", "num_of_participants"},
                        new String[]{"num_of_samples", "num_of_samples"},
                        new String[]{"num_of_files", "num_of_files"}
                },
                GS_CATEGORY_TYPE, "study"
        ));
        searchCategories.add(Map.of(
                GS_END_POINT, SAMPLES_END_POINT,
                GS_COUNT_ENDPOINT, SAMPLES_COUNT_END_POINT,
                GS_COUNT_RESULT_FIELD, "sample_count",
                GS_RESULT_FIELD, "samples",
                GS_SEARCH_FIELD, List.of("sample_id_gs", "participant_id_gs", "study_id_gs", "sample_anatomic_site_str_gs", "diagnosis_category_str_gs", "sample_tumor_status_gs", "diagnosis_str_gs", "tumor_classification_gs"),
                GS_SORT_FIELD, "sample_id",
                GS_COLLECT_FIELDS, new String[][]{
                        new String[]{"sample_id", "sample_id"},
                        new String[]{"participant_id", "participant_id"},
                        new String[]{"study_id", "study_id"},
                        new String[]{"sample_anatomic_site_str", "sample_anatomic_site_str"},
                        new String[]{"sample_tumor_status", "sample_tumor_status"},
                        new String[]{"diagnosis_str", "diagnosis_str"},
                        new String[]{"diagnosis_category_str", "diagnosis_category_str"},
                        new String[]{"tumor_classification", "tumor_classification"}
                },
                GS_CATEGORY_TYPE, "sample"
        ));
        searchCategories.add(Map.of(
                GS_END_POINT, FILES_END_POINT,
                GS_COUNT_ENDPOINT, FILES_COUNT_END_POINT,
                GS_COUNT_RESULT_FIELD, "file_count",
                GS_RESULT_FIELD, "files",
                GS_SEARCH_FIELD, List.of("participant_id_gs","sample_id_gs","study_id_gs","file_description_gs","file_type_gs","file_name_gs","data_category_gs"),
                GS_SORT_FIELD, "file_id",
                GS_COLLECT_FIELDS, new String[][]{
                        new String[]{"id", "id"},
                        new String[]{"participant_id", "participant_id"},
                        new String[]{"sample_id", "sample_id"},
                        new String[]{"study_id", "study_id"},
                        new String[]{"file_name", "file_name"},
                        new String[]{"data_category", "data_category"},
                        new String[]{"file_description", "file_description"},
                        new String[]{"file_type","file_type"},
                        new String[]{"file_size","file_size"}
                },
                GS_CATEGORY_TYPE, "file"
        ));
        searchCategories.add(Map.of(
                GS_END_POINT, NODES_END_POINT,
                GS_COUNT_ENDPOINT, NODES_COUNT_END_POINT,
                GS_COUNT_RESULT_FIELD, "model_count",
                GS_RESULT_FIELD, "model",
                GS_SEARCH_FIELD, List.of("node"),
                GS_SORT_FIELD, "node_kw",
                GS_COLLECT_FIELDS, new String[][]{
                        new String[]{"node", "node"}
                },
                GS_HIGHLIGHT_FIELDS, new String[][] {
                        new String[]{"highlight", "node"}
                },
                GS_CATEGORY_TYPE, "node"
        ));
        searchCategories.add(Map.of(
                GS_END_POINT, PROPERTIES_END_POINT,
                GS_COUNT_ENDPOINT, PROPERTIES_COUNT_END_POINT,
                GS_COUNT_RESULT_FIELD, "model_count",
                GS_RESULT_FIELD, "model",
                GS_SEARCH_FIELD, List.of("property", "property_description", "property_type", "property_required"),
                GS_SORT_FIELD, "property_kw",
                GS_COLLECT_FIELDS, new String[][]{
                        new String[]{"node", "node"},
                        new String[]{"property", "property"},
                        new String[]{"property_type", "property_type"},
                        new String[]{"property_required", "property_required"},
                        new String[]{"property_description", "property_description"}
                },
                GS_HIGHLIGHT_FIELDS, new String[][] {
                        new String[]{"highlight", "property"},
                        new String[]{"highlight", "property_description"},
                        new String[]{"highlight", "property_type"},
                        new String[]{"highlight", "property_required"}
                },
                GS_CATEGORY_TYPE, "property"
        ));
        searchCategories.add(Map.of(
                GS_END_POINT, VALUES_END_POINT,
                GS_COUNT_ENDPOINT, VALUES_COUNT_END_POINT,
                GS_COUNT_RESULT_FIELD, "model_count",
                GS_RESULT_FIELD, "model",
                GS_SEARCH_FIELD, List.of("value"),
                GS_SORT_FIELD, "value_kw",
                GS_COLLECT_FIELDS, new String[][]{
                        new String[]{"node", "node"},
                        new String[]{"property", "property"},
                        new String[]{"property_type", "property_type"},
                        new String[]{"property_required", "property_required"},
                        new String[]{"property_description", "property_description"},
                        new String[]{"value", "value"}
                },
                GS_HIGHLIGHT_FIELDS, new String[][] {
                        new String[]{"highlight", "value"}
                },
                GS_CATEGORY_TYPE, "value"
        ));

        Set<String> combinedCategories = Set.of("model") ;

        for (Map<String, Object> category: searchCategories) {
            String countResultFieldName = (String) category.get(GS_COUNT_RESULT_FIELD);
            String resultFieldName = (String) category.get(GS_RESULT_FIELD);
            String[][] properties = (String[][]) category.get(GS_COLLECT_FIELDS);
            Map<String, Object> query = getGlobalSearchQuery(input, category);

            // Get count
            Request countRequest = new Request("GET", (String) category.get(GS_COUNT_ENDPOINT));
            countRequest.setJsonEntity(gson.toJson(query));
            JsonObject countResult = esService.send(countRequest);
            int oldCount = (int)result.getOrDefault(countResultFieldName, 0);
            result.put(countResultFieldName, countResult.get("count").getAsInt() + oldCount);

            // Get results
            Request request = new Request("GET", (String)category.get(GS_END_POINT));
            String sortFieldName = (String)category.get(GS_SORT_FIELD);
            query.put("sort", Map.of(sortFieldName, "asc"));
            query = addHighlight(query, category);

            if (combinedCategories.contains(resultFieldName)) {
                size = 10000;
                offset = 0;
            }

            List<String> dataFields = new ArrayList<>();
            for (String[] prop: properties) {
                String dataField = prop[1];
                dataFields.add(dataField);
            }
            query.put("_source", Map.of("includes", dataFields));

            request.setJsonEntity(gson.toJson(query));
            List<Map<String, Object>> objects = inventoryESService.collectPage(request, query, properties, size, offset);

            for (var object: objects) {
                object.put(GS_CATEGORY_TYPE, category.get(GS_CATEGORY_TYPE));
            }

            // Add CPI data enrichment for participants
            if (resultFieldName.equals("participants") && objects != null && !objects.isEmpty()) {
                // Check if CPIFetcherService is properly injected
                if (cpiFetcherService != null) {
                    try {
                        // Extract IDs from the participant objects for CPI fetching
                        List<ParticipantRequest> extracted_ids = extractIDs(objects);
                        
                        // Fetch CPI data
                        List<FormattedCPIResponse> cpi_data = cpiFetcherService.fetchAssociatedParticipantIds(extracted_ids);
                        logger.info("GlobalSearch CPI data received: " + cpi_data.size() + " records");
                        
                        if (cpi_data != null && !cpi_data.isEmpty()) {
                            // Enrich CPI data with additional participant information
                            enrichCPIDataWithParticipantInfo(cpi_data);
                            
                            // Update the participant objects with enriched CPI data
                            updateParticipantListWithEnrichedCPIData(objects, cpi_data);
                            
                            logger.info("GlobalSearch participants enriched with CPI data");
                        }
                    } catch (Exception e) {
                        logger.error("Error enriching GlobalSearch participants with CPI data", e);
                        // Continue processing even if CPI enrichment fails
                    }
                } else {
                    logger.warn("CPIFetcherService is not properly injected. CPI integration will be skipped for GlobalSearch.");
                }
            }

            List<Map<String, Object>> existingObjects = (List<Map<String, Object>>)result.getOrDefault(resultFieldName, null);
            if (existingObjects != null) {
                existingObjects.addAll(objects);
                result.put(resultFieldName, existingObjects);
            } else {
                result.put(resultFieldName, objects);
            }

        }

        List<Map<String, Object>> about_results = searchAboutPage(input);
        int about_count = about_results.size();
        result.put("about_count", about_count);
        result.put("about_page", paginate(about_results, size, offset));
        int old_size = (int) params.get("first");
        int old_offset = (int) params.get("offset");
        for (String category: combinedCategories) {
            List<Object> pagedCategory = paginate((List)result.get(category), old_size, old_offset);
            result.put(category, pagedCategory);
        }

        return result;
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
                
                List<Map<String, Object>> result = esService.collectPage(request, query, properties, 200000,
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
        List<String> importData = (List<String>) params.get("import_data");
        String cacheKey = "no_cache";
        Map<String, Object> data = null;
        if (importData == null || importData.size() == 0 || importData.get(0).equals("")) {
            cacheKey = generateCacheKey(params);
            data = (Map<String, Object>)caffeineCache.asMap().get(cacheKey);
        }
        
        if (data != null) {
            logger.info("hit cache!");
            return data;
        } else {
            // logger.info("cache miss... querying for data.");
            data = new HashMap<>();
            // Query related values
            final List<Map<String, Object>> PARTICIPANT_TERM_AGGS = new ArrayList<>();
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
                    AGG_NAME, "dbgap_accession",
                    FILTER_COUNT_QUERY, "filterParticipantCountByDBGAPAccession",
                    AGG_ENDPOINT, PARTICIPANTS_END_POINT
            ));
            PARTICIPANT_TERM_AGGS.add(Map.of(
                    AGG_NAME, "study_acronym",
                    WIDGET_QUERY, "participantCountByStudy",
                    FILTER_COUNT_QUERY, "filterParticipantCountByAcronym",
                    AGG_ENDPOINT, PARTICIPANTS_END_POINT
            ));
            PARTICIPANT_TERM_AGGS.add(Map.of(
                    CARDINALITY_AGG_NAME, "pid",
                    AGG_NAME, "diagnosis_anatomic_site",
                    FILTER_COUNT_QUERY, "filterParticipantCountByDiagnosisAnatomicSite",
                    AGG_ENDPOINT, DIAGNOSIS_END_POINT
            ));
            PARTICIPANT_TERM_AGGS.add(Map.of(
                    CARDINALITY_AGG_NAME, "pid",
                    AGG_NAME, "diagnosis_category",
                    FILTER_COUNT_QUERY, "filterParticipantCountByDiagnosisCategory",
                    AGG_ENDPOINT, DIAGNOSIS_END_POINT
            ));
            PARTICIPANT_TERM_AGGS.add(Map.of(
                    CARDINALITY_AGG_NAME, "pid",
                    AGG_NAME, "disease_phase",
                    FILTER_COUNT_QUERY, "filterParticipantCountByDiseasePhase",
                    ADDITIONAL_UPDATE, Map.of("Initial Diagnosis", 4000, "Not Reported", 3500),
                    AGG_ENDPOINT, DIAGNOSIS_END_POINT
            ));
            PARTICIPANT_TERM_AGGS.add(Map.of(
                    CARDINALITY_AGG_NAME, "pid",
                    AGG_NAME, "diagnosis_classification_system",
                    FILTER_COUNT_QUERY, "filterParticipantCountByDiagnosisClassificationSystem",
                    ADDITIONAL_UPDATE, Map.of("ICD-O-3.2", 5000, "Indication for Study", 1000),
                    AGG_ENDPOINT, DIAGNOSIS_END_POINT
            ));
            PARTICIPANT_TERM_AGGS.add(Map.of(
                    CARDINALITY_AGG_NAME, "pid",
                    AGG_NAME, "diagnosis_basis",
                    FILTER_COUNT_QUERY, "filterParticipantCountByDiagnosisBasis",
                    ADDITIONAL_UPDATE, Map.of("Clinical", 3500, "Not Reported", 2000, "Unknown", 4000),
                    AGG_ENDPOINT, DIAGNOSIS_END_POINT
            ));
            PARTICIPANT_TERM_AGGS.add(Map.of(
                CARDINALITY_AGG_NAME, "pid",
                AGG_NAME, "treatment_type",
                FILTER_COUNT_QUERY, "filterParticipantCountByTreatmentType",
                ADDITIONAL_UPDATE, Map.of("Chemotherapy", 3000, "Radiation Therapy", 1500, "Surgical Procedure", 2000),
                AGG_ENDPOINT, TREATMENT_END_POINT
            ));
            PARTICIPANT_TERM_AGGS.add(Map.of(
                CARDINALITY_AGG_NAME, "pid",
                AGG_NAME, "treatment_agent",
                FILTER_COUNT_QUERY, "filterParticipantCountByTreatmentAgent",
                AGG_ENDPOINT, TREATMENT_END_POINT
            ));
            PARTICIPANT_TERM_AGGS.add(Map.of(
                CARDINALITY_AGG_NAME, "pid",
                AGG_NAME, "age_at_treatment_start",
                FILTER_COUNT_QUERY, "filterParticipantCountByAgeAtTreatmentStart",
                AGG_ENDPOINT, TREATMENT_END_POINT
            ));
            PARTICIPANT_TERM_AGGS.add(Map.of(
                CARDINALITY_AGG_NAME, "pid",
                AGG_NAME, "response_category",
                FILTER_COUNT_QUERY, "filterParticipantCountByResponseCategory",
                AGG_ENDPOINT, TREATMENT_RESPONSE_END_POINT
            ));
            PARTICIPANT_TERM_AGGS.add(Map.of(
                CARDINALITY_AGG_NAME, "pid",
                AGG_NAME, "age_at_response",
                FILTER_COUNT_QUERY, "filterParticipantCountByAgeAtResponse",
                AGG_ENDPOINT, TREATMENT_RESPONSE_END_POINT
            ));
            PARTICIPANT_TERM_AGGS.add(Map.of(
                CARDINALITY_AGG_NAME, "pid",
                AGG_NAME, "last_known_survival_status",
                FILTER_COUNT_QUERY, "filterParticipantCountBySurvivalStatus",
                ADDITIONAL_UPDATE, Map.of("Alive", 3500, "Unknown", 5000),
                AGG_ENDPOINT, SURVIVALS_END_POINT
            ));
            PARTICIPANT_TERM_AGGS.add(Map.of(
                CARDINALITY_AGG_NAME, "pid",
                AGG_NAME, "age_at_last_known_survival_status",
                FILTER_COUNT_QUERY, "filterParticipantCountByAgeAtLastKnownSurvivalStatus",
                AGG_ENDPOINT, SURVIVALS_END_POINT
            ));
            PARTICIPANT_TERM_AGGS.add(Map.of(
                CARDINALITY_AGG_NAME, "pid",
                AGG_NAME, "first_event",
                FILTER_COUNT_QUERY, "filterParticipantCountByFirstEvent",
                AGG_ENDPOINT, SURVIVALS_END_POINT
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
                    ADDITIONAL_UPDATE, Map.of("Primary", 1000, "Not Applicable", 4000, "Not Reported", 1500),
                    AGG_ENDPOINT, SAMPLES_END_POINT
            ));
            //data_category mapped to data_category
            PARTICIPANT_TERM_AGGS.add(Map.of(
                    CARDINALITY_AGG_NAME, "pid",
                    AGG_NAME, "data_category",
                    WIDGET_QUERY, "participantCountByDataCategory",
                    FILTER_COUNT_QUERY, "filterParticipantCountByDataCategory",
                    ADDITIONAL_UPDATE, Map.of("Genomics", 1000, "Pathology Imaging", 1000, "Sequencing", 2000, "Clinical", 1500, "Copy Number Variation", 1000),
                    AGG_ENDPOINT, FILES_END_POINT
            ));
            PARTICIPANT_TERM_AGGS.add(Map.of(
                    CARDINALITY_AGG_NAME, "pid",
                    AGG_NAME, "file_type",
                    FILTER_COUNT_QUERY, "filterParticipantCountByFileType",
                    ADDITIONAL_UPDATE, new HashMap<String, Integer>() {{
                        put("bai", 1500);
                        put("bam", 3500);
                        put("crai", 3600);
                        put("cram", 4000);
                        put("fastq", 2000);
                        put("html", 3000);
                        put("idat", 1000);
                        put("json", 2000);
                        put("pdf", 3000);
                        put("tsv", 1000);
                        put("txt", 3500);
                        put("dicom", 500);
                        put("vcf", 3500);
                    }},
                    AGG_ENDPOINT, FILES_END_POINT
            ));
            PARTICIPANT_TERM_AGGS.add(Map.of(
                    // CARDINALITY_AGG_NAME, "pid",
                    // AGG_NAME, "study_name",
                    // FILTER_COUNT_QUERY, "filterParticipantCountByStudyTitle",
                    // ADDITIONAL_UPDATE, Map.of("Childhood Cancer Survivor Study (CCSS)", 2000, "Molecular Characterization Initiative", 1000),
                    // AGG_ENDPOINT, STUDIES_FACET_END_POINT
                    AGG_NAME, "study_name",
                    FILTER_COUNT_QUERY, "filterParticipantCountByStudyTitle",
                    AGG_ENDPOINT, PARTICIPANTS_END_POINT
            ));
            PARTICIPANT_TERM_AGGS.add(Map.of(
                    // CARDINALITY_AGG_NAME, "pid",
                    // AGG_NAME, "study_status",
                    // FILTER_COUNT_QUERY, "filterParticipantCountByStudyStatus",
                    // ADDITIONAL_UPDATE, Map.of("Active", 2000,"Completed", 3000),
                    // AGG_ENDPOINT, STUDIES_FACET_END_POINT
                    AGG_NAME, "study_status",
                    FILTER_COUNT_QUERY, "filterParticipantCountByStudyStatus",
                    AGG_ENDPOINT, PARTICIPANTS_END_POINT
            ));
            PARTICIPANT_TERM_AGGS.add(Map.of(
                    CARDINALITY_AGG_NAME, "pid",
                    AGG_NAME, "library_selection",
                    FILTER_COUNT_QUERY, "filterParticipantCountByLibrarySelection",
                    ADDITIONAL_UPDATE, Map.of("Hybrid Selection", 4500, "Other", 1000, "Unspecified", 1000),
                    AGG_ENDPOINT, FILES_END_POINT
            ));
            PARTICIPANT_TERM_AGGS.add(Map.of(
                    CARDINALITY_AGG_NAME, "pid",
                    AGG_NAME, "library_source_material",
                    FILTER_COUNT_QUERY, "filterParticipantCountByLibrarySourceMaterial",
                    ADDITIONAL_UPDATE, Map.of("Bulk Cells", 3000, "Not Reported", 2000),
                    AGG_ENDPOINT, FILES_END_POINT
            ));
            PARTICIPANT_TERM_AGGS.add(Map.of(
                    CARDINALITY_AGG_NAME, "pid",
                    AGG_NAME, "library_source_molecule",
                    FILTER_COUNT_QUERY, "filterParticipantCountByLibrarySourceMolecule",
                    ADDITIONAL_UPDATE, Map.of("Genomic", 5000, "Transcriptomic", 3500, "Not Reported", 1000),
                    AGG_ENDPOINT, FILES_END_POINT
            ));
            PARTICIPANT_TERM_AGGS.add(Map.of(
                    CARDINALITY_AGG_NAME, "pid",
                    AGG_NAME, "library_strategy",
                    FILTER_COUNT_QUERY, "filterParticipantCountByLibraryStrategy",
                    ADDITIONAL_UPDATE, Map.of("WXS", 2000, "Other", 500, "RNA-Seq", 1000, "WGS", 1500),
                    AGG_ENDPOINT, FILES_END_POINT
            ));
            PARTICIPANT_TERM_AGGS.add(Map.of(
                    CARDINALITY_AGG_NAME, "pid",
                    AGG_NAME, "file_mapping_level",
                    FILTER_COUNT_QUERY, "filterParticipantCountByFileMappingLevel",
                    ADDITIONAL_UPDATE, Map.of("Participant", 1000,"Sample", 5000),
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
            // System.out.println(participantsCountResult);
            // Map<String, Object> query_diagnosis = inventoryESService.buildFacetFilterQuery(params, RANGE_PARAMS, Set.of(), REGULAR_PARAMS, "nested_filters", "diagnosis");
            // Request diagnosisCountRequest = new Request("GET", DIAGNOSIS_COUNT_END_POINT);
            // System.out.println(gson.toJson(query_diagnosis));
            // diagnosisCountRequest.setJsonEntity(gson.toJson(query_diagnosis));
            // JsonObject diagnosisCountResult = inventoryESService.send(diagnosisCountRequest);
            // int numberOfDiagnosis = diagnosisCountResult.get("count").getAsInt();

            Map<String, Object> query_samples = inventoryESService.buildFacetFilterQuery(params, RANGE_PARAMS, Set.of(), REGULAR_PARAMS, "nested_filters", "samples");
            Map<String, Object> newQuery_samples = new HashMap<>(query_samples);
            newQuery_samples.put("size", 0);
            newQuery_samples.put("track_total_hits", 10000000);
            Map<String, Object> fields_sample = new HashMap<String, Object>();
            fields_sample.put("file_count", Map.of("sum", Map.of("field", "direct_file_count")));
            newQuery_samples.put("aggs", fields_sample);
            Request samplesCountRequest = new Request("GET", SAMPLES_END_POINT);
            // System.out.println(gson.toJson(newQuery_samples));
            samplesCountRequest.setJsonEntity(gson.toJson(newQuery_samples));
            JsonObject samplesCountResult = inventoryESService.send(samplesCountRequest);
            int numberOfSamples = samplesCountResult.getAsJsonObject("hits").getAsJsonObject("total").get("value").getAsInt();
            int samples_file_count = samplesCountResult.getAsJsonObject("aggregations").getAsJsonObject("file_count").get("value").getAsInt();

            Map<String, Object> query_files_all_records = inventoryESService.buildFacetFilterQuery(params, RANGE_PARAMS, Set.of(), REGULAR_PARAMS, "nested_filters", "files_overall");
            int numberOfStudies = getNodeCount("study_id", query_files_all_records, FILES_END_POINT).size();

            Request filesCountRequest = new Request("GET", FILES_COUNT_END_POINT);
            Map<String, Object> query_files_valid_records = inventoryESService.buildFacetFilterQuery(params, RANGE_PARAMS, Set.of(), REGULAR_PARAMS, "nested_filters", "files");
            // System.out.println(gson.toJson(query_files_valid_records));
            filesCountRequest.setJsonEntity(gson.toJson(query_files_valid_records));
            JsonObject filesCountResult = inventoryESService.send(filesCountRequest);
            int numberOfFiles = filesCountResult.get("count").getAsInt();

            data.put("numberOfStudies", numberOfStudies);
            data.put("numberOfDiagnosis", 0);
            data.put("numberOfParticipants", numberOfParticipants);
            data.put("numberOfSamples", numberOfSamples);
            data.put("numberOfFiles", numberOfFiles);
            data.put("participantsFileCount", participants_file_count);
            data.put("diagnosisFileCount", 0);
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
                        String prop = field;
                        String nestedProperty = "sample_diagnosis_file_filters";
                        if (indexType.equals("survivals")) {
                            nestedProperty = "survival_filters";
                        } else if (indexType.equals("treatments")) {
                            nestedProperty = "treatment_filters";
                        } else if (indexType.equals("treatment_responses")) {
                            nestedProperty = "treatment_response_filters";
                        } else {
                            nestedProperty = "sample_diagnosis_file_filters";
                        }
                        query_4_update = inventoryESService.addCustomAggregations(query_4_update, "facetAgg", prop, nestedProperty);
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
            if (!cacheKey.equals("no_cache")) {
                caffeineCache.put(cacheKey, data);
            }
            return data;
        }
    }

    private List<Map<String, Object>> participantOverview(Map<String, Object> params) throws IOException {
        // System.out.println(params);
        final String[][] PROPERTIES = new String[][]{
            new String[]{"id", "id"},
            new String[]{"participant_id", "participant_id"},
            new String[]{"dbgap_accession", "dbgap_accession"},
                new String[]{"study_id", "study_id"},
            new String[]{"race", "race_str"},
            new String[]{"sex_at_birth", "sex_at_birth"},
            new String[]{"synonym_id", "alternate_participant_id"},
            new String[]{"files", "files"},
            new String[]{"diagnosis", "diagnosis_str"},
            new String[]{"anatomic_site", "diagnosis_anatomic_site_str"},
            new String[]{"diagnosis_category", "diagnosis_category_str"},
            new String[]{"age_at_diagnosis", "age_at_diagnosis_str"},
            new String[]{"treatment_agent", "treatment_agent_str"},
            new String[]{"treatment_type", "treatment_type_str"},
            new String[]{"age_at_treatment_start", "age_at_treatment_start_str"},
            new String[]{"first_event", "first_event_str"},
            new String[]{"last_known_survival_status", "last_known_survival_status_str"},
            new String[]{"age_at_last_known_survival_status", "age_at_last_known_survival_status_str"},
        };

        String defaultSort = "participant_id"; // Default sort order

        Map<String, String> mapping = Map.ofEntries(
                Map.entry("id", "id"),
                Map.entry("participant_id", "participant_id"),
                Map.entry("dbgap_accession", "dbgap_accession"),
                Map.entry("study_id", "study_id"),
                Map.entry("race", "race_str"),
                Map.entry("sex_at_birth", "sex_at_birth"),
                Map.entry("synonym_id", "alternate_participant_id"),
                Map.entry("diagnosis", "diagnosis_str"),
                Map.entry("diagnosis_category", "diagnosis_category_str"),
                Map.entry("anatomic_site", "diagnosis_anatomic_site_str"),
                Map.entry("age_at_diagnosis", "age_at_diagnosis_str"),
                Map.entry("treatment_agent", "treatment_agent_str"),
                Map.entry("treatment_type", "treatment_type_str"),
                Map.entry("age_at_treatment_start", "age_at_treatment_start_str"),
                Map.entry("first_event", "first_event_str"),
                Map.entry("last_known_survival_status", "last_known_survival_status_str"),
                Map.entry("age_at_last_known_survival_status", "age_at_last_known_survival_status_str")
        );
        
        // Get the participant list from overview
        List<Map<String, Object>> participant_list = overview(PARTICIPANTS_END_POINT, params, PROPERTIES, defaultSort, mapping, PARTICIPANT_REGULAR_PARAMS, "nested_filters", "participants");
        
        // Extract IDs using helper function
        List<ParticipantRequest> extracted_ids = extractIDs(participant_list);
        
        // Check if CPIFetcherService is properly injected
        if (cpiFetcherService == null) {
            logger.warn("CPIFetcherService is not properly injected. CPI integration will be skipped.");
        } else {
            try {
                List<FormattedCPIResponse> cpi_data = cpiFetcherService.fetchAssociatedParticipantIds(extracted_ids);
                logger.info("CPI data received: " + cpi_data.size() + " records");
                
                // Print the first value as JSON
                if (cpi_data != null && !cpi_data.isEmpty()) {
                    // System.out.println("First CPI data value BEFORE enrichment: " + gson.toJson(cpi_data.get(0)));
                    
                    // Enrich CPI data with additional participant information
                    enrichCPIDataWithParticipantInfo(cpi_data);
                    
                    // Print the first enriched CPI data value
                    // System.out.println("First enriched CPI data value AFTER enrichment: " + gson.toJson(cpi_data.get(0)));
                    
                    // Update the participant_list with the enriched CPI data
                    updateParticipantListWithEnrichedCPIData(participant_list, cpi_data);
                    
                } else {
                    // System.out.println("CPI data is empty or null");
                }
            } catch (Exception e) {
                // System.err.println("Error fetching CPI data: " + e.getMessage());
                logger.error("Error fetching CPI data", e);
            }   
        }

        // System.out.println("Participant list size after enrichment: " + gson.toJson(participant_list));
        return participant_list;
    }
    
    /**
     * Helper function to extract participant_id and study_id from participant list
     * @param participant_list List of participant objects
     * @return List of ParticipantRequest objects containing participant_id and study_id
     */
    private List<ParticipantRequest> extractIDs(List<Map<String, Object>> participant_list) {
        List<ParticipantRequest> ids = new ArrayList<>();
        
        for (Map<String, Object> participant : participant_list) {
            // Extract participant_id
            Object participantId = participant.get("participant_id");
            String participantIdStr = participantId != null ? participantId.toString() : "";
            
            // Extract study_id
            Object studyId = participant.get("study_id");
            String studyIdStr = studyId != null ? studyId.toString() : "";
            
            // Create ParticipantRequest object
            ParticipantRequest participantRequest = new ParticipantRequest(participantIdStr, studyIdStr);
            ids.add(participantRequest);
        }
        
        return ids;
    }

    /**
     * Enriches CPI data with additional participant information using batch queries for improved performance
     */
    private void enrichCPIDataWithParticipantInfo(List<FormattedCPIResponse> cpiData) throws IOException {
        if (cpiData == null || cpiData.isEmpty()) {
            return;
        }

        // System.out.println("Starting CPI data enrichment for " + cpiData.size() + " records");

        // Step 1: Filter out records that don't have cpiData and collect those that do
        List<FormattedCPIResponse> recordsWithCpiData = new ArrayList<>();
        for (FormattedCPIResponse cpiEntry : cpiData) {
            if (hasCpiData(cpiEntry)) {
                recordsWithCpiData.add(cpiEntry);
            }
        }

        // System.out.println("Found " + recordsWithCpiData.size() + " records with cpiData to enrich");

        if (recordsWithCpiData.isEmpty()) {
            // System.out.println("No records with cpiData to enrich, skipping enrichment");
            return;
        }

        // Step 2: Build HashMap mapping study_id to participant_ids
        Map<String, Set<String>> studyToParticipantsMap = buildStudyToParticipantsMap(recordsWithCpiData);
        // System.out.println("Built study-to-participants mapping with " + studyToParticipantsMap.size() + " studies");

        // Step 3: Generate and execute batch OpenSearch query
        List<Map<String, Object>> batchQueryResults = executeBatchQuery(studyToParticipantsMap);
        // System.out.println("Batch query returned " + batchQueryResults.size() + " results");

        // Step 4: Enrich CPI data with batch query results
        enrichCpiDataWithBatchResults(recordsWithCpiData, batchQueryResults);

        // System.out.println("CPI data enrichment completed");
    }

    /**
     * Checks if a FormattedCPIResponse has cpiData
     */
    private boolean hasCpiData(FormattedCPIResponse cpiEntry) {
        try {
            java.lang.reflect.Field cpiDataField = cpiEntry.getClass().getDeclaredField("cpiData");
            cpiDataField.setAccessible(true);
            Object cpiDataValue = cpiDataField.get(cpiEntry);
            
            if (cpiDataValue instanceof List) {
                List<?> cpiDataList = (List<?>) cpiDataValue;
                return !cpiDataList.isEmpty();
            }
            return false;
        } catch (Exception e) {
            logger.debug("Error checking if record has cpiData: " + e.getMessage());
            return false;
        }
    }

    /**
     * Builds a HashMap mapping study_id (repository_of_synonym_id) to participant_ids (associated_id)
     */
    private Map<String, Set<String>> buildStudyToParticipantsMap(List<FormattedCPIResponse> recordsWithCpiData) {
        Map<String, Set<String>> studyToParticipantsMap = new HashMap<>();

        for (FormattedCPIResponse cpiEntry : recordsWithCpiData) {
            try {
                java.lang.reflect.Field cpiDataField = cpiEntry.getClass().getDeclaredField("cpiData");
                cpiDataField.setAccessible(true);
                Object cpiDataValue = cpiDataField.get(cpiEntry);

                if (cpiDataValue instanceof List) {
                    @SuppressWarnings("unchecked")
                    List<Object> cpiDataArray = (List<Object>) cpiDataValue;

                    for (Object cpiDataItem : cpiDataArray) {
                        Map<String, Object> cpiDataMap = convertToMap(cpiDataItem);
                        if (cpiDataMap != null) {
                            String studyId = extractStringValue(cpiDataMap, "repository_of_synonym_id");
                            String participantId = extractStringValue(cpiDataMap, "associated_id");

                            if (studyId != null && participantId != null) {
                                studyToParticipantsMap.computeIfAbsent(studyId, k -> new HashSet<>()).add(participantId);
                            }
                        }
                    }
                }
            } catch (Exception e) {
                logger.error("Error building study-to-participants map for CPI entry: " + e.getMessage(), e);
            }
        }

        return studyToParticipantsMap;
    }

    /**
     * Converts an object to a Map representation
     */
    private Map<String, Object> convertToMap(Object obj) {
        if (obj instanceof Map) {
            @SuppressWarnings("unchecked")
            Map<String, Object> map = (Map<String, Object>) obj;
            return map;
        } else {
            try {
                String jsonString = gson.toJson(obj);
                @SuppressWarnings("unchecked")
                Map<String, Object> map = gson.fromJson(jsonString, Map.class);
                return map;
            } catch (Exception e) {
                logger.debug("Error converting object to Map: " + e.getMessage());
                return null;
            }
        }
    }

    /**
     * Extracts string value from a map, handling both single values and arrays
     */
    private String extractStringValue(Map<String, Object> map, String key) {
        Object value = map.get(key);
        if (value == null) {
            return null;
        }
        
        if (value instanceof List) {
            List<?> list = (List<?>) value;
            if (!list.isEmpty() && list.get(0) != null) {
                return list.get(0).toString();
            }
        } else {
            return value.toString();
        }
        return null;
    }


    private Map<String, Object> studyDetails(Map<String, Object> params) throws IOException {
        Map<String, Object> study;
        String studyId = (String) params.get("study_id");
        List<Map<String, Object>> studies;

        final String[][] PROPERTIES = new String[][]{
            // Demographics
            new String[]{"id", "id"},
            new String[]{"study_id", "study_id"},
            new String[]{"dbgap_accession", "dbgap_accession"},
            new String[]{"study_name", "study_name"},
            new String[]{"study_description", "study_description"},
            new String[]{"pubmed_ids", "pubmed_ids"},
            new String[]{"num_of_participants", "num_of_participants"},
            new String[]{"num_of_samples", "num_of_samples"},
            new String[]{"num_of_files", "num_of_files"},
        };

        String defaultSort = "dbgap_accession"; // Default sort order

        Map<String, String> mapping = Map.ofEntries(
            Map.entry("study_id", "study_id"),
            Map.entry("study_name", "study_name"),
            Map.entry("study_description", "study_description"),
            Map.entry("pubmed_ids", "pubmed_ids"),
            Map.entry("num_of_participants", "num_of_participants"),
            Map.entry("num_of_samples", "num_of_samples"),
            Map.entry("num_of_files", "num_of_files")
        );

        Map<String, Object> study_params = Map.ofEntries(
            Map.entry("study_id", List.of(studyId)),
            Map.entry(ORDER_BY, "study_id"),
            Map.entry(SORT_DIRECTION, "ASC"),
            Map.entry(PAGE_SIZE, 1),
            Map.entry(OFFSET, 0)
        );

        studies = overview(STUDIES_END_POINT, study_params, PROPERTIES, defaultSort, mapping, REGULAR_PARAMS, "nested_filters", "studies");

        // studies = overview(STUDIES_END_POINT, study_params, PROPERTIES, "dbgap_accession", mapping, "studies");

        study = studies.get(0);

        // Get study level statistics
        Map<String, Object> query_params = Map.ofEntries(
            Map.entry("study_id", List.of(studyId))
        );
        Map<String, Object> data = new HashMap<>();
        final List<Map<String, Object>> PARTICIPANT_TERM_AGGS = new ArrayList<>();
        PARTICIPANT_TERM_AGGS.add(Map.of(
                CARDINALITY_AGG_NAME, "pid",
                AGG_NAME, "diagnosis",
                FILTER_COUNT_QUERY, "diagnoses",
                AGG_ENDPOINT, DIAGNOSIS_END_POINT
        ));
        PARTICIPANT_TERM_AGGS.add(Map.of(
                CARDINALITY_AGG_NAME, "pid",
                AGG_NAME, "diagnosis_anatomic_site",
                FILTER_COUNT_QUERY, "anatomic_sites",
                AGG_ENDPOINT, DIAGNOSIS_END_POINT
        ));
        //data_category mapped to data_category
        PARTICIPANT_TERM_AGGS.add(Map.of(
                CARDINALITY_AGG_NAME, "pid",
                AGG_NAME, "data_category",
                FILTER_COUNT_QUERY, "data_categories",
                ADDITIONAL_UPDATE, Map.of("Pathology Imaging", 1000, "Sequencing", 500, "Clinical", 1500),
                AGG_ENDPOINT, FILES_END_POINT
        ));
        for (var agg: PARTICIPANT_TERM_AGGS) {
            String field = (String)agg.get(AGG_NAME);
            Map<String, Integer> additionalUpdate = (Map<String, Integer>)agg.get(ADDITIONAL_UPDATE);
            String filterCountQueryName = (String)agg.get(FILTER_COUNT_QUERY);
            String endpoint = (String)agg.get(AGG_ENDPOINT);
            String indexType = endpoint.replace("/", "").replace("_search", "");
            String cardinalityAggName = (String)agg.get(CARDINALITY_AGG_NAME);
            // System.out.println(cardinalityAggName);
            List<Map<String, Object>> filterCount = filterSubjectCountBy(field, query_params, endpoint, cardinalityAggName, indexType);
            if(RANGE_PARAMS.contains(field)) {
                study.put(filterCountQueryName, filterCount.get(0));
            } else {
                study.put(filterCountQueryName, filterCount);
            }

            if (additionalUpdate != null) {
                List<Map<String, Object>> filterCount_2_update = (List<Map<String, Object>>)study.get(filterCountQueryName);
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
                    Map<String, Object> query_4_update = inventoryESService.buildFacetFilterQuery(query_params, RANGE_PARAMS, Set.of(field), REGULAR_PARAMS, "nested_filters", "participants");
                    String prop = field;
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
                    study.put(filterCountQueryName, filterCount_new);
                }
            }
        }

        // todo: querying idc_tcia index for the supporting data
        // if error, return default idc, tcia data
        String idcData = DEFAULT_IDC_DATA.get(studyId);
        String tciaData = DEFAULT_TCIA_DATA.get(studyId);
        if (idcData == null && tciaData == null) {
            study.put("supporting_data", new ArrayList<>());
        } else {
            //formatting the following code please:
            ArrayList<Map<String, Object>> supportingData = new ArrayList<>();
            if (idcData != null) {
                supportingData.add(Map.of("data_category", "IDC", "data_object", idcData));
            }
            if (tciaData != null) {
                supportingData.add(Map.of("data_category", "TCIA", "data_object", tciaData));
            }
            study.put("supporting_data", supportingData);
        }
        return study;
    }

    private List<Map<String, Object>> studiesListing(Map<String, Object> params) throws IOException {
        final String[][] PROPERTIES = new String[][]{
            // Demographics
            new String[]{"id", "id"},
            new String[]{"study_id", "study_id"},
            new String[]{"study_name", "study_name"},
            new String[]{"num_of_participants", "num_of_participants"},
            new String[]{"num_of_samples", "num_of_samples"},
            new String[]{"num_of_diagnoses", "num_of_diagnoses"},
            new String[]{"sex_at_birth", "sex_at_birth"},
            new String[]{"num_of_files", "num_of_files"},
            new String[]{"num_of_study_files", "num_of_study_files"},
            new String[]{"num_of_participant_files", "num_of_participant_files"},
            new String[]{"num_of_sample_files", "num_of_sample_files"},
            new String[]{"num_of_publications", "num_of_publications"},
        };

        String defaultSort = "dbgap_accession"; // Default sort order

        Map<String, String> mapping = Map.ofEntries(
            Map.entry("study_id", "study_id"),
            Map.entry("study_name", "study_name"),
            Map.entry("num_of_participants", "num_of_participants"),
            Map.entry("num_of_samples", "num_of_samples"),
            Map.entry("num_of_diagnoses", "num_of_diagnoses"),
            Map.entry("num_of_files", "num_of_files"),
            Map.entry("num_of_study_files", "num_of_study_files"),
            Map.entry("num_of_participant_files", "num_of_participant_files"),
            Map.entry("num_of_sample_files", "num_of_sample_files"),
            Map.entry("num_of_publications", "num_of_publications")
        );

        return overview(STUDIES_END_POINT, params, PROPERTIES, defaultSort, mapping, REGULAR_PARAMS, "nested_filters", "studies");
    }

    private List<Map<String, Object>> cohortManifest(Map<String, Object> params) throws IOException {
        List<Map<String, Object>> participants;
        final String[][] PROPERTIES = new String[][]{
            // Demographics
            new String[]{"id", "id"},
            new String[]{"participant_id", "participant_id"},
            new String[]{"dbgap_accession", "dbgap_accession"},
            new String[]{"race", "race"},
            new String[]{"sex_at_birth", "sex_at_birth"},
            new String[]{"diagnosis", "diagnosis_str"},
        };

        String defaultSort = "participant_id"; // Default sort order

        Map<String, String> mapping = Map.ofEntries(
            Map.entry("participant_id", "participant_id"),
            Map.entry("dbgap_accession", "dbgap_accession"),
            Map.entry("race", "race"),
            Map.entry("sex_at_birth", "sex_at_birth"),
            Map.entry("diagnosis", "diagnosis_str")
        );

        return overview(COHORTS_END_POINT, params, PROPERTIES, defaultSort, mapping, REGULAR_PARAMS, "nested_filters", "cohorts");
    }

    private List<Map<String, Object>> cohortMetadata(Map<String, Object> params) throws IOException {
        List<Map<String, Object>> participants;
        Map<String, List<Map<String, Object>>> participantsByStudy = new HashMap<String, List<Map<String, Object>>>();
        List<Map<String, Object>> listOfParticipantsByStudy = new ArrayList<Map<String, Object>>();

        final String[][] PROPERTIES = new String[][]{
            
            new String[]{"id", "id"},
            new String[]{"participant_id", "participant_id"},
            new String[]{"dbgap_accession", "dbgap_accession"},
            new String[]{"race", "race"},
            new String[]{"sex_at_birth", "sex_at_birth"},
            
            new String[]{"diagnoses", "diagnoses"},
            new String[]{"survivals", "survivals"},
            new String[]{"treatments", "treatments"},
            new String[]{"treatment_responses", "treatment_responses"},
            new String[]{"samples", "samples"},
            new String[]{"files", "files"},
        };

        String defaultSort = "participant_id"; // Default sort order

        Map<String, String> mapping = Map.ofEntries(
            Map.entry("participant_id", "participant_id"),
            Map.entry("dbgap_accession", "dbgap_accession"),
            Map.entry("race", "race"),
            Map.entry("sex_at_birth", "sex_at_birth")
        );

        participants = overview(COHORTS_END_POINT, params, PROPERTIES, defaultSort, mapping, REGULAR_PARAMS, "nested_filters", "cohorts");
        
        // Sort survivals array by age_at_last_known_survival_status for each participant
        participants.forEach((Map<String, Object> participant) -> {
            Object survivalsObj = participant.get("survivals");
            if (survivalsObj instanceof List) {
                @SuppressWarnings("unchecked")
                List<Map<String, Object>> survivals = (List<Map<String, Object>>) survivalsObj;
                survivals.sort((a, b) -> {
                    Object aAgeObj = a.get("age_at_last_known_survival_status");
                    Object bAgeObj = b.get("age_at_last_known_survival_status");
                    
                    // Handle null values - put them at the end
                    if (aAgeObj == null && bAgeObj == null) return 0;
                    if (aAgeObj == null) return 1;
                    if (bAgeObj == null) return -1;
                    
                    // Convert to double for comparison
                    double aAge = aAgeObj instanceof Number ? ((Number) aAgeObj).doubleValue() : Double.parseDouble(aAgeObj.toString());
                    double bAge = bAgeObj instanceof Number ? ((Number) bAgeObj).doubleValue() : Double.parseDouble(bAgeObj.toString());
                    
                    return Double.compare(aAge, bAge);
                });
            }
        });
        
        // Restructure the data to a map, keyed by dbgap_accession
        participants.forEach((Map<String, Object> participant) -> {
            String dbgapAccession = (String) participant.get("dbgap_accession");

            if (participantsByStudy.containsKey(dbgapAccession)) {
                participantsByStudy.get(dbgapAccession).add(participant);
            } else {
                participantsByStudy.put(dbgapAccession, new ArrayList<Map<String, Object>>(
                    List.of(participant)
                ));
            }
        });

        // Restructure the map to a list
        participantsByStudy.forEach((accession, people) -> {
            listOfParticipantsByStudy.add(Map.ofEntries(
                Map.entry("dbgap_accession", accession),
                Map.entry("participants", people)
            ));
        });
        return listOfParticipantsByStudy;
    }

    /**
     * Generates chart data for cohort comparison
     * @param params Contains c1, c2, c3 (cohort participant IDs) and charts (properties to chart)
     * @return List of chart results, one per property
     * @throws IOException
     */
    private List<Map<String, Object>> cohortCharts(Map<String, Object> params) throws IOException {
        List<Map<String, Object>> chartConfigs = null;
        List<Map<String, Object>> charts = new ArrayList<Map<String, Object>>();
        Map<String, Object> cohorts = new HashMap<String, Object>();
        List<String> cohortsCombined = new ArrayList<String>();
        List<Map<String, Object>> result = new ArrayList<Map<String, Object>>();

        if (params == null || !params.containsKey("charts")) {
            return List.of(); // No charts specified
        }

        if (!(params.containsKey("c1") || params.containsKey("c2") || params.containsKey("c3"))) {
            return List.of(); // No cohorts specified
        }

        // Combine cohorts from c1, c2, c3 into a single list
        for (String key : List.of("c1", "c2", "c3")) {
            if (!params.containsKey(key)) {
                continue;
            }

            Object cohortRaw = params.get(key);
            if (cohortRaw instanceof List) {
                @SuppressWarnings("unchecked")
                List<String> cohort = (List<String>) cohortRaw;

                if (!cohort.isEmpty()) {
                    // Add cohort to combined list
                    cohortsCombined.addAll(cohort);
                    cohorts.put(key, cohort);
                }
            }
        }

        if (cohortsCombined.isEmpty()) {
            return result;
        }

        Object chartConfigsRaw = params.get("charts");
        if (chartConfigsRaw instanceof List) {
            @SuppressWarnings("unchecked")
            List<Map<String, Object>> castedChartConfigs = (List<Map<String, Object>>) chartConfigsRaw;
            chartConfigs = castedChartConfigs;
        }

        if (chartConfigs == null || chartConfigs.isEmpty()) {
            return result;
        }

        // Generate charts for each configuration
        for (Map<String, Object> chartConfig : chartConfigs) {
            // Prepare map that represents the entire chart
            String property = (String) chartConfig.get("property");
            String type = (String) chartConfig.get("type");
            Map<String, Object> chartData = new HashMap<String, Object>();
            chartData.put("property", property);
            int totalNumberOfParticipants = 0;
            List<String> bucketNames;
            List<String> bucketNamesTopFew;
            List<String> bucketNamesTopMany;

            // Obtain details for querying Opensearch
            Map<String, String> propertyConfig = getPropertyConfig(property);
            if (propertyConfig == null) {
                logger.warn("Skipping unknown property: " + property);
                continue;
            }

            String cardinalityAggName = propertyConfig.get("cardinalityAggName");
            //if cardinalityAggName is "", set it to null
            if (cardinalityAggName.equals("")) {
                cardinalityAggName = null;
            }
            String endpoint = propertyConfig.get("endpoint");
            String indexName = propertyConfig.get("index");

            // Determine most populous buckets
            Map<String, Object> combinedCohortParams = Map.of("id", cohortsCombined);  // Changed from participant_pk to id
            bucketNames = inventoryESService.getBucketNames(property, combinedCohortParams, RANGE_PARAMS, cardinalityAggName, indexName, endpoint);

            if (bucketNames.size() > COHORT_CHART_BUCKET_LIMIT_LOW) {
                bucketNamesTopFew = new ArrayList<>(bucketNames.subList(0, COHORT_CHART_BUCKET_LIMIT_LOW));
            } else {
                bucketNamesTopFew = new ArrayList<>(bucketNames);
            }

            if (bucketNames.size() > COHORT_CHART_BUCKET_LIMIT_HIGH) {
                bucketNamesTopMany = new ArrayList<>(bucketNames.subList(0, COHORT_CHART_BUCKET_LIMIT_HIGH));
            } else {
                bucketNamesTopMany = new ArrayList<>(bucketNames);
            }

            // If chart type is percentage, then count the total number of participants
            if ("percentage".equals(type)) {
                Map<String, Object> combinedCohortsQuery = inventoryESService.buildFacetFilterQuery(combinedCohortParams, RANGE_PARAMS, Set.of(), Set.of(), "", "participants");
                totalNumberOfParticipants = inventoryESService.getCount(combinedCohortsQuery, "participants");
            }

            // Prepare list of data for each cohort
            List<Map<String, Object>> cohortsData = new ArrayList<Map<String, Object>>();

            // Retrieve data for each cohort
            for (String cohortName : cohorts.keySet()) {
                // Prepare map of data for the cohort
                Map<String, Object> cohortData = new HashMap<String, Object>();
                Map<String, Object> cohortParams = Map.of("id", cohorts.get(cohortName));  // Changed from participant_pk to id
                cohortData.put("cohort", cohortName);

                // Retrieve data for the cohort
                List<Map<String, Object>> cohortGroupCounts = filterSubjectCountBy(property, cohortParams, endpoint, cardinalityAggName, indexName);
                List<Map<String, Object>> cohortGroupCountsTruncated = new ArrayList<Map<String, Object>>();
                int otherMany = 0;
                int otherFew = 0;

                // Format for efficient retrieval
                Map<String, Object> groupsToSubjects = new HashMap<>();
                for (Map<String, Object> groupCount : cohortGroupCounts) {
                    String group = (String) groupCount.get("group");
                    Object subjects = groupCount.get("subjects");
                    groupsToSubjects.put(group, subjects);
                }

                // Add buckets and their counts to a truncated list of results
                for (String bucketName : bucketNames) {
                    Integer subjects = (Integer) groupsToSubjects.getOrDefault(bucketName, 0);

                    if (bucketNamesTopMany.contains(bucketName)) {
                        cohortGroupCountsTruncated.add(Map.of("group", bucketName, "subjects", (double) subjects));
                    } else {
                        otherMany += subjects;
                    }

                    if (!bucketNamesTopFew.contains(bucketName)) {
                        otherFew += subjects;
                    }
                }

                if (bucketNames.size() > COHORT_CHART_BUCKET_LIMIT_LOW) {
                    cohortGroupCountsTruncated.add(Map.of("group", "OtherFew", "subjects", (double) otherFew));
                }

                if (bucketNames.size() > COHORT_CHART_BUCKET_LIMIT_HIGH) {
                    cohortGroupCountsTruncated.add(Map.of("group", "OtherMany", "subjects", (double) otherMany));
                }

                // If chart type is percentage, then replace counts with percentages
                if ("percentage".equals(type)) {
                    List<Map<String, Object>> cohortGroupPercentages = new ArrayList<Map<String, Object>>();

                    for (Map<String, Object> groupCount : cohortGroupCountsTruncated) {
                        String group = (String) groupCount.get("group");
                        double count = (Double) groupCount.get("subjects");
                        double percentage = totalNumberOfParticipants > 0 ? (count / totalNumberOfParticipants) * 100 : 0.0;

                        cohortGroupPercentages.add(Map.of("group", group, "subjects", percentage));
                    }

                    cohortData.put("participantsByGroup", cohortGroupPercentages);
                } else if ("count".equals(type)) {
                    cohortData.put("participantsByGroup", cohortGroupCountsTruncated);
                }

                // Add cohort data to the list of cohorts
                cohortsData.add(cohortData);
            }

            // Add list of all cohorts' data to the chart
            chartData.put("cohorts", cohortsData);

            // Add chart to the list of charts
            charts.add(chartData);
        }

        return charts;
    }

    private List<Map<String, Object>> diagnosisOverview(Map<String, Object> params) throws IOException {
        final String[][] PROPERTIES = new String[][]{
            new String[]{"d_id", "id"},
            new String[]{"pid", "pid"},
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
            new String[]{"diagnosis_category", "diagnosis_category"},
            new String[]{"age_at_diagnosis", "age_at_diagnosis"},
            new String[]{"tumor_grade_source", "tumor_grade_source"},
            new String[]{"tumor_stage_source", "tumor_stage_source"},
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
                Map.entry("tumor_stage_source", "tumor_stage_source")
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
            new String[]{"study_status", "study_status"},
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
                Map.entry("study_status", "study_status"),
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
            new String[]{"diagnosis", "diagnosis_str"},
            new String[]{"diagnosis_category", "diagnosis_category_str"},
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
                Map.entry("tumor_classification", "tumor_classification"),
                Map.entry("diagnosis", "diagnosis_str"),
                Map.entry("diagnosis_category", "diagnosis_category_str")
        );

        return overview(SAMPLES_END_POINT, params, PROPERTIES, defaultSort, mapping, REGULAR_PARAMS, "nested_filters", "samples");
    }

    private List<Map<String, Object>> fileOverview(Map<String, Object> params) throws IOException {
        final String[][] PROPERTIES = new String[][]{
                new String[]{"id", "id"},
            new String[]{"file_id", "file_id"},
            new String[]{"guid", "guid"},
            new String[]{"file_name", "file_name"},
            new String[]{"data_category", "data_category"},
            new String[]{"file_description", "file_description"},
            new String[]{"file_type", "file_type"},
            new String[]{"file_size", "file_size"},
            new String[]{"library_selection", "library_selection"},
            new String[]{"library_source_material", "library_source_material"},
            new String[]{"library_source_molecule", "library_source_molecule"},
            new String[]{"library_strategy", "library_strategy"},
            new String[]{"file_mapping_level", "file_mapping_level"},
            new String[]{"file_access", "file_access"},
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
                Map.entry("data_category", "data_category"),
                Map.entry("file_description", "file_description"),
                Map.entry("file_type", "file_type"),
                Map.entry("file_size", "file_size"),
                Map.entry("study_id", "study_id"),
                Map.entry("library_selection", "library_selection.sort"),
                Map.entry("library_source_material", "library_source_material.sort"),
                Map.entry("library_source_molecule", "library_source_molecule.sort"),
                Map.entry("library_strategy", "library_strategy.sort"),
                Map.entry("file_mapping_level", "file_mapping_level"),
                Map.entry("file_access", "file_access"),
                Map.entry("participant_id", "participant_id"),
                Map.entry("sample_id", "sample_id"),
                Map.entry("md5sum", "md5sum")
        );

        return overview(FILES_END_POINT, params, PROPERTIES, defaultSort, mapping, REGULAR_PARAMS, "nested_filters", "files");
    }

    private Map<String, Object> getFilenames(Map<String, Object> params) throws IOException {
        try {
            final String[][] PROPERTIES = new String[][]{
                    new String[]{"id", "id"},
                new String[]{"file_id", "file_id"},
                new String[]{"guid", "guid"},
                new String[]{"file_name", "file_name"},
                new String[]{"data_category", "data_category"},
                new String[]{"file_description", "file_description"},
                new String[]{"file_type", "file_type"},
                new String[]{"file_size", "file_size"},
                new String[]{"library_selection", "library_selection"},
                new String[]{"library_source_material", "library_source_material"},
                new String[]{"library_source_molecule", "library_source_molecule"},
                new String[]{"library_strategy", "library_strategy"},
                new String[]{"file_mapping_level", "file_mapping_level"},
                new String[]{"file_access", "file_access"},
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
                Map.entry("data_category", "data_category"),
                Map.entry("file_description", "file_description"),
                Map.entry("file_type", "file_type"),
                Map.entry("file_size", "file_size"),
                Map.entry("study_id", "study_id"),
                Map.entry("library_selection", "library_selection.sort"),
                Map.entry("library_source_material", "library_source_material.sort"),
                Map.entry("library_source_molecule", "library_source_molecule.sort"),
                Map.entry("library_strategy", "library_strategy.sort"),
                Map.entry("file_mapping_level", "file_mapping_level"),
                Map.entry("file_access", "file_access"),
                Map.entry("participant_id", "participant_id"),
                Map.entry("sample_id", "sample_id"),
                Map.entry("md5sum", "md5sum")
            );

            String filename = (String) params.get("filename");
            String order_by = (String) params.get(ORDER_BY);
            if (order_by == null) {
                order_by = "file_id";
            }
            String sortDirectionParam = (String) params.get(SORT_DIRECTION);
            String direction = (sortDirectionParam != null) ? sortDirectionParam.toLowerCase() : "asc";
            Object pageSizeObj = params.get(PAGE_SIZE);
            Object offsetObj = params.get(OFFSET);
            int pageSize = (pageSizeObj != null) ? (int) pageSizeObj : 10;
            int offset = (offsetObj != null) ? (int) offsetObj : 0;

            // Build query with facet filters (same as fileOverview)
            // Exclude "filename" since it's a String, not a List, and we handle it separately with wildcard
            Map<String, Object> query = inventoryESService.buildFacetFilterQuery(params, RANGE_PARAMS, Set.of(PAGE_SIZE, OFFSET, ORDER_BY, SORT_DIRECTION, "filename"), REGULAR_PARAMS, "nested_filters", "files");
            // Create mutable copy since buildFacetFilterQuery may return immutable collections
            query = new HashMap<>(query);
            if (filename != null && !filename.isEmpty()) {
                // Navigate to the appropriate location to add wildcard based on query structure
                // When there ARE facet filters: {"query": {"bool": {"should": [{"bool": {"must": {...}, "filter": [...]}}]}}}
                // When there are NO facet filters: {"query": {"bool": {"must": {"exists": {"field": "file_id"}}}}}
                
                try {
                    Map<String, Object> queryMap = (Map<String, Object>) query.get("query");
                    Map<String, Object> boolQuery = (Map<String, Object>) queryMap.get("bool");
                    
                    // Create multi-field wildcard search
                    // Search across: file_name, data_category, file_description, file_type, file_access,
                    // study_id, participant_id, sample_id, guid, md5sum, library_selection, 
                    // library_source_material, library_strategy, library_source_molecule, file_mapping_level
                    List<String> searchFields = List.of(
                        "file_name", "data_category", "file_description", "file_type", "file_access",
                        "study_id", "participant_id", "sample_id", "guid", "md5sum", 
                        "library_selection", "library_source_material", "library_strategy", 
                        "library_source_molecule", "file_mapping_level"
                    );
                    
                    List<Map<String, Object>> shouldClauses = new ArrayList<>();
                    for (String field : searchFields) {
                        Map<String, Object> wildcardParams = new HashMap<>();
                        wildcardParams.put("value", "*" + filename + "*");
                        wildcardParams.put("case_insensitive", true);
                        
                        Map<String, Object> fieldClause = new HashMap<>();
                        fieldClause.put(field, wildcardParams);
                        
                        Map<String, Object> wildcardClause = new HashMap<>();
                        wildcardClause.put("wildcard", fieldClause);
                        
                        shouldClauses.add(wildcardClause);
                    }
                    
                    // Wrap the should clauses in a bool query (matches if ANY field contains the search text)
                    Map<String, Object> multiFieldSearch = new HashMap<>();
                    multiFieldSearch.put("bool", Map.of("should", shouldClauses, "minimum_should_match", 1));
                    
                    // Check if query has "should" structure (with facet filters)
                    if (boolQuery.containsKey("should")) {
                        List<Object> shouldList = (List<Object>) boolQuery.get("should");
                        if (shouldList != null && !shouldList.isEmpty()) {
                            // Get the first (and only) element in should array
                            Map<String, Object> shouldBool = (Map<String, Object>) shouldList.get(0);
                            Map<String, Object> innerBool = (Map<String, Object>) shouldBool.get("bool");
                            List<Object> filterList = (List<Object>) innerBool.get("filter");
                            
                            if (filterList != null) {
                                // Create mutable copy of filter list and add multi-field search
                                List<Object> mutableFilterList = new ArrayList<>(filterList);
                                mutableFilterList.add(0, multiFieldSearch);
                                
                                // Rebuild the query structure
                                Map<String, Object> mutableInnerBool = new HashMap<>(innerBool);
                                mutableInnerBool.put("filter", mutableFilterList);
                                
                                Map<String, Object> mutableShouldBool = new HashMap<>(shouldBool);
                                mutableShouldBool.put("bool", mutableInnerBool);
                                
                                List<Object> mutableShouldList = new ArrayList<>();
                                mutableShouldList.add(mutableShouldBool);
                                
                                Map<String, Object> mutableBoolQuery = new HashMap<>(boolQuery);
                                mutableBoolQuery.put("should", mutableShouldList);
                                
                                Map<String, Object> mutableQueryMap = new HashMap<>(queryMap);
                                mutableQueryMap.put("bool", mutableBoolQuery);
                                
                                query.put("query", mutableQueryMap);
                            }
                        }
                    } else {
                        // No facet filters - simpler structure with just "must"
                        // Add multi-field search to must clause
                        Object mustObj = boolQuery.get("must");
                        List<Object> mustList = new ArrayList<>();
                        
                        if (mustObj != null) {
                            if (mustObj instanceof List) {
                                mustList.addAll((List<Object>) mustObj);
                            } else {
                                mustList.add(mustObj);
                            }
                        }
                        
                        mustList.add(multiFieldSearch);
                        
                        Map<String, Object> mutableBoolQuery = new HashMap<>(boolQuery);
                        mutableBoolQuery.put("must", mustList);
                        
                        Map<String, Object> mutableQueryMap = new HashMap<>(queryMap);
                        mutableQueryMap.put("bool", mutableBoolQuery);
                        
                        query.put("query", mutableQueryMap);
                    }
                } catch (Exception e) {
                    logger.error("Error adding wildcard search: " + e.getMessage(), e);
                }
            }

            query.put("sort", mapSortOrder(order_by, direction, defaultSort, mapping));
            query.put("_source", Map.of("includes", Set.of("id","file_id","guid","file_name","data_category","file_description","file_type","file_size","library_selection","library_source_material","library_source_molecule","library_strategy","file_mapping_level","file_access","study_id","participant_id","sample_id","md5sum","files")));

            Request request = new Request("GET", FILES_END_POINT);
            
            // Get total count with the same query but without pagination
            Map<String, Object> countQuery = new HashMap<>(query);
            countQuery.remove("size");
            countQuery.remove("from");
            countQuery.put("size", 0);
            countQuery.put("track_total_hits", true);
            
            Request countRequest = new Request("GET", FILES_END_POINT);
            countRequest.setJsonEntity(gson.toJson(countQuery));
            JsonObject countResult = inventoryESService.send(countRequest);
            
            int totalCount = 0;
            if (countResult != null && countResult.has("hits")) {
                JsonObject hits = countResult.getAsJsonObject("hits");
                if (hits.has("total")) {
                    JsonObject total = hits.getAsJsonObject("total");
                    if (total.has("value")) {
                        totalCount = total.get("value").getAsInt();
                    }
                }
            }
            System.out.println(gson.toJson(query));
            
            // Get paginated results
            List<Map<String, Object>> page = inventoryESService.collectPage(request, query, PROPERTIES, pageSize, offset);
        
            // Return FilenamesResult structure
            Map<String, Object> result = new HashMap<>();
            result.put("files", page);
            result.put("totalCount", totalCount);
            
            return result;
        } catch (Exception e) {
            logger.error("Error in getFilenames: " + e.getMessage(), e);
            throw new IOException("Error in getFilenames: " + e.getMessage(), e);
        }
    }

    // if the nestedProperty is set, this will filter based upon the params against the nested property for the endpoint's index.
    // otherwise, this will filter based upon the params against the top level properties for the index
    private List<Map<String, Object>> overview(String endpoint, Map<String, Object> params, String[][] properties, String defaultSort, Map<String, String> mapping, Set<String> regular_fields, String nestedProperty, String overviewType) throws IOException {
        
        Request request = new Request("GET", endpoint);
        Map<String, Object> query = inventoryESService.buildFacetFilterQuery(params, RANGE_PARAMS, Set.of(PAGE_SIZE, OFFSET, ORDER_BY, SORT_DIRECTION), regular_fields, nestedProperty, overviewType);
        // System.out.println(query);
        String order_by = (String)params.get(ORDER_BY);
        String direction = ((String)params.get(SORT_DIRECTION)).toLowerCase();
        query.put("sort", mapSortOrder(order_by, direction, defaultSort, mapping));
        // "_source": {"exclude": [ "sample_diagnosis_file_filters"]}
        if (overviewType.equals("participants")) {
            query.put("_source", Map.of("exclude", Set.of("sample_diagnosis_file_filters", "survival_filters", "treatment_filters", "treatment_response_filters")));
        }
        if (overviewType.equals("diagnosis")) {
            query.put("_source", Map.of("exclude", Set.of("sample_file_filters", "survival_filters", "treatment_filters", "treatment_response_filters")));
        }
        if (overviewType.equals("samples")) {
            query.put("_source", Map.of("exclude", Set.of("diagnosis_filters", "file_filters", "survival_filters", "treatment_filters", "treatment_response_filters")));
        }
        if (overviewType.equals("files")) {
            query.put("_source", Map.of("includes", Set.of("id","file_id","guid","file_name","data_category","file_description","file_type","file_size","library_selection","library_source_material","library_source_molecule","library_strategy","file_mapping_level","file_access","study_id","participant_id","sample_id","md5sum","files")));
            //query.put("_source", Map.of("exclude", Set.of("combined_filters", "participant_filters", "sample_diagnosis_filters", "survival_filters", "treatment_filters", "treatment_response_filters")));
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

    private Integer numberOfStudies(Map<String, Object> params) throws IOException {
        Map<String, Object> query_files_all_records = inventoryESService.buildFacetFilterQuery(params, RANGE_PARAMS, Set.of(), REGULAR_PARAMS, "nested_filters", "files_overall");
        int numStudies = getNodeCount("study_id", query_files_all_records, FILES_END_POINT).size();
        return numStudies;
    }

    private List<Map<String, Object>> filesManifestInList(Map<String, Object> params) throws IOException {
        final String[][] properties = new String[][]{
                new String[]{"guid", "guid"},
                new String[]{"file_name", "file_name"},
                new String[]{"participant_id", "participant_id"},
                new String[]{"md5sum", "md5sum"}
        };
        Map<String, Object> file_ids = new HashMap<>();
        file_ids.put("id", params.get("id"));

        int pageSize = (int) params.get(PAGE_SIZE);
        int offset = (int) params.get(OFFSET);
        Map<String, Object> query = esService.buildListQuery(file_ids, Set.of(), false);
        query.put("_source", Map.of("includes", Set.of("guid", "file_name", "participant_id", "md5sum")));
        Request request = new Request("GET", FILES_END_POINT);

        return esService.collectPage(request, query, properties, pageSize, offset);
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

    /**
     * Executes batch OpenSearch query for all study/participant combinations
     */
    private List<Map<String, Object>> executeBatchQuery(Map<String, Set<String>> studyToParticipantsMap) throws IOException {
        if (studyToParticipantsMap.isEmpty()) {
            return new ArrayList<>();
        }

        // Build the batch query
        Map<String, Object> query = buildBatchQuery(studyToParticipantsMap);
        
        // System.out.println("Executing batch query: " + gson.toJson(query));

        // Execute the query
        Request request = new Request("GET", PARTICIPANTS_END_POINT);
        request.setJsonEntity(gson.toJson(query));
        
        JsonObject response = inventoryESService.send(request);
        JsonArray hits = response.getAsJsonObject("hits").getAsJsonArray("hits");
        
        List<Map<String, Object>> results = new ArrayList<>();
        for (JsonElement hit : hits) {
            JsonObject source = hit.getAsJsonObject().getAsJsonObject("_source");
            Map<String, Object> result = new HashMap<>();
            
            if (source.has("id")) {
                result.put("id", source.get("id").getAsString());
            }
            if (source.has("participant_id")) {
                result.put("participant_id", source.get("participant_id").getAsString());
            }
            if (source.has("study_id")) {
                result.put("study_id", source.get("study_id").getAsString());
            }
            
            results.add(result);
        }

        return results;
    }

    /**
     * Builds the batch OpenSearch query based on the study-to-participants mapping
     */
    private Map<String, Object> buildBatchQuery(Map<String, Set<String>> studyToParticipantsMap) {
        List<Object> shouldClauses = new ArrayList<>();

        for (Map.Entry<String, Set<String>> entry : studyToParticipantsMap.entrySet()) {
            String studyId = entry.getKey();
            Set<String> participantIds = entry.getValue();

            Map<String, Object> boolFilter = Map.of(
                "bool", Map.of(
                    "filter", List.of(
                        Map.of("term", Map.of("study_id", studyId)),
                        Map.of("terms", Map.of("participant_id", new ArrayList<>(participantIds)))
                    )
                )
            );

            shouldClauses.add(boolFilter);
        }

        Map<String, Object> query = Map.of(
            "query", Map.of(
                "bool", Map.of(
                    "should", shouldClauses
                )
            ),
            "size", 10000, // Adjust size as needed
            "_source", List.of("id", "participant_id", "study_id")
        );

        return query;
    }

    /**
     * Enriches CPI data with the results from the batch query
     */
    private void enrichCpiDataWithBatchResults(List<FormattedCPIResponse> recordsWithCpiData, List<Map<String, Object>> batchQueryResults) {
        // Create lookup map for quick access to query results
        Map<String, String> participantStudyToPidMap = new HashMap<>();
        
        for (Map<String, Object> result : batchQueryResults) {
            String participantId = (String) result.get("participant_id");
            String studyId = (String) result.get("study_id");
            String pId = (String) result.get("id");
            
            if (participantId != null && studyId != null && pId != null) {
                String key = participantId + "_" + studyId;
                participantStudyToPidMap.put(key, pId);
            }
        }

        // System.out.println("Created lookup map with " + participantStudyToPidMap.size() + " participant/study combinations");

        // Enrich each CPI data record
        for (FormattedCPIResponse cpiEntry : recordsWithCpiData) {
            enrichSingleCpiEntry(cpiEntry, participantStudyToPidMap);
        }
    }


    /**
     * Enriches a single CPI entry with p_id and data_type
     */
    private void enrichSingleCpiEntry(FormattedCPIResponse cpiEntry, Map<String, String> participantStudyToPidMap) {
        try {
            java.lang.reflect.Field cpiDataField = cpiEntry.getClass().getDeclaredField("cpiData");
            cpiDataField.setAccessible(true);
            Object cpiDataValue = cpiDataField.get(cpiEntry);

            if (cpiDataValue instanceof List) {
                @SuppressWarnings("unchecked")
                List<Object> cpiDataArray = (List<Object>) cpiDataValue;

                for (int i = 0; i < cpiDataArray.size(); i++) {
                    Object cpiDataItem = cpiDataArray.get(i);
                    Map<String, Object> cpiDataMap = convertToMap(cpiDataItem);
                    
                    if (cpiDataMap != null) {
                        String participantId = extractStringValue(cpiDataMap, "associated_id");
                        String studyId = extractStringValue(cpiDataMap, "repository_of_synonym_id");
                        
                        if (participantId != null && studyId != null) {
                            String lookupKey = participantId + "_" + studyId;
                            
                            if (participantStudyToPidMap.containsKey(lookupKey)) {
                                // Found match in OpenSearch - set internal data
                                cpiDataMap.put("p_id", participantStudyToPidMap.get(lookupKey));
                                cpiDataMap.put("data_type", "internal");
                                // System.out.println("Enriched CPI data: participant=" + participantId + ", study=" + studyId + ", p_id=" + participantStudyToPidMap.get(lookupKey) + ", data_type=internal");
                            } else {
                                // No match found - set external data
                                cpiDataMap.put("p_id", null);
                                cpiDataMap.put("data_type", "external");
                                // System.out.println("Enriched CPI data: participant=" + participantId + ", study=" + studyId + ", p_id=null, data_type=external");
                            }
                            
                            // If we converted to a new Map, replace the original item
                            if (!(cpiDataItem instanceof Map)) {
                                cpiDataArray.set(i, cpiDataMap);
                            }
                        }
                    }
                }
                
                // Update the cpiData field with enriched array
                cpiDataField.set(cpiEntry, cpiDataArray);
            }
        } catch (Exception e) {
            logger.error("Error enriching single CPI entry: " + e.getMessage(), e);
        }
    }

    /**
     * Updates the participant_list with enriched CPI data by matching participant_id and study_id
     */
    private void updateParticipantListWithEnrichedCPIData(List<Map<String, Object>> participant_list, List<FormattedCPIResponse> enriched_cpi_data) {
        if (participant_list == null || participant_list.isEmpty() || enriched_cpi_data == null || enriched_cpi_data.isEmpty()) {
            return;
        }

        // Create a map for quick lookup of enriched CPI data by participant_id + study_id combination
        Map<String, Object> enrichedCPILookup = new HashMap<>();
        
        for (FormattedCPIResponse cpiResponse : enriched_cpi_data) {
            try {
                // Extract participant_id and study_id from the CPI response
                Object participantIdObj = getFieldValue(cpiResponse, "participantId");
                Object studyIdObj = getFieldValue(cpiResponse, "studyId");
                
                String participantId = participantIdObj != null ? participantIdObj.toString() : null;
                String studyId = studyIdObj != null ? studyIdObj.toString() : null;
                
                if (participantId != null && studyId != null) {
                    String lookupKey = participantId + "_" + studyId;
                    
                    // Extract the enriched cpiData array from the response (this is the array with enriched objects)
                    Object enrichedCpiDataArray = getFieldValue(cpiResponse, "cpiData");
                    if (enrichedCpiDataArray != null) {
                        enrichedCPILookup.put(lookupKey, enrichedCpiDataArray);
                        // System.out.println("Added enriched CPI data array for participant: " + participantId + ", study: " + studyId);
                    }
                }
            } catch (Exception e) {
                logger.error("Error processing CPI response for lookup map: " + e.getMessage(), e);
            }
        }

        // Update each participant in the participant_list with enriched CPI data
        for (Map<String, Object> participant : participant_list) {
            try {
                String participantId = getStringValue(participant, "participant_id");
                String studyId = getStringValue(participant, "study_id");
                
                if (participantId != null && studyId != null) {
                    String lookupKey = participantId + "_" + studyId;
                    
                    // Check if we have enriched CPI data for this participant
                    if (enrichedCPILookup.containsKey(lookupKey)) {
                        Object enrichedCpiDataArray = enrichedCPILookup.get(lookupKey);
                        
                        // Update the cpi_data field in the participant record with the enriched array
                        participant.put("cpi_data", enrichedCpiDataArray);
                        // System.out.println("Updated participant " + participantId + " with enriched CPI data array");
                    } else {
                        // System.out.println("No enriched CPI data found for participant: " + participantId + ", study: " + studyId);
                    }
                }
            } catch (Exception e) {
                logger.error("Error updating participant with enriched CPI data: " + e.getMessage(), e);
            }
        }
        
        // System.out.println("Completed updating participant_list with enriched CPI data");
    }

    /**
     * Helper method to extract field values from FormattedCPIResponse objects using reflection
     */
    private Object getFieldValue(FormattedCPIResponse obj, String fieldName) {
        try {
            java.lang.reflect.Field field = obj.getClass().getDeclaredField(fieldName);
            field.setAccessible(true);
            Object value = field.get(obj);
            return value;
        } catch (Exception e) {
            logger.debug("Could not access field '" + fieldName + "' from FormattedCPIResponse: " + e.getMessage());
            return null;
        }
    }

    /**
     * Helper method to safely extract string values from maps
     */
    private String getStringValue(Map<String, Object> map, String key) {
        Object value = map.get(key);
        return value != null ? value.toString() : null;
    }

    /**
     * Maps property names to their OpenSearch index and aggregation configuration
     * This replaces the YAML configuration approach used in C3DC
     * @param propertyName The property to get configuration for
     * @return Map containing index, endpoint, and cardinalityAggName, or null if not found
     */
    private Map<String, String> getPropertyConfig(String propertyName) {
        // Demographics/Participant properties
        if ("race".equals(propertyName) || "sex_at_birth".equals(propertyName)) {
            return Map.of(
                "index", "participants",
                "endpoint", PARTICIPANTS_END_POINT,
                "cardinalityAggName", ""  // No cardinality needed for participant-level fields
            );
        }
        
        // Treatment properties
        if ("treatment_type".equals(propertyName) || "treatment_agent".equals(propertyName)) {
            return Map.of(
                "index", "treatments",
                "endpoint", TREATMENT_END_POINT,
                "cardinalityAggName", "pid"  // Count unique participants
            );
        }
        
        // Treatment Response properties
        if ("response".equals(propertyName) || "response_category".equals(propertyName)) {
            return Map.of(
                "index", "treatment_responses",
                "endpoint", TREATMENT_RESPONSE_END_POINT,
                "cardinalityAggName", "pid"  // Count unique participants
            );
        }
        
        // Diagnosis properties
        if ("diagnosis".equals(propertyName) || "diagnosis_anatomic_site".equals(propertyName) || 
            "disease_phase".equals(propertyName) || "diagnosis_classification_system".equals(propertyName)) {
            return Map.of(
                "index", "diagnosis",
                "endpoint", DIAGNOSIS_END_POINT,
                "cardinalityAggName", "pid"  // Count unique participants
            );
        }
        
        // Survival properties
        if ("last_known_survival_status".equals(propertyName) || "first_event".equals(propertyName)) {
            return Map.of(
                "index", "survivals",
                "endpoint", SURVIVALS_END_POINT,
                "cardinalityAggName", "pid"  // Count unique participants
            );
        }
        
        // Sample properties
        if ("sample_anatomic_site".equals(propertyName) || "sample_tumor_status".equals(propertyName) || 
            "tumor_classification".equals(propertyName)) {
            return Map.of(
                "index", "samples",
                "endpoint", SAMPLES_END_POINT,
                "cardinalityAggName", "pid"  // Count unique participants
            );
        }
        
        // Study properties
        if ("dbgap_accession".equals(propertyName) || "study_name".equals(propertyName) || 
            "study_acronym".equals(propertyName)) {
            return Map.of(
                "index", "studies",
                "endpoint", STUDIES_END_POINT,
                "cardinalityAggName", ""  // No cardinality needed
            );
        }
        
        // Property not found
        logger.warn("No configuration found for property: " + propertyName);
        return null;
    }
}
