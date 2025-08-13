package gov.nih.nci.bento_ri.model;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.List;

/**
 * Data model for formatted CPI response
 */
public class FormattedCPIResponse {
    @JsonProperty("participant_id")
    private String participantId;
    
    @JsonProperty("study_id")
    private String studyId;
    
    @JsonProperty("cpi_data")
    private List<CPIDataItem> cpiData;

    public FormattedCPIResponse() {}

    public FormattedCPIResponse(String participantId, String studyId, List<CPIDataItem> cpiData) {
        this.participantId = participantId;
        this.studyId = studyId;
        this.cpiData = cpiData;
    }

    public String getParticipantId() {
        return participantId;
    }

    public void setParticipantId(String participantId) {
        this.participantId = participantId;
    }

    public String getStudyId() {
        return studyId;
    }

    public void setStudyId(String studyId) {
        this.studyId = studyId;
    }

    public List<CPIDataItem> getCpiData() {
        return cpiData;
    }

    public void setCpiData(List<CPIDataItem> cpiData) {
        this.cpiData = cpiData;
    }

    /**
     * Nested class for CPI data items
     */
    public static class CPIDataItem {
        @JsonProperty("associated_id")
        private String associatedId;
        
        @JsonProperty("repository_of_synonym_id")
        private String repositoryOfSynonymId;
        
        @JsonProperty("domain_description")
        private String domainDescription;
        
        @JsonProperty("domain_category")
        private String domainCategory;
        
        @JsonProperty("data_location")
        private String dataLocation;

        public CPIDataItem() {}

        public CPIDataItem(String associatedId, String repositoryOfSynonymId, String domainDescription, 
                          String domainCategory, String dataLocation) {
            this.associatedId = associatedId;
            this.repositoryOfSynonymId = repositoryOfSynonymId;
            this.domainDescription = domainDescription;
            this.domainCategory = domainCategory;
            this.dataLocation = dataLocation;
        }

        public String getAssociatedId() {
            return associatedId;
        }

        public void setAssociatedId(String associatedId) {
            this.associatedId = associatedId;
        }

        public String getRepositoryOfSynonymId() {
            return repositoryOfSynonymId;
        }

        public void setRepositoryOfSynonymId(String repositoryOfSynonymId) {
            this.repositoryOfSynonymId = repositoryOfSynonymId;
        }

        public String getDomainDescription() {
            return domainDescription;
        }

        public void setDomainDescription(String domainDescription) {
            this.domainDescription = domainDescription;
        }

        public String getDomainCategory() {
            return domainCategory;
        }

        public void setDomainCategory(String domainCategory) {
            this.domainCategory = domainCategory;
        }

        public String getDataLocation() {
            return dataLocation;
        }

        public void setDataLocation(String dataLocation) {
            this.dataLocation = dataLocation;
        }
    }
}
