package gov.nih.nci.bento_ri.controller;

import gov.nih.nci.bento_ri.model.FormattedCPIResponse;
import gov.nih.nci.bento_ri.model.ParticipantRequest;
import gov.nih.nci.bento_ri.service.CPIFetcherService;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * REST Controller for CPI (Child Participant Index) API operations
 */
@RestController
@RequestMapping("/api/v1/cpi")
@CrossOrigin(origins = "*")
public class CPIController {
    
    private static final Logger logger = LogManager.getLogger(CPIController.class);
    
    @Autowired
    private CPIFetcherService cpiFetcherService;
    
    /**
     * Fetch associated participant IDs from CPI service
     * 
     * @param participantRequests List of participant requests
     * @return Associated participant IDs response
     */
    @PostMapping("/associated-participant-ids")
    public ResponseEntity<List<FormattedCPIResponse>> getAssociatedParticipantIds(
            @RequestBody List<ParticipantRequest> participantRequests) {
        
        logger.info("Received request to fetch associated participant IDs for {} participants", 
                   participantRequests.size());
        
        try {
            // Validate input
            if (participantRequests == null || participantRequests.isEmpty()) {
                return ResponseEntity.badRequest().build();
            }
            
            // Validate each participant request
            for (ParticipantRequest request : participantRequests) {
                if (request.getParticipantId() == null || request.getParticipantId().trim().isEmpty()) {
                    return ResponseEntity.badRequest().build();
                }
                if (request.getStudyId() == null || request.getStudyId().trim().isEmpty()) {
                    return ResponseEntity.badRequest().build();
                }
            }
            
            // Call the service
            List<FormattedCPIResponse> response = cpiFetcherService.fetchAssociatedParticipantIds(participantRequests);
            
            logger.info("Successfully processed CPI request");
            return ResponseEntity.ok(response);
            
        } catch (IllegalStateException e) {
            logger.error("Configuration error: {}", e.getMessage());
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).build();
            
        } catch (Exception e) {
            logger.error("Error processing CPI request: {}", e.getMessage(), e);
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).build();
        }
    }
    
    /**
     * Health check endpoint for CPI service
     */
    @GetMapping("/health")
    public ResponseEntity<Map<String, Object>> healthCheck() {
        Map<String, Object> response = new HashMap<>();
        response.put("status", "UP");
        response.put("service", "CPI Fetcher Service");
        response.put("timestamp", System.currentTimeMillis());
        return ResponseEntity.ok(response);
    }
}
