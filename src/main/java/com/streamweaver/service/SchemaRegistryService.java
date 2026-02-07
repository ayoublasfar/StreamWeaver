package com.streamweaver.service;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.streamweaver.entity.SchemaVersion;
import com.streamweaver.repository.SchemaVersionRepository;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestTemplate;

import java.util.*;
import java.util.stream.Collectors;

@Service
@Slf4j
public class SchemaRegistryService {
    
    @Value("${schema.registry.url}")
    private String schemaRegistryUrl;
    
    @Autowired
    private SchemaVersionRepository schemaVersionRepository;
    
    private final RestTemplate restTemplate = new RestTemplate();
    private final ObjectMapper objectMapper = new ObjectMapper();
    
    /**
     * Infer JSON schema from message with support for nested objects
     */
    public String inferSchema(String jsonMessage) {
        try {
            JsonNode node = objectMapper.readTree(jsonMessage);
            Map<String, Object> schema = new LinkedHashMap<>();
            
            node.fields().forEachRemaining(entry -> {
                String fieldName = entry.getKey();
                JsonNode fieldValue = entry.getValue();
                schema.put(fieldName, inferTypeRecursive(fieldValue));
            });
            
            return objectMapper.writeValueAsString(schema);
            
        } catch (Exception e) {
            log.error("Error inferring schema: {}", e.getMessage());
            return "{}";
        }
    }
    
    /**
     * Recursively infer type for nested structures
     */
    private Object inferTypeRecursive(JsonNode node) {
        if (node.isInt()) return "integer";
        if (node.isLong()) return "long";
        if (node.isDouble() || node.isFloat()) return "double";
        if (node.isBoolean()) return "boolean";
        if (node.isNull()) return "null";
        if (node.isArray()) {
            if (node.isEmpty()) return Map.of("type", "array", "items", "unknown");
            // Infer type from first element
            return Map.of("type", "array", "items", inferTypeRecursive(node.get(0)));
        }
        if (node.isObject()) {
            Map<String, Object> objectSchema = new LinkedHashMap<>();
            node.fields().forEachRemaining(entry -> {
                objectSchema.put(entry.getKey(), inferTypeRecursive(entry.getValue()));
            });
            return Map.of("type", "object", "properties", objectSchema);
        }
        return "string";
    }
    
    /**
     * Detect schema drift by comparing with previous version
     */
    public boolean detectSchemaDrift(String subject, String currentSchema) {
        try {
            List<SchemaVersion> versions = schemaVersionRepository.findBySubject(subject);
            
            if (versions.isEmpty()) {
                log.info("📝 New subject detected: {}", subject);
                return false;
            }
            
            SchemaVersion latest = versions.stream()
                .max(Comparator.comparing(SchemaVersion::getVersion))
                .orElse(null);
            
            if (latest != null && !latest.getSchemaDefinition().equals(currentSchema)) {
                log.warn("⚠️ Schema drift detected for subject: {}", subject);
                log.warn("Previous: {}", latest.getSchemaDefinition());
                log.warn("Current: {}", currentSchema);
                return true;
            }
            
            return false;
            
        } catch (Exception e) {
            log.error("Error detecting schema drift: {}", e.getMessage());
            return false;
        }
    }
    
    /**
     * Register new schema version
     */
    public SchemaVersion registerSchema(String subject, String schemaDefinition, String registeredBy) {
        try {
            List<SchemaVersion> existing = schemaVersionRepository.findBySubject(subject);
            Integer nextVersion = existing.isEmpty() ? 1 : 
                existing.stream()
                    .map(SchemaVersion::getVersion)
                    .max(Integer::compareTo)
                    .orElse(0) + 1;
            
            SchemaVersion schemaVersion = SchemaVersion.builder()
                .subject(subject)
                .version(nextVersion)
                .schemaDefinition(schemaDefinition)
                .compatibilityMode("BACKWARD")
                .isActive(true)
                .registeredBy(registeredBy)
                .build();
            
            SchemaVersion saved = schemaVersionRepository.save(schemaVersion);
            log.info("✅ Registered schema version {} for subject: {}", nextVersion, subject);
            
            return saved;
            
        } catch (Exception e) {
            log.error("Error registering schema: {}", e.getMessage(), e);
            return null;
        }
    }
    
    /**
     * Get all schemas from Schema Registry
     */
    public List<String> getAllSubjects() {
        try {
            String url = schemaRegistryUrl + "/subjects";
            String[] subjects = restTemplate.getForObject(url, String[].class);
            return subjects != null ? Arrays.asList(subjects) : Collections.emptyList();
            
        } catch (Exception e) {
            log.error("Error fetching subjects from Schema Registry: {}", e.getMessage());
            return Collections.emptyList();
        }
    }
    
    /**
     * Get schema by subject and version from Schema Registry
     */
    public String getSchemaFromRegistry(String subject, int version) {
        try {
            String url = String.format("%s/subjects/%s/versions/%d", 
                schemaRegistryUrl, subject, version);
            String response = restTemplate.getForObject(url, String.class);
            return response;
            
        } catch (Exception e) {
            log.error("Error fetching schema from registry: {}", e.getMessage());
            return null;
        }
    }
}