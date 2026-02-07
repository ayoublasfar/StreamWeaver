package com.streamweaver.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.streamweaver.entity.SchemaVersion;
import com.streamweaver.repository.SchemaVersionRepository;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class SchemaRegistryServiceTest {

    @Mock
    private SchemaVersionRepository schemaVersionRepository;

    @InjectMocks
    private SchemaRegistryService schemaRegistryService;

    private ObjectMapper objectMapper;

    @BeforeEach
    void setUp() {
        objectMapper = new ObjectMapper();
    }

    @Test
    void testInferSchema_SimpleObject() throws JsonProcessingException {
        // Given
        String jsonMessage = "{\"name\":\"John\",\"age\":30,\"active\":true}";
        
        // When
        String schema = schemaRegistryService.inferSchema(jsonMessage);
        
        // Then
        assertNotNull(schema);
        assertFalse(schema.isEmpty());
        assertTrue(schema.contains("name"));
        assertTrue(schema.contains("age"));
        assertTrue(schema.contains("active"));
    }

    @Test
    void testInferSchema_NestedObject() throws JsonProcessingException {
        // Given
        String jsonMessage = "{\"user\":{\"id\":1,\"name\":\"John\"}}";
        
        // When
        String schema = schemaRegistryService.inferSchema(jsonMessage);
        
        // Then
        assertNotNull(schema);
        assertTrue(schema.contains("user"));
        assertTrue(schema.contains("object"));
    }

    @Test
    void testInferSchema_WithArray() {
        // Given
        String jsonMessage = "{\"tags\":[\"java\",\"spring\",\"kafka\"]}";
        
        // When
        String schema = schemaRegistryService.inferSchema(jsonMessage);
        
        // Then
        assertNotNull(schema);
        assertTrue(schema.contains("tags"));
        assertTrue(schema.contains("array"));
    }

    @Test
    void testInferSchema_InvalidJson() {
        // Given
        String invalidJson = "{invalid json}";
        
        // When
        String schema = schemaRegistryService.inferSchema(invalidJson);
        
        // Then
        assertEquals("{}", schema);
    }

    @Test
    void testDetectSchemaDrift_NoExistingSchema() {
        // Given
        String subject = "new-subject";
        String currentSchema = "{\"name\":\"string\"}";
        when(schemaVersionRepository.findBySubject(subject)).thenReturn(Collections.emptyList());
        
        // When
        boolean driftDetected = schemaRegistryService.detectSchemaDrift(subject, currentSchema);
        
        // Then
        assertFalse(driftDetected);
        verify(schemaVersionRepository, times(1)).findBySubject(subject);
    }

    @Test
    void testDetectSchemaDrift_SameSchema() {
        // Given
        String subject = "test-subject";
        String currentSchema = "{\"name\":\"string\"}";
        
        SchemaVersion existingSchema = SchemaVersion.builder()
            .subject(subject)
            .version(1)
            .schemaDefinition(currentSchema)
            .build();
        
        when(schemaVersionRepository.findBySubject(subject)).thenReturn(Arrays.asList(existingSchema));
        
        // When
        boolean driftDetected = schemaRegistryService.detectSchemaDrift(subject, currentSchema);
        
        // Then
        assertFalse(driftDetected);
    }

    @Test
    void testDetectSchemaDrift_DifferentSchema() {
        // Given
        String subject = "test-subject";
        String oldSchema = "{\"name\":\"string\"}";
        String newSchema = "{\"name\":\"string\",\"age\":\"integer\"}";
        
        SchemaVersion existingSchema = SchemaVersion.builder()
            .subject(subject)
            .version(1)
            .schemaDefinition(oldSchema)
            .build();
        
        when(schemaVersionRepository.findBySubject(subject)).thenReturn(Arrays.asList(existingSchema));
        
        // When
        boolean driftDetected = schemaRegistryService.detectSchemaDrift(subject, newSchema);
        
        // Then
        assertTrue(driftDetected);
    }

    @Test
    void testRegisterSchema_NewSubject() {
        // Given
        String subject = "new-subject";
        String schemaDefinition = "{\"name\":\"string\"}";
        String registeredBy = "test-user";
        
        when(schemaVersionRepository.findBySubject(subject)).thenReturn(Collections.emptyList());
        when(schemaVersionRepository.save(any(SchemaVersion.class))).thenAnswer(invocation -> {
            SchemaVersion saved = invocation.getArgument(0);
            saved.setId(1L);
            return saved;
        });
        
        // When
        SchemaVersion result = schemaRegistryService.registerSchema(subject, schemaDefinition, registeredBy);
        
        // Then
        assertNotNull(result);
        assertEquals(subject, result.getSubject());
        assertEquals(1, result.getVersion());
        assertEquals(schemaDefinition, result.getSchemaDefinition());
        assertEquals(registeredBy, result.getRegisteredBy());
        verify(schemaVersionRepository, times(1)).save(any(SchemaVersion.class));
    }

    @Test
    void testRegisterSchema_ExistingSubject() {
        // Given
        String subject = "existing-subject";
        String schemaDefinition = "{\"name\":\"string\",\"age\":\"integer\"}";
        String registeredBy = "test-user";
        
        SchemaVersion existingSchema = SchemaVersion.builder()
            .subject(subject)
            .version(1)
            .schemaDefinition("{\"name\":\"string\"}")
            .build();
        
        when(schemaVersionRepository.findBySubject(subject)).thenReturn(Arrays.asList(existingSchema));
        when(schemaVersionRepository.save(any(SchemaVersion.class))).thenAnswer(invocation -> {
            SchemaVersion saved = invocation.getArgument(0);
            saved.setId(2L);
            return saved;
        });
        
        // When
        SchemaVersion result = schemaRegistryService.registerSchema(subject, schemaDefinition, registeredBy);
        
        // Then
        assertNotNull(result);
        assertEquals(subject, result.getSubject());
        assertEquals(2, result.getVersion()); // Version should increment
        assertEquals(schemaDefinition, result.getSchemaDefinition());
        verify(schemaVersionRepository, times(1)).save(any(SchemaVersion.class));
    }
}
