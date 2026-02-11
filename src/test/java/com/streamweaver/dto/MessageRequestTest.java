package com.streamweaver.dto;

import jakarta.validation.ConstraintViolation;
import jakarta.validation.Validation;
import jakarta.validation.Validator;
import jakarta.validation.ValidatorFactory;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;

class MessageRequestTest {

    private Validator validator;

    @BeforeEach
    void setUp() {
        ValidatorFactory factory = Validation.buildDefaultValidatorFactory();
        validator = factory.getValidator();
    }

    @Test
    void testValidMessageRequest() {
        // Given
        MessageRequest request = MessageRequest.builder()
            .content("{\"service\":\"test\",\"message\":\"Hello\"}")
            .key("test-key")
            .build();

        // When
        Set<ConstraintViolation<MessageRequest>> violations = validator.validate(request);

        // Then
        assertTrue(violations.isEmpty(), "Valid message request should have no violations");
    }

    @Test
    void testValidMessageRequest_WithoutKey() {
        // Given
        MessageRequest request = MessageRequest.builder()
            .content("{\"service\":\"test\",\"message\":\"Hello\"}")
            .build();

        // When
        Set<ConstraintViolation<MessageRequest>> violations = validator.validate(request);

        // Then
        assertTrue(violations.isEmpty(), "Key is optional, so this should be valid");
    }

    @Test
    void testInvalidMessageRequest_EmptyContent() {
        // Given
        MessageRequest request = MessageRequest.builder()
            .content("")
            .build();

        // When
        Set<ConstraintViolation<MessageRequest>> violations = validator.validate(request);

        // Then
        assertFalse(violations.isEmpty(), "Empty content should violate @NotBlank");
        assertEquals(1, violations.size());
        assertTrue(violations.iterator().next().getMessage().contains("cannot be empty"));
    }

    @Test
    void testInvalidMessageRequest_NullContent() {
        // Given
        MessageRequest request = MessageRequest.builder()
            .content(null)
            .build();

        // When
        Set<ConstraintViolation<MessageRequest>> violations = validator.validate(request);

        // Then
        assertFalse(violations.isEmpty(), "Null content should violate @NotBlank");
        assertEquals(1, violations.size());
    }

    @Test
    void testInvalidMessageRequest_BlankContent() {
        // Given
        MessageRequest request = MessageRequest.builder()
            .content("   ")
            .build();

        // When
        Set<ConstraintViolation<MessageRequest>> violations = validator.validate(request);

        // Then
        assertFalse(violations.isEmpty(), "Blank content should violate @NotBlank");
        assertEquals(1, violations.size());
    }
}
