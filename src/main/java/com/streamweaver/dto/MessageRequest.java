package com.streamweaver.dto;

import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@Schema(description = "Request to produce a message to Kafka")
public class MessageRequest {
    
    @NotBlank(message = "Message content cannot be empty")
    @Schema(description = "JSON message content", example = "{\"service\":\"api-gateway\",\"level\":\"INFO\",\"message\":\"Request processed\"}")
    private String content;
    
    @Schema(description = "Optional message key for Kafka partitioning")
    private String key;
}
