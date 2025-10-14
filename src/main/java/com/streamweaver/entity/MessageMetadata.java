package com.streamweaver.entity;

import jakarta.persistence.*;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.Instant;

@Entity
@Table(name = "message_metadata", indexes = {
    @Index(name = "idx_topic", columnList = "topic"),
    @Index(name = "idx_created_at", columnList = "created_at"),
    @Index(name = "idx_service_name", columnList = "service_name")
})
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class MessageMetadata {
    
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;
    
    @Column(name = "message_key")
    private String messageKey;
    
    @Column(nullable = false)
    private String topic;
    
    @Column(name = "partition_number")
    private Integer partition;
    
    @Column(name = "offset_value")
    private Long offset;
    
    @Column(name = "raw_message", columnDefinition = "TEXT")
    private String rawMessage;
    
    @Column(name = "normalized_message", columnDefinition = "TEXT")
    private String normalizedMessage;
    
    @Column(name = "service_name")
    private String serviceName;
    
    @Column(name = "log_level")
    private String logLevel;
    
    @Column(name = "schema_version")
    private String schemaVersion;
    
    @Column(name = "schema_id")
    private Integer schemaId;
    
    @Column(name = "processing_time_ms")
    private Long processingTimeMs;
    
    @Column(name = "created_at", nullable = false)
    private Instant createdAt;
    
    @Column(name = "processed_at")
    private Instant processedAt;
    
    @Column(name = "created_by")
    private String createdBy;
    
    @PrePersist
    protected void onCreate() {
        createdAt = Instant.now();
    }
}
