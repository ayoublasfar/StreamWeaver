package com.streamweaver.entity;

import jakarta.persistence.*;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.Instant;

@Entity
@Table(name = "schema_versions", indexes = {
    @Index(name = "idx_subject", columnList = "subject"),
    @Index(name = "idx_version", columnList = "version")
})
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class SchemaVersion {
    
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;
    
    @Column(nullable = false)
    private String subject;
    
    @Column(nullable = false)
    private Integer version;
    
    @Column(name = "schema_id")
    private Integer schemaId;
    
    @Column(name = "schema_definition", columnDefinition = "TEXT", nullable = false)
    private String schemaDefinition;
    
    @Column(name = "compatibility_mode")
    private String compatibilityMode;
    
    @Column(name = "is_active")
    private Boolean isActive;
    
    @Column(name = "registered_at", nullable = false)
    private Instant registeredAt;
    
    @Column(name = "registered_by")
    private String registeredBy;
    
    @PrePersist
    protected void onCreate() {
        registeredAt = Instant.now();
        if (isActive == null) {
            isActive = true;
        }
    }
}
