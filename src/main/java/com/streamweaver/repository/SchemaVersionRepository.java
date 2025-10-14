package com.streamweaver.repository;

import com.streamweaver.entity.SchemaVersion;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.util.List;
import java.util.Optional;

@Repository
public interface SchemaVersionRepository extends JpaRepository<SchemaVersion, Long> {
    
    List<SchemaVersion> findBySubject(String subject);
    
    Optional<SchemaVersion> findBySubjectAndVersion(String subject, Integer version);
    
    Optional<SchemaVersion> findBySchemaId(Integer schemaId);
    
    List<SchemaVersion> findByIsActive(Boolean isActive);
}