package com.streamweaver.repository;

import com.streamweaver.entity.MessageMetadata;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.stereotype.Repository;

import java.time.Instant;
import java.util.List;

@Repository
public interface MessageMetadataRepository extends JpaRepository<MessageMetadata, Long> {
    
    List<MessageMetadata> findByTopic(String topic);
    
    List<MessageMetadata> findByServiceName(String serviceName);
    
    List<MessageMetadata> findByLogLevel(String logLevel);
    
    List<MessageMetadata> findByCreatedAtBetween(Instant start, Instant end);
    
    @Query("SELECT COUNT(m) FROM MessageMetadata m WHERE m.topic = ?1")
    Long countByTopic(String topic);
    
    @Query("SELECT AVG(m.processingTimeMs) FROM MessageMetadata m WHERE m.topic = ?1")
    Double averageProcessingTime(String topic);
}
