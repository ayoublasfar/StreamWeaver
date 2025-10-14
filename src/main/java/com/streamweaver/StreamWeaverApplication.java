package com.streamweaver;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.streamweaver.entity.MessageMetadata;
import com.streamweaver.entity.SchemaVersion;
import com.streamweaver.repository.MessageMetadataRepository;
import com.streamweaver.repository.SchemaVersionRepository;
import com.streamweaver.service.SchemaRegistryService;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.config.KafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;
import org.springframework.stereotype.Service;
import org.springframework.web.bind.annotation.*;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.annotation.Value;

import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@SpringBootApplication
@EnableKafka
@Slf4j
public class StreamWeaverApplication {

    public static void main(String[] args) {
        SpringApplication.run(StreamWeaverApplication.class, args);
        log.info("🚀 StreamWeaver Application Started Successfully!");
        log.info("📊 PostgreSQL Integration: ACTIVE");
        log.info("🔧 Schema Registry Integration: ACTIVE");
    }
}

// ============== Kafka Configuration ==============
@org.springframework.context.annotation.Configuration
class KafkaConfig {

    @Value("${spring.kafka.bootstrap-servers}")
    private String bootstrapServers;

    @Bean
    public ProducerFactory<String, String> producerFactory() {
        Map<String, Object> config = new HashMap<>();
        config.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        return new DefaultKafkaProducerFactory<>(config);
    }

    @Bean
    public KafkaTemplate<String, String> kafkaTemplate() {
        return new KafkaTemplate<>(producerFactory());
    }

    @Bean
    public ConsumerFactory<String, String> consumerFactory() {
        Map<String, Object> config = new HashMap<>();
        config.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        config.put(ConsumerConfig.GROUP_ID_CONFIG, "streamweaver-group");
        config.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        config.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        return new DefaultKafkaConsumerFactory<>(config);
    }

    @Bean
    public KafkaListenerContainerFactory<ConcurrentMessageListenerContainer<String, String>> 
           kafkaListenerContainerFactory() {
        ConcurrentKafkaListenerContainerFactory<String, String> factory = 
            new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory());
        return factory;
    }
}

// ============== Kafka Consumer Service ==============
@Service
@Slf4j
class KafkaConsumerService {

    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;
    
    @Autowired
    private MessageMetadataRepository messageMetadataRepository;
    
    @Autowired
    private SchemaRegistryService schemaRegistryService;
    
    private final ObjectMapper objectMapper = new ObjectMapper();

    @KafkaListener(topics = "raw-data", groupId = "streamweaver-group")
    public void consumeRawData(ConsumerRecord<String, String> record) {
        long startTime = System.currentTimeMillis();
        
        try {
            log.info("📨 Received message: key={}, partition={}, offset={}", 
                     record.key(), record.partition(), record.offset());
            log.info("📝 Message content: {}", record.value());

            // Extract metadata from message
            String serviceName = extractServiceName(record.value());
            String logLevel = extractLogLevel(record.value());
            
            // Infer and check schema
            String currentSchema = schemaRegistryService.inferSchema(record.value());
            String subject = serviceName != null ? serviceName + "-schema" : "default-schema";
            
            boolean schemaDriftDetected = schemaRegistryService.detectSchemaDrift(subject, currentSchema);
            
            // Register new schema version if drift detected
            SchemaVersion schemaVersion = null;
            if (schemaDriftDetected) {
                schemaVersion = schemaRegistryService.registerSchema(subject, currentSchema, "ayoublasfar");
            }
            
            // Normalize data
            String normalized = normalizeData(record.value());
            
            // Calculate processing time
            long processingTime = System.currentTimeMillis() - startTime;
            
            // Save to PostgreSQL
            MessageMetadata metadata = MessageMetadata.builder()
                .messageKey(record.key())
                .topic(record.topic())
                .partition(record.partition())
                .offset(record.offset())
                .rawMessage(record.value())
                .normalizedMessage(normalized)
                .serviceName(serviceName)
                .logLevel(logLevel)
                .schemaVersion(schemaVersion != null ? schemaVersion.getVersion().toString() : "1")
                .schemaId(schemaVersion != null ? schemaVersion.getSchemaId() : null)
                .processingTimeMs(processingTime)
                .processedAt(Instant.now())
                .createdBy("ayoublasfar")
                .build();
            
            MessageMetadata saved = messageMetadataRepository.save(metadata);
            log.info("💾 Saved to PostgreSQL with ID: {}", saved.getId());
            
            // Send to normalized topic
            kafkaTemplate.send("normalized-data", normalized);
            log.info("✅ Normalized and forwarded message ({}ms)", processingTime);
            
        } catch (Exception e) {
            log.error("❌ Error processing message: {}", e.getMessage(), e);
        }
    }

    private String normalizeData(String rawData) {
        return String.format("{\"data\":%s,\"normalized_at\":\"%s\",\"version\":\"1.0\"}", 
                           rawData, Instant.now());
    }
    
    private String extractServiceName(String message) {
        try {
            JsonNode node = objectMapper.readTree(message);
            if (node.has("service")) return node.get("service").asText();
            if (node.has("service_name")) return node.get("service_name").asText();
            if (node.has("application")) return node.get("application").asText();
        } catch (Exception e) {
            // Ignore
        }
        return "unknown";
    }
    
    private String extractLogLevel(String message) {
        try {
            JsonNode node = objectMapper.readTree(message);
            if (node.has("level")) return node.get("level").asText();
            if (node.has("log_level")) return node.get("log_level").asText();
            if (node.has("severity")) return node.get("severity").asText();
        } catch (Exception e) {
            // Ignore
        }
        return "INFO";
    }
}

// ============== REST Controller ==============
@RestController
@Slf4j
class StreamWeaverController {

    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;
    
    @Autowired
    private MessageMetadataRepository messageMetadataRepository;
    
    @Autowired
    private SchemaVersionRepository schemaVersionRepository;
    
    @Autowired
    private SchemaRegistryService schemaRegistryService;

    @GetMapping("/health")
    public Map<String, Object> health() {
        Map<String, Object> response = new HashMap<>();
        response.put("status", "UP");
        response.put("application", "StreamWeaver");
        response.put("timestamp", Instant.now().toString());
        response.put("features", Map.of(
            "postgresql", "ACTIVE",
            "schema_registry", "ACTIVE",
            "kafka", "ACTIVE"
        ));
        return response;
    }

    @PostMapping("/produce")
    public Map<String, String> produceMessage(@RequestBody String message) {
        try {
            kafkaTemplate.send("raw-data", message);
            log.info("📤 Message sent to raw-data topic: {}", message);
            
            Map<String, String> response = new HashMap<>();
            response.put("status", "success");
            response.put("message", "Data sent to Kafka");
            response.put("topic", "raw-data");
            return response;
            
        } catch (Exception e) {
            log.error("❌ Error sending message: {}", e.getMessage(), e);
            Map<String, String> response = new HashMap<>();
            response.put("status", "error");
            response.put("message", e.getMessage());
            return response;
        }
    }
    
    // ============== PostgreSQL Endpoints ==============
    
    @GetMapping("/api/messages")
    public List<MessageMetadata> getAllMessages() {
        return messageMetadataRepository.findAll();
    }
    
    @GetMapping("/api/messages/topic/{topic}")
    public List<MessageMetadata> getMessagesByTopic(@PathVariable String topic) {
        return messageMetadataRepository.findByTopic(topic);
    }
    
    @GetMapping("/api/messages/service/{service}")
    public List<MessageMetadata> getMessagesByService(@PathVariable String service) {
        return messageMetadataRepository.findByServiceName(service);
    }
    
    @GetMapping("/api/messages/level/{level}")
    public List<MessageMetadata> getMessagesByLevel(@PathVariable String level) {
        return messageMetadataRepository.findByLogLevel(level);
    }
    
    @GetMapping("/api/stats/topic/{topic}")
    public Map<String, Object> getTopicStats(@PathVariable String topic) {
        Map<String, Object> stats = new HashMap<>();
        stats.put("topic", topic);
        stats.put("total_messages", messageMetadataRepository.countByTopic(topic));
        stats.put("avg_processing_time_ms", messageMetadataRepository.averageProcessingTime(topic));
        return stats;
    }
    
    // ============== Schema Registry Endpoints ==============
    
    @GetMapping("/api/schemas")
    public List<SchemaVersion> getAllSchemas() {
        return schemaVersionRepository.findAll();
    }
    
    @GetMapping("/api/schemas/subject/{subject}")
    public List<SchemaVersion> getSchemasBySubject(@PathVariable String subject) {
        return schemaVersionRepository.findBySubject(subject);
    }
    
    @GetMapping("/api/schemas/active")
    public List<SchemaVersion> getActiveSchemas() {
        return schemaVersionRepository.findByIsActive(true);
    }
    
    @GetMapping("/api/schemas/registry/subjects")
    public List<String> getRegistrySubjects() {
        return schemaRegistryService.getAllSubjects();
    }
}
