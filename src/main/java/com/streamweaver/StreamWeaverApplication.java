package com.streamweaver;

import io.swagger.v3.oas.annotations.OpenAPIDefinition;
import io.swagger.v3.oas.annotations.info.Info;
import io.swagger.v3.oas.annotations.info.Contact;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.streamweaver.dto.ApiResponse;
import com.streamweaver.dto.MessageRequest;
import com.streamweaver.entity.MessageMetadata;
import com.streamweaver.entity.SchemaVersion;
import com.streamweaver.repository.MessageMetadataRepository;
import com.streamweaver.repository.SchemaVersionRepository;
import com.streamweaver.service.SchemaRegistryService;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
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
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.*;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.serialization.StringDeserializer;

import jakarta.validation.Valid;
import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@SpringBootApplication
@EnableKafka
@Slf4j
@OpenAPIDefinition(
    info = @Info(
        title = "StreamWeaver API",
        version = "1.0.0",
        description = "A Unified Real-Time Data Fabric for Intelligent Stream Integration",
        contact = @Contact(name = "StreamWeaver Team", url = "https://github.com/ayoublasfar/StreamWeaver")
    )
)
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
    
    @Value("${app.default-user}")
    private String defaultUser;
    
    private final ObjectMapper objectMapper = new ObjectMapper();

    @KafkaListener(topics = "raw-data", groupId = "streamweaver-group")
    public void consumeRawData(ConsumerRecord<String, String> record) {
        long startTime = System.currentTimeMillis();
        
        try {
            log.info("📨 Received message: key={}, partition={}, offset={}", 
                     record.key(), record.partition(), record.offset());
            log.debug("Message content: {}", record.value());

            // Validate JSON format
            if (!isValidJson(record.value())) {
                log.error("❌ Invalid JSON format in message");
                return;
            }

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
                schemaVersion = schemaRegistryService.registerSchema(subject, currentSchema, defaultUser);
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
                .createdBy(defaultUser)
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
    
    private boolean isValidJson(String json) {
        try {
            objectMapper.readTree(json);
            return true;
        } catch (Exception e) {
            return false;
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
            log.debug("Could not extract service name: {}", e.getMessage());
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
            log.debug("Could not extract log level: {}", e.getMessage());
        }
        return "INFO";
    }
}

// ============== REST Controller ==============
@RestController
@Slf4j
@Validated
@Tag(name = "StreamWeaver API", description = "Real-time data streaming and schema management endpoints")
class StreamWeaverController {

    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;
    
    @Autowired
    private MessageMetadataRepository messageMetadataRepository;
    
    @Autowired
    private SchemaVersionRepository schemaVersionRepository;
    
    @Autowired
    private SchemaRegistryService schemaRegistryService;
    
    private final ObjectMapper objectMapper = new ObjectMapper();

    @GetMapping("/health")
    @Operation(summary = "Health check", description = "Check the health status of the application")
    public ApiResponse<Map<String, Object>> health() {
        Map<String, Object> features = Map.of(
            "postgresql", "ACTIVE",
            "schema_registry", "ACTIVE",
            "kafka", "ACTIVE"
        );
        
        Map<String, Object> data = new HashMap<>();
        data.put("application", "StreamWeaver");
        data.put("features", features);
        
        return ApiResponse.success("Application is healthy", data);
    }

    @PostMapping("/produce")
    @Operation(summary = "Produce message", description = "Send a message to the Kafka raw-data topic")
    public ApiResponse<Map<String, String>> produceMessage(@Valid @RequestBody MessageRequest request) {
        try {
            // Validate JSON format
            try {
                objectMapper.readTree(request.getContent());
            } catch (Exception e) {
                throw new IllegalArgumentException("Invalid JSON format in message content");
            }
            
            String key = request.getKey() != null ? request.getKey() : "default-key";
            kafkaTemplate.send("raw-data", key, request.getContent());
            log.info("📤 Message sent to raw-data topic with key: {}", key);
            
            Map<String, String> data = new HashMap<>();
            data.put("topic", "raw-data");
            data.put("key", key);
            
            return ApiResponse.success("Data sent to Kafka successfully", data);
            
        } catch (IllegalArgumentException e) {
            throw e;
        } catch (Exception e) {
            log.error("❌ Error sending message: {}", e.getMessage(), e);
            throw new RuntimeException("Failed to send message to Kafka: " + e.getMessage());
        }
    }
    
    // ============== PostgreSQL Endpoints ==============
    
    @GetMapping("/api/messages")
    @Operation(summary = "Get all messages", description = "Retrieve all processed messages from the database")
    public ApiResponse<List<MessageMetadata>> getAllMessages() {
        List<MessageMetadata> messages = messageMetadataRepository.findAll();
        return ApiResponse.success("Retrieved all messages", messages);
    }
    
    @GetMapping("/api/messages/topic/{topic}")
    @Operation(summary = "Get messages by topic", description = "Retrieve messages filtered by Kafka topic")
    public ApiResponse<List<MessageMetadata>> getMessagesByTopic(@PathVariable String topic) {
        List<MessageMetadata> messages = messageMetadataRepository.findByTopic(topic);
        return ApiResponse.success("Retrieved messages for topic: " + topic, messages);
    }
    
    @GetMapping("/api/messages/service/{service}")
    @Operation(summary = "Get messages by service", description = "Retrieve messages filtered by service name")
    public ApiResponse<List<MessageMetadata>> getMessagesByService(@PathVariable String service) {
        List<MessageMetadata> messages = messageMetadataRepository.findByServiceName(service);
        return ApiResponse.success("Retrieved messages for service: " + service, messages);
    }
    
    @GetMapping("/api/messages/level/{level}")
    @Operation(summary = "Get messages by log level", description = "Retrieve messages filtered by log level")
    public ApiResponse<List<MessageMetadata>> getMessagesByLevel(@PathVariable String level) {
        List<MessageMetadata> messages = messageMetadataRepository.findByLogLevel(level);
        return ApiResponse.success("Retrieved messages for log level: " + level, messages);
    }
    
    @GetMapping("/api/stats/topic/{topic}")
    @Operation(summary = "Get topic statistics", description = "Retrieve aggregated statistics for a specific topic")
    public ApiResponse<Map<String, Object>> getTopicStats(@PathVariable String topic) {
        Map<String, Object> stats = new HashMap<>();
        stats.put("topic", topic);
        stats.put("total_messages", messageMetadataRepository.countByTopic(topic));
        stats.put("avg_processing_time_ms", messageMetadataRepository.averageProcessingTime(topic));
        
        return ApiResponse.success("Retrieved statistics for topic: " + topic, stats);
    }
    
    // ============== Schema Registry Endpoints ==============
    
    @GetMapping("/api/schemas")
    @Operation(summary = "Get all schemas", description = "Retrieve all registered schema versions")
    public ApiResponse<List<SchemaVersion>> getAllSchemas() {
        List<SchemaVersion> schemas = schemaVersionRepository.findAll();
        return ApiResponse.success("Retrieved all schemas", schemas);
    }
    
    @GetMapping("/api/schemas/subject/{subject}")
    @Operation(summary = "Get schemas by subject", description = "Retrieve all schema versions for a specific subject")
    public ApiResponse<List<SchemaVersion>> getSchemasBySubject(@PathVariable String subject) {
        List<SchemaVersion> schemas = schemaVersionRepository.findBySubject(subject);
        return ApiResponse.success("Retrieved schemas for subject: " + subject, schemas);
    }
    
    @GetMapping("/api/schemas/active")
    @Operation(summary = "Get active schemas", description = "Retrieve all currently active schema versions")
    public ApiResponse<List<SchemaVersion>> getActiveSchemas() {
        List<SchemaVersion> schemas = schemaVersionRepository.findByIsActive(true);
        return ApiResponse.success("Retrieved active schemas", schemas);
    }
    
    @GetMapping("/api/schemas/registry/subjects")
    @Operation(summary = "Get registry subjects", description = "Retrieve all subjects from the Schema Registry")
    public ApiResponse<List<String>> getRegistrySubjects() {
        List<String> subjects = schemaRegistryService.getAllSubjects();
        return ApiResponse.success("Retrieved subjects from Schema Registry", subjects);
    }
}
