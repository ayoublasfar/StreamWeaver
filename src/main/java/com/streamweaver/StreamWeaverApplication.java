package com.streamweaver;

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
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Instant;
import java.util.HashMap;
import java.util.Map;

@SpringBootApplication
@EnableKafka
@Slf4j
public class StreamWeaverApplication {

    public static void main(String[] args) {
        SpringApplication.run(StreamWeaverApplication.class, args);
        log.info("🚀 StreamWeaver Application Started Successfully!");
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

    @KafkaListener(topics = "raw-data", groupId = "streamweaver-group")
    public void consumeRawData(ConsumerRecord<String, String> record) {
        try {
            log.info("📨 Received message: key={}, partition={}, offset={}", 
                     record.key(), record.partition(), record.offset());
            log.info("📝 Message content: {}", record.value());

            // Process and normalize
            String normalized = normalizeData(record.value());
            
            // Send to normalized topic
            kafkaTemplate.send("normalized-data", normalized);
            log.info("✅ Normalized and forwarded message");
            
        } catch (Exception e) {
            log.error("❌ Error processing message: {}", e.getMessage(), e);
        }
    }

    private String normalizeData(String rawData) {
        // Simple normalization: add timestamp and metadata
        return String.format("{\"data\":%s,\"normalized_at\":\"%s\",\"version\":\"1.0\"}", 
                           rawData, Instant.now());
    }
}

// ============== REST Controller ==============
@RestController
@Slf4j
class StreamWeaverController {

    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;

    @GetMapping("/health")
    public Map<String, String> health() {
        Map<String, String> response = new HashMap<>();
        response.put("status", "UP");
        response.put("application", "StreamWeaver");
        response.put("timestamp", Instant.now().toString());
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
}
