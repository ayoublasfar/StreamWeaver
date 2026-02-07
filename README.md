# StreamWeaver 🌐

**A Unified Real-Time Data Fabric for Intelligent Stream Integration, Schema Evolution, and Distributed Analytics**

StreamWeaver is a research and production-grade platform designed to address the challenges of heterogeneous data stream integration in distributed systems. The project implements intelligent schema normalization, drift detection, and real-time analytics capabilities to create a consistent, queryable data layer across diverse streaming sources.

---

## 📋 Overview

In distributed environments, data originates from multiple sources—APIs, IoT sensors, application logs, transaction systems, and user events. Each source typically employs different schemas, transmission frequencies, and data formats (JSON, Avro, CSV, Protobuf), creating significant integration complexity.

StreamWeaver addresses this challenge by constructing a real-time data fabric that:

- Ingests heterogeneous data streams through distributed message queues
- Normalizes structures and metadata dynamically across sources
- Detects and adapts to schema drift and field evolution automatically
- Propagates unified data to analytics systems and downstream services in real-time

The platform functions as an intelligent integration layer that transforms disparate data streams into a consistent, queryable real-time data infrastructure.

---

## 🏗️ System Architecture

### Core Components

| Component | Purpose | Technology |
|-----------|---------|------------|
| **📥 Ingestion Layer** | Consumes live data from IoT devices, APIs, and applications | Apache Kafka, Confluent Schema Registry |
| **🔄 Normalization Engine** | Converts heterogeneous formats to unified schema | Java, Spring Boot, Apache Avro |
| **🔍 Schema Drift Detector** | ML-based detection of structural changes in incoming data | Python, TensorFlow/Scikit-learn |
| **📚 Metadata Catalog** | Tracks field lineage, data sources, and temporal metadata | Neo4j, PostgreSQL |
| **⚡ Stream Processor** | Real-time aggregation, enrichment, and routing | Apache Flink / Spark Structured Streaming |
| **📊 API & Monitoring** | Query interface and operational visualization | REST API (Spring Boot), Grafana, Prometheus |

---

## 🔬 Research Applications

StreamWeaver supports investigation and implementation in:

- **Real-Time Data Integration**: Unified ingestion of multi-source streaming data
- **Schema Evolution Management**: Automated detection and reconciliation of structural changes
- **Stream Processing Optimization**: Performance analysis of distributed processing frameworks
- **Data Quality Assurance**: Real-time monitoring and validation of streaming pipelines
- **Machine Learning Operations**: Feature engineering and model serving on streaming data

---

## 💡 Use Cases

### 📈 Financial Market Data Integration
Aggregation of tick data from multiple exchanges with automatic schema mismatch detection, feeding unified streams to ML forecasting models and analytics systems.

### 🌡️ IoT Infrastructure Unification
Normalization of telemetry data from various sensor manufacturers with differing field formats and protocols into a consistent structure for centralized monitoring and analysis.

### 🏢 Enterprise Data Lake Ingestion
Harmonization of multi-department datasets with varying schemas before persistence in centralized data warehouses, ensuring consistency and quality.

### 📡 Multi-Protocol Event Streaming
Integration of events from microservices using different serialization formats (JSON, Avro, Protobuf) into a unified event stream for cross-service analytics.

---

## 🎯 Core Capabilities

### Consistency
Automatic harmonization of inconsistent data schemas across diverse sources without manual intervention.

### Reliability
Proactive detection of pipeline failures and schema drift before propagation to downstream systems.

### Observability
Real-time tracking of data lineage, quality metrics, and transformation history across the entire pipeline.

### Scalability
Dynamic handling of increasing data volumes and new stream sources through distributed processing architecture.

### Operational Impact
- Reduction in integration engineering overhead
- Minimization of analytics system downtime
- Guarantee of data consistency across enterprise systems
- Accelerated time-to-insight for business intelligence

---

## 🛠️ Technology Stack

| Layer | Technologies |
|-------|-------------|
| **Programming & API** | Java, Spring Boot, REST APIs |
| **Streaming & Messaging** | Apache Kafka, Confluent Schema Registry |
| **Stream Processing** | Apache Spark Structured Streaming, Apache Flink |
| **Metadata & Storage** | PostgreSQL, Delta Lake, Neo4j Graph Database |
| **Orchestration** | Apache Airflow |
| **ML Services** | Python, Scikit-learn, TensorFlow |
| **Monitoring** | Grafana, Prometheus |
| **Deployment** | Docker, Kubernetes, GitHub Actions CI/CD |

---

## 🧬 Technical Innovation

### 🤖 ML-Based Schema Drift Detection
Machine learning models detect and reconcile field-level schema changes automatically, adapting to structural evolution without manual configuration.

### 🔗 Real-Time Lineage Graph
Construction of visual dependency trees using graph databases (Neo4j) to track data flow and transformations across the entire fabric.

### 🎯 Adaptive Routing
Dynamic load balancing of data flows across topics and partitions based on throughput patterns and system capacity.

### 📊 Unified Query Layer
Single interface for querying both real-time streaming data and historical batch data, enabling seamless temporal analysis.

This positions StreamWeaver as more than a traditional ETL system—it functions as a self-healing, adaptive data fabric with built-in intelligence.

---

### ⚙️ Configuration

The system requires configuration of:
1. Kafka broker endpoints and topic mappings
2. Schema Registry connection details
3. Database connections (PostgreSQL, Neo4j)
4. Processing engine parameters (Flink/Spark)
5. ML model endpoints and thresholds

---

## 🔍 Key Features

- **🔄 Multi-Format Ingestion**: Native support for JSON, Avro, Protobuf, CSV
- **🧠 Intelligent Normalization**: Rule-based and ML-assisted schema mapping
- **📊 Real-Time Analytics**: Sub-second latency for stream processing
- **🔗 Lineage Tracking**: Complete data provenance from source to destination
- **⚡ Horizontal Scalability**: Distributed processing across cluster nodes
- **🛡️ Fault Tolerance**: Automatic recovery and checkpoint management

---

## 📈 Performance Characteristics

The platform is designed for:
- Throughput: Millions of events per second
- Latency: Sub-second end-to-end processing
- Availability: 99.9%+ uptime with proper deployment
- Scalability: Linear scaling with cluster size

---

## 🗺️ Development Roadmap

- [ ] Enhanced ML models for anomaly detection
- [ ] Support for additional streaming protocols (MQTT, AMQP)
- [ ] Interactive data lineage visualization UI
- [ ] Advanced query optimization for hybrid workloads
- [ ] Multi-cloud deployment templates
- [ ] Real-time data quality scoring

---

## ✨ Recent Improvements

StreamWeaver has been enhanced with several production-ready improvements:

### Code Quality & Maintainability
- ✅ Added comprehensive `.gitignore` for Maven/Java projects
- ✅ Removed hard-coded values, replaced with configuration
- ✅ Proper package organization and code structure
- ✅ Added unit tests for critical components

### API & Documentation
- ✅ Full Swagger/OpenAPI documentation at `/swagger-ui.html`
- ✅ Input validation with DTOs and JSR-303 annotations
- ✅ Consistent API responses with `ApiResponse<T>` wrapper
- ✅ Comprehensive error handling with `GlobalExceptionHandler`

### Features & Reliability
- ✅ Enhanced schema inference with nested object support
- ✅ Retry mechanism for external service calls (exponential backoff)
- ✅ Improved logging with proper log levels
- ✅ JSON validation before processing

See [IMPROVEMENTS.md](IMPROVEMENTS.md) for detailed documentation of all changes.

---

## 📖 API Documentation

After starting the application, access the interactive API documentation at:
- **Swagger UI**: http://localhost:8088/swagger-ui.html
- **OpenAPI JSON**: http://localhost:8088/api-docs

### Key Endpoints

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/health` | GET | Application health check |
| `/produce` | POST | Send message to Kafka |
| `/api/messages` | GET | Retrieve all messages |
| `/api/messages/topic/{topic}` | GET | Get messages by topic |
| `/api/schemas` | GET | Retrieve all schemas |
| `/api/stats/topic/{topic}` | GET | Get topic statistics |

---
