# AWS Glue, Kafka, PySpark & Agentic AI Lab

A comprehensive hands-on lab for building an end-to-end data pipeline that flows from raw JSON files through streaming and batch processing, ultimately feeding an Agentic AI system powered by AWS Bedrock.

![Architecture](docs/architecture.png)

## Architecture Overview

```
┌─────────────────────────────────────────────────────────────────┐
│                      DATA INGESTION LAYER                        │
│  JSON Files → ijson Parser → Kafka Producer → Kafka Topic        │
└─────────────────────────────────────────────────────────────────┘
                                │
                                ▼
┌─────────────────────────────────────────────────────────────────┐
│                      PROCESSING LAYER                            │
│  Kafka Consumer → PySpark (Batch/Streaming) → Parquet Files      │
└─────────────────────────────────────────────────────────────────┘
                                │
                                ▼
┌─────────────────────────────────────────────────────────────────┐
│                   STORAGE & ORCHESTRATION                        │
│  S3 Data Lake ←→ AWS Glue (Crawler + ETL) ←→ Athena             │
└─────────────────────────────────────────────────────────────────┘
                                │
                                ▼
┌─────────────────────────────────────────────────────────────────┐
│                      AGENTIC AI LAYER                            │
│  Bedrock Embeddings → Vector Store → Multi-Agent System          │
│                                                                   │
│  ┌─────────┐ ┌─────────┐ ┌─────────┐ ┌─────────┐               │
│  │Trans.   │ │ Trend   │ │ Report  │ │Knowledge│               │
│  │Analyst  │ │Analyzer │ │Generator│ │Specialist│              │
│  └────┬────┘ └────┬────┘ └────┬────┘ └────┬────┘               │
│       └───────────┴───────────┴───────────┘                      │
│                       │                                          │
│              ┌────────▼────────┐                                │
│              │   SUPERVISOR    │ ←→ AWS Bedrock Claude          │
│              └─────────────────┘                                │
└─────────────────────────────────────────────────────────────────┘
```

## Technologies

| Component | Technology | Purpose |
|-----------|------------|---------|
| JSON Parsing | ijson | Memory-efficient streaming parser |
| Message Queue | Apache Kafka | Real-time data streaming |
| Processing | PySpark | Distributed batch & stream processing |
| Storage | Amazon S3 + Parquet | Columnar data lake storage |
| ETL | AWS Glue | Managed ETL with Data Catalog |
| Query | Amazon Athena | Serverless SQL analytics |
| Embeddings | AWS Bedrock Titan | Vector generation for RAG |
| Vector Store | FAISS / OpenSearch | Similarity search |
| Agents | AWS Bedrock Claude | Agentic AI with tool use |

---

## Quick Start

### Prerequisites

- Python 3.9+
- Docker & Docker Compose
- AWS CLI configured with appropriate permissions
- AWS Account with Bedrock access (for Agentic AI modules)

### Installation

```bash
# Clone the repository
mkdir glue-kafka-spark-lab
cd glue-kafka-spark-lab
git clone https://github.com/Sarang68/glue-kafka-spark-lab.git .

# Create virtual environment
python3 -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate

# Install core dependencies
pip install pyspark kafka-python ijson boto3 faker pandas numpy pyarrow

# Install Agentic AI dependencies
pip install faiss-cpu

# Start Kafka (Docker required)
docker-compose up -d

# Verify Kafka is running
docker-compose ps
```

---

## Project Structure

```
glue-kafka-spark-lab/
├── docker-compose.yml          # Kafka + Zookeeper setup
├── requirements.txt            # Python dependencies
├── README.md                   # This file
│
├── # Module 1: Data Generation & ijson
├── generate_data.py            # Generate sample transaction data
├── ijson_parser.py             # Memory-efficient JSON parsing
│
├── # Module 2: Kafka Streaming
├── kafka_producer.py           # Stream JSON to Kafka
├── kafka_consumer.py           # Consume from Kafka
│
├── # Module 3: PySpark Processing
├── spark_kafka_consumer.py     # Structured Streaming from Kafka
├── spark_batch_processing.py   # Batch analytics + Parquet output
│
├── # Module 4: AWS Glue
├── glue_setup.py               # Create database, crawler, jobs
├── glue_etl_job.py             # ETL job script (runs in Glue)
├── create_glue_job.py          # Deploy and run Glue job
│
├── # Module 5: Full Pipeline
├── full_pipeline.py            # End-to-end orchestration
│
├── # Modules 6-7: Agentic AI
├── bedrock_agents/
│   ├── README.md
│   ├── requirements.txt
│   ├── bedrock_embeddings.py   # Titan embeddings generation
│   ├── vector_store.py         # FAISS + OpenSearch stores
│   ├── bedrock_agent.py        # Single agent with tool use
│   ├── multi_agent_system.py   # Supervisor + specialized agents
│   └── agentic_pipeline.py     # Complete agentic pipeline
│
├── # Outputs (generated)
├── output/                     # Local Parquet files
└── pipeline_output/            # Pipeline artifacts
```

---

## Module Guide

### Module 1: ijson — Memory-Efficient JSON Parsing

**Learn:** Stream-parse large JSON files without loading into memory.

```bash
# Generate sample data (50K transactions)
python generate_data.py

# Parse with ijson and compare memory usage
python ijson_parser.py
```

**Key Insight:** ijson uses ~2MB vs ~180MB for standard json.load() — a 90x reduction.

---

### Module 2: Kafka Producer/Consumer

**Learn:** Stream data through Kafka with partitioning and consumer groups.

```bash
# Terminal 1: Start producer (streams JSON → Kafka)
python kafka_producer.py

# Terminal 2: Start consumer (Kafka → aggregations)
python kafka_consumer.py
```

**Key Insight:** Combining ijson + Kafka enables memory-efficient ingestion of arbitrarily large files.

---

### Module 3: PySpark Processing

**Learn:** Process data with Spark SQL and Structured Streaming.

```bash
# Batch processing with analytics
python spark_batch_processing.py

# Structured Streaming from Kafka (requires Kafka running)
python spark_kafka_consumer.py
```

**Key Insight:** PySpark provides both batch and streaming with the same DataFrame API.

---

### Module 4: AWS Glue ETL

**Learn:** Deploy serverless ETL with automatic schema discovery.

```bash
# Set your S3 bucket
export GLUE_BUCKET=your-glue-lab-bucket

# Upload data to S3
aws s3 sync output/transactions_parquet/ s3://$GLUE_BUCKET/raw/transactions/

# Create Glue resources and run crawler
python glue_setup.py

# Create and run ETL job
python create_glue_job.py
```

**Key Insight:** Glue Crawlers automatically infer schema; ETL jobs scale without infrastructure management.

---

### Module 5: Full Pipeline Integration

**Learn:** Orchestrate the complete data flow from JSON to S3.

```bash
# Run complete pipeline
python full_pipeline.py
```

**Pipeline Steps:**
1. Generate transaction data
2. Stream to Kafka
3. Process with PySpark
4. Save as partitioned Parquet
5. Upload to S3 for Glue

---

### Module 6: Bedrock Embeddings & Vector Store

**Learn:** Generate embeddings and build a vector store for RAG.

```bash
cd bedrock_agents

# Generate embeddings from transaction insights
python bedrock_embeddings.py

# Build FAISS vector store
python vector_store.py
```

**Key Insight:** Embedding business context enables semantic search — ask "underperforming regions" and find relevant data even without exact keyword matches.

---

### Module 7: Agentic AI with AWS Bedrock

**Learn:** Build AI agents that reason, use tools, and take actions.

```bash
cd bedrock_agents

# Interactive single agent
python bedrock_agent.py

# Multi-agent system with supervisor
python multi_agent_system.py

# Or run the complete agentic pipeline
python agentic_pipeline.py --records 10000 --embeddings
```

**Available Agent Tools:**
| Tool | Description |
|------|-------------|
| `query_transactions` | Filter, aggregate, find top N |
| `analyze_trends` | Revenue, counts by dimension |
| `search_knowledge` | RAG over embedded insights |
| `compare_segments` | Compare products, regions, channels |

**Sample Interaction:**
```
You: What are our top 3 products by revenue?

Agent: [Tool: query_transactions]
       [Tool: analyze_trends]

Based on the transaction data:
1. iPhone 15: $412,847 (28% of total)
2. Galaxy S24: $389,234 (26% of total)
3. Pixel 8: $298,472 (20% of total)
...
```

---

## Complete Pipeline Execution

### Option 1: Step-by-Step

```bash
# 1. Generate data
python generate_data.py

# 2. Start Kafka and stream data
docker-compose up -d
python kafka_producer.py

# 3. Process with Spark
python spark_batch_processing.py

# 4. Upload to S3 and run Glue
export GLUE_BUCKET=your-bucket
aws s3 sync output/ s3://$GLUE_BUCKET/data/
python glue_setup.py
python create_glue_job.py

# 5. Generate embeddings and build vector store
cd bedrock_agents
python bedrock_embeddings.py
python vector_store.py

# 6. Run agent
python bedrock_agent.py
```

### Option 2: Automated Pipeline

```bash
# Data engineering pipeline only
python full_pipeline.py

# Complete pipeline with Agentic AI
cd bedrock_agents
python agentic_pipeline.py --records 10000 --full
```

### Pipeline Options

```bash
python agentic_pipeline.py --help

Options:
  --records INT     Number of transactions to generate (default: 10000)
  --kafka           Enable Kafka streaming
  --embeddings      Enable Bedrock embedding generation
  --full            Run all components
```

---

## Configuration

### Docker Compose (Kafka)

```yaml
# docker-compose.yml
version: '3'
services:
  zookeeper:
    image: confluentinc/cp-zookeeper:7.4.0
    environment:
      ZOOKEEPER_CLIENT_PORT: 2181
    ports:
      - "2181:2181"

  kafka:
    image: confluentinc/cp-kafka:7.4.0
    depends_on:
      - zookeeper
    ports:
      - "9092:9092"
    environment:
      KAFKA_BROKER_ID: 1
      KAFKA_ZOOKEEPER_CONNECT: zookeeper:2181
      KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://localhost:9092
      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1
```

### AWS Configuration

```bash
# Configure AWS CLI
aws configure

# Required IAM permissions:
# - S3: read/write to your bucket
# - Glue: full access
# - Bedrock: InvokeModel permission
# - (Optional) OpenSearch Serverless for production vector store
```

### Environment Variables

```bash
export GLUE_BUCKET=your-glue-lab-bucket
export AWS_REGION=us-east-1
```

---

## Verification & Testing

```bash
# Test ijson
python -c "import ijson; print('✓ ijson')"

# Test Kafka connection
python -c "from kafka import KafkaProducer; p = KafkaProducer(bootstrap_servers=['localhost:9092']); print('✓ Kafka')"

# Test PySpark
python -c "from pyspark.sql import SparkSession; SparkSession.builder.getOrCreate().stop(); print('✓ Spark')"

# Test AWS credentials
aws sts get-caller-identity && echo "✓ AWS"

# Test Bedrock access
python -c "import boto3; boto3.client('bedrock-runtime', region_name='us-east-1'); print('✓ Bedrock')"

# List Kafka topics
docker exec -it $(docker ps -q -f name=kafka) kafka-topics --list --bootstrap-server localhost:9092
```

---

## Cleanup

```bash
# Stop Kafka
docker-compose down

# Remove local files
rm -rf large_transactions.json pipeline_data.json output/ pipeline_output/
rm -rf agent_pipeline_data.json agent_output/ agent_insights.json
rm -rf *.index *.pkl *_embeddings.json

# Delete AWS resources (optional)
aws glue delete-job --job-name transactions-etl-job
aws glue delete-crawler --crawler transactions-crawler
aws glue delete-database --name transactions_db
aws s3 rm s3://$GLUE_BUCKET --recursive
```

---

## Troubleshooting

| Issue | Solution |
|-------|----------|
| Kafka connection refused | Ensure Docker is running: `docker-compose up -d` |
| Spark OOM error | Reduce data size or increase driver memory |
| Bedrock access denied | Check IAM permissions for `bedrock:InvokeModel` |
| Glue job fails | Check CloudWatch logs in AWS Console |
| FAISS import error | Install with: `pip install faiss-cpu` |

---

## Key Takeaways

| Technology | Use Case | Key Insight |
|------------|----------|-------------|
| **ijson** | Large JSON parsing | Streaming parser — constant memory regardless of file size |
| **Kafka** | Real-time messaging | Decouples producers/consumers, enables replay |
| **PySpark** | Distributed processing | Same API for batch and streaming |
| **AWS Glue** | Managed ETL | Serverless, auto-scales, integrated catalog |
| **Bedrock Embeddings** | Vector generation | Semantic search over business data |
| **FAISS** | Vector similarity | Sub-millisecond search at scale |
| **Bedrock Agents** | Agentic AI | Autonomous reasoning with tool use |

---

## Resources

- [AWS Glue Documentation](https://docs.aws.amazon.com/glue/)
- [Apache Kafka Documentation](https://kafka.apache.org/documentation/)
- [PySpark Documentation](https://spark.apache.org/docs/latest/api/python/)
- [AWS Bedrock Documentation](https://docs.aws.amazon.com/bedrock/)
- [FAISS Documentation](https://faiss.ai/)

---

## License

MIT License - See [LICENSE](LICENSE) file

---

## Author

**Sarang** — Distinguished Engineer with 22+ years of enterprise platform transformation experience, currently focused on the intersection of Data Engineering and Generative AI.

- GitHub: [github.com/Sarang68](https://github.com/Sarang68)
- LinkedIn: [Connect with me](https://linkedin.com/in/your-profile)

---

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

1. Fork the repository
2. Create your feature branch (`git checkout -b feature/AmazingFeature`)
3. Commit your changes (`git commit -m 'Add some AmazingFeature'`)
4. Push to the branch (`git push origin feature/AmazingFeature`)
5. Open a Pull Request
