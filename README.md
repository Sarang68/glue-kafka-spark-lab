# AWS Bedrock Agentic AI Extension

This module extends the Glue-Kafka-Spark data pipeline with **Agentic AI** capabilities using AWS Bedrock.

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                    DATA PIPELINE                             │
│  JSON → ijson → Kafka → PySpark → Parquet → S3 → Glue      │
└─────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────┐
│                    AI LAYER                                  │
│  Bedrock Embeddings → FAISS Vector Store → RAG              │
└─────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────┐
│                 AGENTIC AI LAYER                             │
│  Transaction Agent ←→ Bedrock Claude (Tool Use)             │
│  Multi-Agent System: Supervisor + Specialized Agents        │
└─────────────────────────────────────────────────────────────┘
```

## Components

| File | Purpose |
|------|---------|
| `bedrock_embeddings.py` | Generate embeddings using Titan Embed v2 |
| `vector_store.py` | FAISS (local) and OpenSearch (AWS) vector stores |
| `bedrock_agent.py` | Single agent with tool use pattern |
| `multi_agent_system.py` | Supervisor + specialized agents |
| `agentic_pipeline.py` | End-to-end pipeline integration |

## Quick Start

### 1. Install Dependencies

```bash
pip install -r requirements.txt
```

### 2. Configure AWS

```bash
aws configure
# Ensure your IAM role has bedrock:InvokeModel permissions
```

### 3. Run the Pipeline

```bash
# Quick demo (no Kafka, no embeddings)
python agentic_pipeline.py --records 5000

# Full pipeline with embeddings
python agentic_pipeline.py --records 10000 --embeddings

# Full pipeline with all components
python agentic_pipeline.py --records 10000 --full
```

### 4. Interactive Agent

```bash
# Single agent with tool use
python bedrock_agent.py

# Multi-agent system
python multi_agent_system.py
```

## Usage Examples

### Single Agent

```python
from bedrock_agent import TransactionAgent

agent = TransactionAgent()
response = agent.chat("What are the top products by revenue?")
print(response)
```

### Multi-Agent System

```python
from multi_agent_system import SupervisorAgent

supervisor = SupervisorAgent()
response = supervisor.process(
    "Analyze sales trends and provide recommendations"
)
print(response)
```

### RAG with Vector Store

```python
from bedrock_embeddings import BedrockEmbeddings
from vector_store import FAISSVectorStore

# Generate embeddings
embedder = BedrockEmbeddings()
embedding = embedder.get_embedding("iPhone sales in Northeast")

# Search
store = FAISSVectorStore()
store.load('transaction_vector_store')
results = store.search(embedding, k=5)
```

## Agent Tools

The Transaction Agent has access to these tools:

| Tool | Description |
|------|-------------|
| `query_transactions` | Filter, aggregate, find top N transactions |
| `analyze_trends` | Revenue, count, avg transaction by dimension |
| `search_knowledge` | RAG search over embedded insights |
| `compare_segments` | Compare products, regions, channels |

## Multi-Agent Roles

| Agent | Specialty |
|-------|-----------|
| **Supervisor** | Orchestrates, delegates, synthesizes |
| **Transaction Analyst** | Queries and analyzes raw data |
| **Trend Analyzer** | Identifies patterns and anomalies |
| **Report Generator** | Creates summaries and recommendations |
| **Knowledge Specialist** | RAG-based contextual retrieval |

## AWS Bedrock Models Used

- **Embeddings**: `amazon.titan-embed-text-v2:0`
- **Agent (main)**: `anthropic.claude-3-sonnet-20240229-v1:0`
- **Agent (specialized)**: `anthropic.claude-3-haiku-20240307-v1:0`

## Production Deployment

For production, consider:

1. **OpenSearch Serverless** instead of FAISS for vector store
2. **Bedrock Knowledge Base** for managed RAG
3. **Lambda functions** for agent action groups
4. **Step Functions** for workflow orchestration

See `bedrock_knowledge_base.py` in the main extension docs for CloudFormation templates.

## Sample Queries

```
- "What are our top 3 products by revenue?"
- "Compare performance across regions"
- "Analyze the impact of promotions on sales"
- "Which stores are underperforming?"
- "Give me an executive summary with recommendations"
```

## License

MIT - See LICENSE file
