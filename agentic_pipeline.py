# MIT License
#
# Copyright (c) 2026 Sarang68
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in all
# copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.

"""
Complete pipeline: Data Ingestion → Processing → Embeddings → Agent
"""
import os
import time

def run_complete_agentic_pipeline():
    """Execute the complete pipeline from data to agent"""
    
    print("=" * 70)
    print("AGENTIC AI PIPELINE - AWS Glue + Kafka + Spark + Bedrock")
    print("=" * 70)
    
    # Step 1: Generate and ingest data
    print("\n[PHASE 1] Data Generation & Ingestion")
    print("-" * 50)
    from generate_data import generate_transactions
    import json
    
    data = generate_transactions(10000)
    with open("agent_pipeline_data.json", "w") as f:
        json.dump(data, f)
    print("✓ Generated 10,000 transactions")
    
    # Step 2: Stream to Kafka (if running)
    print("\n[PHASE 2] Kafka Streaming")
    print("-" * 50)
    try:
        from kafka_producer import stream_json_to_kafka
        stream_json_to_kafka("agent_pipeline_data.json", topic="agent-transactions")
        print("✓ Streamed to Kafka")
    except Exception as e:
        print(f"⚠ Kafka not available, skipping: {e}")
    
    # Step 3: Spark processing
    print("\n[PHASE 3] PySpark Processing")
    print("-" * 50)
    from spark_batch_processing import create_spark_session, ijson_to_spark_rdd, save_to_parquet
    
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("ERROR")
    df = ijson_to_spark_rdd(spark, "agent_pipeline_data.json")
    save_to_parquet(df, "agent_output/transactions")
    spark.stop()
    print("✓ Processed and saved to Parquet")
    
    # Step 4: Generate embeddings
    print("\n[PHASE 4] Embedding Generation")
    print("-" * 50)
    try:
        from bedrock_embeddings import BedrockEmbeddings, create_aggregated_insights
        
        embedder = BedrockEmbeddings()
        insights = create_aggregated_insights("agent_output/transactions")
        
        for insight in insights:
            insight['embedding'] = embedder.get_embedding(insight['text'])
        
        with open('agent_insights.json', 'w') as f:
            json.dump(insights, f)
        print(f"✓ Generated embeddings for {len(insights)} insights")
    except Exception as e:
        print(f"⚠ Bedrock not available: {e}")
        print("  Skipping embedding generation")
    
    # Step 5: Build vector store
    print("\n[PHASE 5] Vector Store")
    print("-" * 50)
    try:
        from vector_store import FAISSVectorStore
        
        store = FAISSVectorStore()
        with open('agent_insights.json', 'r') as f:
            insights = json.load(f)
        store.add_documents(insights)
        store.save('agent_vector_store')
        print("✓ Built FAISS vector store")
    except Exception as e:
        print(f"⚠ Vector store setup failed: {e}")
    
    # Step 6: Initialize agent
    print("\n[PHASE 6] Agent Initialization")
    print("-" * 50)
    try:
        from bedrock_agent import TransactionAgent
        agent = TransactionAgent()
        print("✓ Agent initialized and ready")
        
        # Demo query
        print("\n[DEMO QUERY]")
        print("-" * 50)
        query = "What are the top 3 products by revenue and which regions are they selling best in?"
        print(f"Query: {query}\n")
        response = agent.chat(query)
        print(f"Response:\n{response}")
        
    except Exception as e:
        print(f"⚠ Agent initialization failed: {e}")
    
    print("\n" + "=" * 70)
    print("PIPELINE COMPLETE")
    print("=" * 70)


if __name__ == "__main__":
    run_complete_agentic_pipeline()
