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
Generate embeddings from processed transaction data for RAG.
Uses AWS Bedrock Titan Embeddings model.
"""
import boto3
import json
import pandas as pd
from typing import List, Dict
import numpy as np

class BedrockEmbeddings:
    def __init__(self, region: str = 'us-east-1'):
        self.bedrock_runtime = boto3.client(
            service_name='bedrock-runtime',
            region_name=region
        )
        self.model_id = 'amazon.titan-embed-text-v2:0'
    
    def get_embedding(self, text: str) -> List[float]:
        """Generate embedding for a single text"""
        response = self.bedrock_runtime.invoke_model(
            modelId=self.model_id,
            body=json.dumps({
                "inputText": text,
                "dimensions": 1024,
                "normalize": True
            })
        )
        result = json.loads(response['body'].read())
        return result['embedding']
    
    def get_batch_embeddings(self, texts: List[str], batch_size: int = 10) -> List[List[float]]:
        """Generate embeddings for multiple texts"""
        embeddings = []
        for i in range(0, len(texts), batch_size):
            batch = texts[i:i + batch_size]
            for text in batch:
                embedding = self.get_embedding(text)
                embeddings.append(embedding)
            print(f"Processed {min(i + batch_size, len(texts))}/{len(texts)} texts")
        return embeddings


def create_transaction_summaries(parquet_path: str) -> List[Dict]:
    """
    Convert transaction records to text summaries for embedding.
    This creates rich context for RAG retrieval.
    """
    df = pd.read_parquet(parquet_path)
    
    summaries = []
    for _, row in df.iterrows():
        # Create natural language summary of each transaction
        summary = f"""
        Transaction {row['transaction_id']} on {row['timestamp']}:
        Customer {row['customer_id']} purchased {row['product']} 
        for ${row['amount']:.2f} at store {row['store_id']}.
        Payment method: {row['payment_method']}.
        Region: {row['region']}, Channel: {row['channel']}.
        Promotion applied: {row['promotion_applied']}.
        """
        summaries.append({
            'transaction_id': row['transaction_id'],
            'text': summary.strip(),
            'metadata': {
                'product': row['product'],
                'region': row['region'],
                'amount': float(row['amount']),
                'store_id': row['store_id']
            }
        })
    
    return summaries


def create_aggregated_insights(parquet_path: str) -> List[Dict]:
    """
    Create aggregated insights documents for knowledge base.
    These provide high-level business context.
    """
    df = pd.read_parquet(parquet_path)
    
    insights = []
    
    # Regional insights
    regional = df.groupby('region').agg({
        'amount': ['sum', 'mean', 'count'],
        'promotion_applied': 'mean'
    }).round(2)
    
    for region in regional.index:
        stats = regional.loc[region]
        insight = f"""
        Regional Performance Summary - {region}:
        Total Revenue: ${stats[('amount', 'sum')]:,.2f}
        Average Transaction: ${stats[('amount', 'mean')]:,.2f}
        Total Transactions: {int(stats[('amount', 'count')])}
        Promotion Usage Rate: {stats[('promotion_applied', 'mean')]*100:.1f}%
        """
        insights.append({
            'document_id': f'regional_{region.lower()}',
            'text': insight.strip(),
            'metadata': {'type': 'regional_summary', 'region': region}
        })
    
    # Product insights
    product = df.groupby('product').agg({
        'amount': ['sum', 'mean', 'count']
    }).round(2)
    
    for product_name in product.index:
        stats = product.loc[product_name]
        insight = f"""
        Product Performance Summary - {product_name}:
        Total Revenue: ${stats[('amount', 'sum')]:,.2f}
        Average Sale Price: ${stats[('amount', 'mean')]:,.2f}
        Units Sold: {int(stats[('amount', 'count')])}
        """
        insights.append({
            'document_id': f'product_{product_name.lower().replace(" ", "_")}',
            'text': insight.strip(),
            'metadata': {'type': 'product_summary', 'product': product_name}
        })
    
    return insights


if __name__ == "__main__":
    # Generate embeddings for transaction summaries
    embedder = BedrockEmbeddings()
    
    print("Creating transaction summaries...")
    summaries = create_transaction_summaries("output/transactions_parquet")
    
    print(f"\nGenerating embeddings for {len(summaries[:100])} transactions...")
    texts = [s['text'] for s in summaries[:100]]  # Limit for demo
    embeddings = embedder.get_batch_embeddings(texts)
    
    # Save embeddings
    for i, emb in enumerate(embeddings):
        summaries[i]['embedding'] = emb
    
    with open('transaction_embeddings.json', 'w') as f:
        json.dump(summaries[:100], f)
    
    print(f"Saved embeddings to transaction_embeddings.json")
    
    # Create aggregated insights
    print("\nCreating aggregated insights...")
    insights = create_aggregated_insights("output/transactions_parquet")
    
    print(f"Generating embeddings for {len(insights)} insights...")
    insight_texts = [i['text'] for i in insights]
    insight_embeddings = embedder.get_batch_embeddings(insight_texts)
    
    for i, emb in enumerate(insight_embeddings):
        insights[i]['embedding'] = emb
    
    with open('insight_embeddings.json', 'w') as f:
        json.dump(insights, f)
    
    print(f"Saved insights to insight_embeddings.json")
