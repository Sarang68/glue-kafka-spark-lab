# vector_store.py
"""
Vector store implementation for transaction data RAG.
Supports FAISS (local) and OpenSearch Serverless (AWS).
"""
import json
import numpy as np
import faiss
from typing import List, Dict, Tuple
import pickle

class FAISSVectorStore:
    """Local FAISS vector store for development/testing"""
    
    def __init__(self, dimension: int = 1024):
        self.dimension = dimension
        self.index = faiss.IndexFlatIP(dimension)  # Inner product for cosine similarity
        self.documents = []
        self.metadata = []
    
    def add_documents(self, documents: List[Dict]):
        """Add documents with embeddings to the store"""
        embeddings = []
        for doc in documents:
            embeddings.append(doc['embedding'])
            self.documents.append(doc.get('text', ''))
            self.metadata.append(doc.get('metadata', {}))
        
        # Normalize for cosine similarity
        embeddings_array = np.array(embeddings, dtype='float32')
        faiss.normalize_L2(embeddings_array)
        self.index.add(embeddings_array)
        
        print(f"Added {len(documents)} documents. Total: {self.index.ntotal}")
    
    def search(self, query_embedding: List[float], k: int = 5) -> List[Tuple[str, Dict, float]]:
        """Search for similar documents"""
        query = np.array([query_embedding], dtype='float32')
        faiss.normalize_L2(query)
        
        distances, indices = self.index.search(query, k)
        
        results = []
        for i, idx in enumerate(indices[0]):
            if idx != -1:
                results.append((
                    self.documents[idx],
                    self.metadata[idx],
                    float(distances[0][i])
                ))
        return results
    
    def save(self, path: str):
        """Save vector store to disk"""
        faiss.write_index(self.index, f"{path}.index")
        with open(f"{path}.pkl", 'wb') as f:
            pickle.dump({'documents': self.documents, 'metadata': self.metadata}, f)
        print(f"Saved vector store to {path}")
    
    def load(self, path: str):
        """Load vector store from disk"""
        self.index = faiss.read_index(f"{path}.index")
        with open(f"{path}.pkl", 'rb') as f:
            data = pickle.load(f)
            self.documents = data['documents']
            self.metadata = data['metadata']
        print(f"Loaded vector store with {self.index.ntotal} documents")


class OpenSearchVectorStore:
    """AWS OpenSearch Serverless vector store for production"""
    
    def __init__(self, host: str, index_name: str, region: str = 'us-east-1'):
        from opensearchpy import OpenSearch, RequestsHttpConnection
        from requests_aws4auth import AWS4Auth
        import boto3
        
        credentials = boto3.Session().get_credentials()
        auth = AWS4Auth(
            credentials.access_key,
            credentials.secret_key,
            region,
            'aoss',
            session_token=credentials.token
        )
        
        self.client = OpenSearch(
            hosts=[{'host': host, 'port': 443}],
            http_auth=auth,
            use_ssl=True,
            verify_certs=True,
            connection_class=RequestsHttpConnection
        )
        self.index_name = index_name
    
    def create_index(self, dimension: int = 1024):
        """Create vector index"""
        index_body = {
            "settings": {
                "index": {
                    "knn": True,
                    "knn.algo_param.ef_search": 100
                }
            },
            "mappings": {
                "properties": {
                    "embedding": {
                        "type": "knn_vector",
                        "dimension": dimension,
                        "method": {
                            "name": "hnsw",
                            "space_type": "cosinesimil",
                            "engine": "nmslib"
                        }
                    },
                    "text": {"type": "text"},
                    "metadata": {"type": "object"}
                }
            }
        }
        self.client.indices.create(index=self.index_name, body=index_body)
        print(f"Created index: {self.index_name}")
    
    def add_documents(self, documents: List[Dict]):
        """Bulk index documents"""
        from opensearchpy.helpers import bulk
        
        actions = []
        for i, doc in enumerate(documents):
            actions.append({
                "_index": self.index_name,
                "_id": doc.get('document_id', str(i)),
                "_source": {
                    "embedding": doc['embedding'],
                    "text": doc.get('text', ''),
                    "metadata": doc.get('metadata', {})
                }
            })
        
        success, failed = bulk(self.client, actions)
        print(f"Indexed {success} documents, {failed} failed")
    
    def search(self, query_embedding: List[float], k: int = 5) -> List[Tuple[str, Dict, float]]:
        """KNN search"""
        query = {
            "size": k,
            "query": {
                "knn": {
                    "embedding": {
                        "vector": query_embedding,
                        "k": k
                    }
                }
            }
        }
        
        response = self.client.search(index=self.index_name, body=query)
        
        results = []
        for hit in response['hits']['hits']:
            results.append((
                hit['_source']['text'],
                hit['_source']['metadata'],
                hit['_score']
            ))
        return results


def build_vector_store():
    """Build and populate vector store from embeddings"""
    
    # Load embeddings
    with open('transaction_embeddings.json', 'r') as f:
        transactions = json.load(f)
    
    with open('insight_embeddings.json', 'r') as f:
        insights = json.load(f)
    
    # Create FAISS store
    store = FAISSVectorStore(dimension=1024)
    
    # Add all documents
    store.add_documents(transactions)
    store.add_documents(insights)
    
    # Save for later use
    store.save('transaction_vector_store')
    
    return store


if __name__ == "__main__":
    store = build_vector_store()
    print("\nVector store ready for RAG queries!")
