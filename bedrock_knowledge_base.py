# bedrock_knowledge_base.py
"""
Setup and manage AWS Bedrock Knowledge Base.
Integrates S3 data with Bedrock for native RAG capabilities.
"""
import boto3
import json
import time
from typing import Dict, Optional

class BedrockKnowledgeBase:
    """Manage AWS Bedrock Knowledge Base for transaction data"""
    
    def __init__(self, region: str = 'us-east-1'):
        self.bedrock_agent = boto3.client('bedrock-agent', region_name=region)
        self.bedrock_agent_runtime = boto3.client('bedrock-agent-runtime', region_name=region)
        self.s3 = boto3.client('s3', region_name=region)
        self.region = region
    
    def create_knowledge_base(
        self,
        name: str,
        description: str,
        role_arn: str,
        embedding_model: str = 'amazon.titan-embed-text-v2:0'
    ) -> Dict:
        """Create a new knowledge base"""
        
        response = self.bedrock_agent.create_knowledge_base(
            name=name,
            description=description,
            roleArn=role_arn,
            knowledgeBaseConfiguration={
                'type': 'VECTOR',
                'vectorKnowledgeBaseConfiguration': {
                    'embeddingModelArn': f'arn:aws:bedrock:{self.region}::foundation-model/{embedding_model}'
                }
            },
            storageConfiguration={
                'type': 'OPENSEARCH_SERVERLESS',
                'opensearchServerlessConfiguration': {
                    'collectionArn': 'YOUR_OPENSEARCH_COLLECTION_ARN',  # Replace
                    'vectorIndexName': 'transaction-vectors',
                    'fieldMapping': {
                        'vectorField': 'embedding',
                        'textField': 'text',
                        'metadataField': 'metadata'
                    }
                }
            }
        )
        
        print(f"Created Knowledge Base: {response['knowledgeBase']['knowledgeBaseId']}")
        return response['knowledgeBase']
    
    def add_s3_data_source(
        self,
        knowledge_base_id: str,
        bucket_name: str,
        prefix: str,
        name: str = 'transaction-data-source'
    ) -> Dict:
        """Add S3 data source to knowledge base"""
        
        response = self.bedrock_agent.create_data_source(
            knowledgeBaseId=knowledge_base_id,
            name=name,
            dataSourceConfiguration={
                'type': 'S3',
                's3Configuration': {
                    'bucketArn': f'arn:aws:s3:::{bucket_name}',
                    'inclusionPrefixes': [prefix]
                }
            },
            vectorIngestionConfiguration={
                'chunkingConfiguration': {
                    'chunkingStrategy': 'FIXED_SIZE',
                    'fixedSizeChunkingConfiguration': {
                        'maxTokens': 512,
                        'overlapPercentage': 20
                    }
                }
            }
        )
        
        print(f"Created Data Source: {response['dataSource']['dataSourceId']}")
        return response['dataSource']
    
    def start_ingestion(self, knowledge_base_id: str, data_source_id: str) -> str:
        """Start data ingestion job"""
        
        response = self.bedrock_agent.start_ingestion_job(
            knowledgeBaseId=knowledge_base_id,
            dataSourceId=data_source_id
        )
        
        job_id = response['ingestionJob']['ingestionJobId']
        print(f"Started Ingestion Job: {job_id}")
        return job_id
    
    def query_knowledge_base(
        self,
        knowledge_base_id: str,
        query: str,
        num_results: int = 5
    ) -> Dict:
        """Query the knowledge base"""
        
        response = self.bedrock_agent_runtime.retrieve(
            knowledgeBaseId=knowledge_base_id,
            retrievalQuery={'text': query},
            retrievalConfiguration={
                'vectorSearchConfiguration': {
                    'numberOfResults': num_results
                }
            }
        )
        
        return response['retrievalResults']
    
    def query_with_generation(
        self,
        knowledge_base_id: str,
        query: str,
        model_id: str = 'anthropic.claude-3-sonnet-20240229-v1:0'
    ) -> str:
        """Query knowledge base and generate response (RAG)"""
        
        response = self.bedrock_agent_runtime.retrieve_and_generate(
            input={'text': query},
            retrieveAndGenerateConfiguration={
                'type': 'KNOWLEDGE_BASE',
                'knowledgeBaseConfiguration': {
                    'knowledgeBaseId': knowledge_base_id,
                    'modelArn': f'arn:aws:bedrock:{self.region}::foundation-model/{model_id}'
                }
            }
        )
        
        return response['output']['text']


def setup_knowledge_base_infrastructure():
    """
    Infrastructure setup guide for Bedrock Knowledge Base.
    This would typically be done via CloudFormation or Terraform.
    """
    
    cloudformation_template = """
AWSTemplateFormatVersion: '2010-09-09'
Description: Bedrock Knowledge Base Infrastructure for Transaction Analytics

Parameters:
  BucketName:
    Type: String
    Description: S3 bucket containing transaction data

Resources:
  # OpenSearch Serverless Collection
  TransactionVectorCollection:
    Type: AWS::OpenSearchServerless::Collection
    Properties:
      Name: transaction-vectors
      Type: VECTORSEARCH
      Description: Vector store for transaction embeddings

  # Security Policy for OpenSearch
  SecurityPolicy:
    Type: AWS::OpenSearchServerless::SecurityPolicy
    Properties:
      Name: transaction-security-policy
      Type: encryption
      Policy:
        Rules:
          - ResourceType: collection
            Resource:
              - collection/transaction-vectors
        AWSOwnedKey: true

  # Network Policy
  NetworkPolicy:
    Type: AWS::OpenSearchServerless::SecurityPolicy
    Properties:
      Name: transaction-network-policy
      Type: network
      Policy:
        - Rules:
            - ResourceType: collection
              Resource:
                - collection/transaction-vectors
            - ResourceType: dashboard
              Resource:
                - collection/transaction-vectors
          AllowFromPublic: true

  # IAM Role for Bedrock
  BedrockKnowledgeBaseRole:
    Type: AWS::IAM::Role
    Properties:
      RoleName: BedrockKnowledgeBaseRole
      AssumeRolePolicyDocument:
        Version: '2012-10-17'
        Statement:
          - Effect: Allow
            Principal:
              Service: bedrock.amazonaws.com
            Action: sts:AssumeRole
      ManagedPolicyArns:
        - arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess
      Policies:
        - PolicyName: BedrockKBPolicy
          PolicyDocument:
            Version: '2012-10-17'
            Statement:
              - Effect: Allow
                Action:
                  - aoss:APIAccessAll
                Resource: '*'
              - Effect: Allow
                Action:
                  - bedrock:InvokeModel
                Resource: '*'

Outputs:
  CollectionArn:
    Value: !GetAtt TransactionVectorCollection.Arn
  RoleArn:
    Value: !GetAtt BedrockKnowledgeBaseRole.Arn
"""
    
    print("CloudFormation template for infrastructure setup:")
    print(cloudformation_template)
    
    return cloudformation_template


if __name__ == "__main__":
    # Print setup guide
    print("=" * 60)
    print("AWS Bedrock Knowledge Base Setup Guide")
    print("=" * 60)
    
    print("""
Steps to set up Bedrock Knowledge Base:

1. Deploy CloudFormation template (generated below)
2. Upload transaction insights to S3:
   aws s3 sync insight_documents/ s3://your-bucket/knowledge-base/

3. Create Knowledge Base:
   kb = BedrockKnowledgeBase()
   kb.create_knowledge_base(
       name='transaction-analytics-kb',
       description='Knowledge base for transaction analytics',
       role_arn='arn:aws:iam::ACCOUNT:role/BedrockKnowledgeBaseRole'
   )

4. Add data source and start ingestion

5. Query using RAG:
   response = kb.query_with_generation(
       knowledge_base_id='YOUR_KB_ID',
       query='What are the top performing products?'
   )
""")
    
    setup_knowledge_base_infrastructure()
