# Additional dependencies
pip install boto3 langchain langchain-aws opensearch-py faiss-cpu

# Ensure AWS credentials have Bedrock access
aws bedrock list-foundation-models --region us-east-1
