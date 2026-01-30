# Configure AWS CLI
aws configure
# Enter your AWS Access Key, Secret Key, Region (e.g., us-east-1)

# Create S3 bucket for Glue
aws s3 mb s3://your-glue-lab-bucket-${USER}

# Upload Parquet files to S3
aws s3 sync output/transactions_parquet/ s3://your-glue-lab-bucket-${USER}/raw/transactions/
