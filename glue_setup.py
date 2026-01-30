# glue_setup.py
import boto3

glue_client = boto3.client('glue', region_name='us-east-1')
BUCKET_NAME = 'your-glue-lab-bucket'  # Replace with your bucket

def create_glue_database():
    """Create Glue Data Catalog database"""
    try:
        glue_client.create_database(
            DatabaseInput={
                'Name': 'transactions_db',
                'Description': 'Transaction analytics database'
            }
        )
        print("Created database: transactions_db")
    except glue_client.exceptions.AlreadyExistsException:
        print("Database already exists")

def create_glue_crawler():
    """Create Glue Crawler to discover schema"""
    try:
        glue_client.create_crawler(
            Name='transactions-crawler',
            Role='AWSGlueServiceRole',  # Create this role in IAM first
            DatabaseName='transactions_db',
            Targets={
                'S3Targets': [
                    {'Path': f's3://{BUCKET_NAME}/raw/transactions/'}
                ]
            },
            SchemaChangePolicy={
                'UpdateBehavior': 'UPDATE_IN_DATABASE',
                'DeleteBehavior': 'LOG'
            }
        )
        print("Created crawler: transactions-crawler")
    except glue_client.exceptions.AlreadyExistsException:
        print("Crawler already exists")

def run_crawler():
    """Start the crawler"""
    glue_client.start_crawler(Name='transactions-crawler')
    print("Started crawler. Check AWS Console for progress.")

if __name__ == "__main__":
    create_glue_database()
    create_glue_crawler()
    run_crawler()
