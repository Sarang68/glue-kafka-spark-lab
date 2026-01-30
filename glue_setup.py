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
