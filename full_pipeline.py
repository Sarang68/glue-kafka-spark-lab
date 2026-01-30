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
Complete pipeline: JSON → ijson → Kafka → Spark → Parquet → S3 → Glue
"""
import os
import time
import threading
from concurrent.futures import ThreadPoolExecutor

# Import our modules
from generate_data import generate_transactions
from kafka_producer import stream_json_to_kafka
from spark_batch_processing import create_spark_session, ijson_to_spark_rdd, save_to_parquet

def step1_generate_data():
    """Generate sample data"""
    print("\n" + "="*50)
    print("STEP 1: Generating sample data...")
    print("="*50)
    import json
    data = generate_transactions(25000)
    with open("pipeline_data.json", "w") as f:
        json.dump(data, f)
    print("✓ Generated 25,000 transactions")

def step2_stream_to_kafka():
    """Stream to Kafka"""
    print("\n" + "="*50)
    print("STEP 2: Streaming to Kafka...")
    print("="*50)
    stream_json_to_kafka("pipeline_data.json", topic="pipeline-transactions")
    print("✓ Streamed to Kafka topic: pipeline-transactions")

def step3_spark_processing():
    """Process with Spark"""
    print("\n" + "="*50)
    print("STEP 3: Processing with PySpark...")
    print("="*50)
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("ERROR")
    
    df = ijson_to_spark_rdd(spark, "pipeline_data.json")
    
    # Analytics
    print("\nQuick Analytics:")
    df.groupBy("product").agg(
        {"amount": "sum", "*": "count"}
    ).show()
    
    # Save
    save_to_parquet(df, "pipeline_output/transactions")
    spark.stop()
    print("✓ Saved Parquet files")

def step4_upload_to_s3():
    """Upload to S3 for Glue"""
    print("\n" + "="*50)
    print("STEP 4: Uploading to S3...")
    print("="*50)
    
    bucket = os.environ.get('GLUE_BUCKET', 'your-glue-lab-bucket')
    os.system(f"aws s3 sync pipeline_output/ s3://{bucket}/pipeline/")
    print(f"✓ Uploaded to s3://{bucket}/pipeline/")

def run_full_pipeline():
    """Execute complete pipeline"""
    start = time.time()
    
    step1_generate_data()
    step2_stream_to_kafka()
    step3_spark_processing()
    step4_upload_to_s3()
    
    elapsed = time.time() - start
    print("\n" + "="*50)
    print(f"PIPELINE COMPLETE in {elapsed:.2f} seconds")
    print("="*50)
    print("\nNext steps:")
    print("1. Run Glue Crawler on s3://bucket/pipeline/")
    print("2. Execute Glue ETL job")
    print("3. Query with Athena")

if __name__ == "__main__":
    run_full_pipeline()
