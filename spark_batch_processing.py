# spark_batch_processing.py
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import ijson
import os

def create_spark_session():
    return SparkSession.builder \
        .appName("BatchProcessing") \
        .config("spark.driver.memory", "2g") \
        .getOrCreate()

def ijson_to_spark_rdd(spark, filepath):
    """
    Use ijson to stream-parse JSON, then create Spark RDD.
    Useful when JSON structure is complex or file is huge.
    """
    # Parse with ijson first
    records = []
    with open(filepath, 'rb') as f:
        for item in ijson.items(f, 'transactions.item'):
            # Flatten metadata for Spark
            flat_record = {
                'transaction_id': item['transaction_id'],
                'customer_id': item['customer_id'],
                'product': item['product'],
                'amount': item['amount'],
                'timestamp': item['timestamp'],
                'store_id': item['store_id'],
                'payment_method': item['payment_method'],
                'region': item['metadata']['region'],
                'channel': item['metadata']['channel'],
                'promotion_applied': item['metadata']['promotion_applied']
            }
            records.append(flat_record)
    
    # Create DataFrame from parsed records
    return spark.createDataFrame(records)

def run_batch_analytics(spark, df):
    """Run comprehensive batch analytics"""
    
    df.createOrReplaceTempView("transactions")
    
    print("\n=== 1. Revenue by Region ===")
    spark.sql("""
        SELECT region, 
               COUNT(*) as transactions,
               ROUND(SUM(amount), 2) as total_revenue,
               ROUND(AVG(amount), 2) as avg_transaction
        FROM transactions
        GROUP BY region
        ORDER BY total_revenue DESC
    """).show()
    
    print("\n=== 2. Product Performance by Channel ===")
    spark.sql("""
        SELECT product, channel,
               COUNT(*) as transactions,
               ROUND(SUM(amount), 2) as revenue
        FROM transactions
        GROUP BY product, channel
        ORDER BY product, revenue DESC
    """).show(20)
    
    print("\n=== 3. Promotion Impact Analysis ===")
    spark.sql("""
        SELECT promotion_applied,
               COUNT(*) as transactions,
               ROUND(AVG(amount), 2) as avg_amount,
               ROUND(SUM(amount), 2) as total_revenue
        FROM transactions
        GROUP BY promotion_applied
    """).show()
    
    print("\n=== 4. Top 10 Stores by Revenue ===")
    spark.sql("""
        SELECT store_id,
               COUNT(*) as transactions,
               ROUND(SUM(amount), 2) as revenue
        FROM transactions
        GROUP BY store_id
        ORDER BY revenue DESC
        LIMIT 10
    """).show()
    
    return df

def save_to_parquet(df, output_path):
    """Save processed data as Parquet (Glue-ready format)"""
    df.write \
        .mode("overwrite") \
        .partitionBy("region", "product") \
        .parquet(output_path)
    print(f"\nSaved to Parquet: {output_path}")

if __name__ == "__main__":
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")
    
    print("Loading data with ijson + Spark...")
    df = ijson_to_spark_rdd(spark, "large_transactions.json")
    
    print(f"Loaded {df.count()} records")
    df.printSchema()
    
    # Run analytics
    df = run_batch_analytics(spark, df)
    
    # Save for Glue
    save_to_parquet(df, "output/transactions_parquet")
    
    spark.stop()
