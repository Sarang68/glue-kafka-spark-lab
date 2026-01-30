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

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

# Define schema for our transactions
transaction_schema = StructType([
    StructField("transaction_id", StringType(), True),
    StructField("customer_id", StringType(), True),
    StructField("product", StringType(), True),
    StructField("amount", DoubleType(), True),
    StructField("timestamp", StringType(), True),
    StructField("store_id", StringType(), True),
    StructField("payment_method", StringType(), True),
    StructField("metadata", StructType([
        StructField("region", StringType(), True),
        StructField("channel", StringType(), True),
        StructField("promotion_applied", BooleanType(), True)
    ]), True)
])

def create_spark_session():
    """Create Spark session with Kafka integration"""
    spark = (SparkSession.builder
        .appName("KafkaSparkStreaming")
        .config("spark.jars.packages", 
                "org.apache.spark:spark-sql-kafka-0-10_2.13:3.5.3")
        .config("spark.sql.streaming.checkpointLocation", "/tmp/checkpoint")
        .config("spark.driver.memory", "2g")
        .master("local[*]")
        .getOrCreate())
    
    spark.sparkContext.setLogLevel("WARN")
    return spark
def run_streaming_aggregation():
    """
    Read from Kafka, parse JSON, and compute real-time aggregations.
    """
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")
    
    # Read from Kafka
    kafka_df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("subscribe", "transactions") \
        .option("startingOffsets", "earliest") \
        .load()
    
    # Parse JSON from Kafka value
    parsed_df = kafka_df \
        .select(
            col("key").cast("string").alias("key"),
            from_json(col("value").cast("string"), transaction_schema).alias("data"),
            col("timestamp").alias("kafka_timestamp")
        ) \
        .select("key", "data.*", "kafka_timestamp")
    
    # Real-time aggregation: Revenue by product in 1-minute windows
    windowed_agg = parsed_df \
        .withColumn("event_time", to_timestamp("timestamp")) \
        .withWatermark("event_time", "2 minutes") \
        .groupBy(
            window("event_time", "1 minute"),
            "product"
        ) \
        .agg(
            count("*").alias("transaction_count"),
            sum("amount").alias("total_revenue"),
            avg("amount").alias("avg_transaction")
        )
    
    # Write to console (for demo)
    query = windowed_agg.writeStream \
        .outputMode("update") \
        .format("console") \
        .option("truncate", False) \
        .trigger(processingTime="10 seconds") \
        .start()
    
    print("Streaming started. Press Ctrl+C to stop...")
    query.awaitTermination()

if __name__ == "__main__":
    run_streaming_aggregation()
