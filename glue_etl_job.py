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

import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql.functions import *

# Initialize Glue context
args = getResolvedOptions(sys.argv, ['JOB_NAME', 'SOURCE_BUCKET', 'TARGET_BUCKET'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Read from Data Catalog (created by crawler)
datasource = glueContext.create_dynamic_frame.from_catalog(
    database="transactions_db",
    table_name="transactions_parquet"
)

print(f"Record count: {datasource.count()}")
datasource.printSchema()

# Convert to Spark DataFrame for complex transformations
df = datasource.toDF()

# Transformation 1: Add derived columns
df_transformed = df \
    .withColumn("transaction_date", to_date("timestamp")) \
    .withColumn("transaction_hour", hour(to_timestamp("timestamp"))) \
    .withColumn("is_high_value", when(col("amount") > 500, True).otherwise(False)) \
    .withColumn("amount_bucket", 
        when(col("amount") < 200, "low")
        .when(col("amount") < 500, "medium")
        .when(col("amount") < 1000, "high")
        .otherwise("premium"))

# Transformation 2: Create aggregated summary table
daily_summary = df_transformed.groupBy("transaction_date", "region", "product") \
    .agg(
        count("*").alias("transaction_count"),
        sum("amount").alias("daily_revenue"),
        avg("amount").alias("avg_transaction_value"),
        sum(when(col("promotion_applied") == True, 1).otherwise(0)).alias("promo_transactions")
    )

# Convert back to DynamicFrame
transformed_dyf = DynamicFrame.fromDF(df_transformed, glueContext, "transformed")
summary_dyf = DynamicFrame.fromDF(daily_summary, glueContext, "summary")

# Write transformed data
glueContext.write_dynamic_frame.from_options(
    frame=transformed_dyf,
    connection_type="s3",
    connection_options={
        "path": f"s3://{args['TARGET_BUCKET']}/processed/transactions/",
        "partitionKeys": ["region", "transaction_date"]
    },
    format="parquet"
)

# Write summary data
glueContext.write_dynamic_frame.from_options(
    frame=summary_dyf,
    connection_type="s3",
    connection_options={
        "path": f"s3://{args['TARGET_BUCKET']}/aggregated/daily_summary/"
    },
    format="parquet"
)

job.commit()
print("ETL Job completed successfully!")
