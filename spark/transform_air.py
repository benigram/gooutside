import sys
import time
import os
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import to_timestamp, to_date, col
from google.cloud import storage

# Argument: timestamp string (e.g. "2025-05-06T12:00:00+00:00")
ts_str = sys.argv[1]
ts = datetime.fromisoformat(ts_str)
date = ts.date().isoformat()

# Init Spark
spark = SparkSession.builder \
    .appName("AirQualityToParquet - Single Day") \
    .getOrCreate()

# Auth for GCS
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "/opt/credentials/fastapi-gcs-key.json"

# Paths
raw_bucket = "gooutside-raw"
processed_bucket = "gooutside-processed"
input_path = f"gs://{raw_bucket}/air/bamberg/{date}T*.json"  # <- Wildcard!
output_path = f"gs://{processed_bucket}/parquet/air/bamberg/"

print(f"🔍 Reading all files for date {date}: {input_path}")

# Read all JSONs for the day
df = spark.read.option("multiline", "true").json(input_path)

# Check schema
df.printSchema()
df.show(5, truncate=False)

# Transform timestamp
df = df.withColumn("timestamp", to_timestamp("timestamp"))
df = df.withColumn("date", to_date("timestamp"))

# Flatten 'components' field
df = df.withColumn("NO2", col("components").getItem("NO2"))
df = df.withColumn("PM10", col("components").getItem("PM10"))
df = df.withColumn("PM25", col("components").getItem("PM2.5"))
df = df.drop("components")

# Make sure only the matching partition is overwritten
spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

# Write to Parquet partitioned by date
df.write \
    .mode("overwrite") \
    .partitionBy("date") \
    .parquet(output_path)

print(f"✅ Saved all files for {date} to: {output_path}")

