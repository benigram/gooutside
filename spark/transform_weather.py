import sys
import os
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import to_timestamp, to_date, col
from google.cloud import storage

# Argument: date string (e.g. "2024-05-01")
date_str = sys.argv[1]
date = datetime.strptime(date_str, "%Y-%m-%d").date()

# Init Spark
spark = SparkSession.builder \
    .appName("WeatherToParquet - Single Day") \
    .getOrCreate()

# Auth for GCS
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "/app/credentials/fastapi-gcs-key.json"

# GCS paths
raw_bucket = "gooutside-raw"
processed_bucket = "gooutside-processed"
input_path = f"gs://{raw_bucket}/weather/bamberg/{date}T*.json"
output_path = f"gs://{processed_bucket}/parquet/weather/bamberg/"

print(f"🔍 Reading weather files for {date}: {input_path}")

# Read JSON from GCS
df = spark.read.option("multiline", "true").json(input_path)

# Check schema
df.printSchema()
df.show(5, truncate=False)

# Transform timestamp
df = df.withColumn("timestamp", to_timestamp("timestamp"))
df = df.withColumn("date", to_date("timestamp"))

# Check schema
df.printSchema()
df.show(5, truncate=False)

# Write as Parquet (only overwrite matching partition)
spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

df.write \
    .mode("overwrite") \
    .partitionBy("date") \
    .parquet(output_path)

print(f"✅ Finished writing weather data for {date} to: {output_path}")
