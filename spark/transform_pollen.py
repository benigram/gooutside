import sys
import time
import os
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, lit
from google.cloud import storage

# Arguments
execution_date_str = sys.argv[1]  # "2025-05-05"
execution_date = datetime.strptime(execution_date_str, "%Y-%m-%d").date()

# Init Spark
spark = SparkSession.builder \
    .appName("PollenToParquet") \
    .getOrCreate()

# Auth for GCS
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "/opt/credentials/fastapi-gcs-key.json"

# GCS paths
raw_bucket = "gooutside-raw"
processed_bucket = "gooutside-processed"

input_filename = f"pollen/bayern_nord/{execution_date}.json"
input_path = f"gs://{raw_bucket}/{input_filename}"
output_path = f"gs://{processed_bucket}/flat/pollen_bayern_nord/"

# Wait for file to appear in GCS (max 60 seconds)
print(f"🔍 Looking for GCS file: {input_filename}")
client = storage.Client()
bucket = client.bucket(raw_bucket)

for attempt in range(12):
    if bucket.blob(input_filename).exists():
        print(f"✅ Found: {input_filename}")
        break
    print(f"⏳ File not found yet... attempt {attempt + 1}/12")
    time.sleep(5)
else:
    print("❌ GCS file not found after waiting. Aborting Spark job.")
    sys.exit(1)

# Read JSON file from GCS
print(f"📥 Reading from: {input_path}")
df = spark.read.option("multiline", "true").json(input_path)

# Check initial schema
df.printSchema()
df.show(5, truncate=False)


# Flatten 'forecast' field
df = df.withColumn("Esche", col("forecast").getItem("Esche")) \
       .withColumn("Graeser", col("forecast").getItem("Graeser")) \
       .withColumn("Beifuss", col("forecast").getItem("Beifuss")) \
       .withColumn("Birke", col("forecast").getItem("Birke")) \
       .withColumn("Ambrosia", col("forecast").getItem("Ambrosia")) \
       .withColumn("Erle", col("forecast").getItem("Erle")) \
       .withColumn("Hasel", col("forecast").getItem("Hasel")) \
       .withColumn("Roggen", col("forecast").getItem("Roggen")) \
       .drop("forecast")

# Add static date column for filtering in BigQuery
df = df.withColumn("date", lit(execution_date))

# Write Parquet file (no partitioning)
df.write.mode("append").parquet(output_path)

print(f"✅ Successfully wrote Parquet with date column to: {output_path}")
