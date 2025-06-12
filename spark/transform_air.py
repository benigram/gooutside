import sys
import time
import os
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import to_date, col, date_format
from google.cloud import storage

# Argument: timestamp string (e.g. "2025-05-06T12:00:00+00:00")
ts_str = sys.argv[1]
ts = datetime.fromisoformat(ts_str)
date_str = ts.date().isoformat()

# Init Spark
spark = SparkSession.builder \
    .appName("AirQualityToParquet - Single Day Partitioned") \
    .getOrCreate()

# Auth for GCS
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "/opt/credentials/fastapi-gcs-key.json"

# Paths
raw_bucket = "gooutside-raw"
processed_bucket = "gooutside-processed"
input_prefix = f"air/bamberg/{date_str}T"
input_path = f"gs://{raw_bucket}/{input_prefix}*.json"
output_path = f"gs://{processed_bucket}/flat/air_bamberg/{date_str}/"

print(f"🔍 Looking for air data files: {input_prefix}*.json")
client = storage.Client()
bucket = client.bucket(raw_bucket)

# Wait + retry for files (max 12 attempts, 60 sec total)
found = False
for attempt in range(12):
    blobs = list(bucket.list_blobs(prefix=input_prefix))
    if blobs:
        print(f"✅ Found files for {date_str}")
        found = True
        break
    print(f"⏳ Waiting for air files... attempt {attempt+1}/12")
    time.sleep(5)

if not found:
    print(f"❌ No air data found for {date_str}. Exiting.")
    sys.exit(0)

# Read JSONs
print(f"📥 Reading air data: {input_path}")
df = spark.read.option("multiline", "true").json(input_path)

# Schema check
df.printSchema()
df.show(5, truncate=False)

# Transform timestamp + partition column
df = df.withColumn("timestamp", date_format("timestamp", "yyyy-MM-dd HH:mm:ss"))
df = df.withColumn("date", to_date("timestamp"))

# Flatten components
df = df.withColumn("NO2", col("components").getItem("NO2")) \
       .withColumn("PM10", col("components").getItem("PM10")) \
       .withColumn("PM25", col("components").getItem("PM2.5")) \
       .drop("components")

# Write as Parquet (overwrite single day)
print(f"📝 Writing Parquet to {output_path}")
df.write.mode("overwrite").parquet(output_path)

print(f"✅ Finished air data for {date_str}")

