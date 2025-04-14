from pyspark.sql import SparkSession
from pyspark.sql.functions import to_timestamp, to_date, col
import os

# Init Spark
spark = SparkSession.builder \
    .appName("WeatherToParquet - Single Day") \
    .getOrCreate()

# Auth for GCS
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "/app/credentials/fastapi-gcs-key.json"

# GCS paths
raw_bucket = "gooutside-raw"
processed_bucket = "gooutside-processed"

# Test one Date
#input_path = f"gs://{raw_bucket}/weather_forecast/bamberg/2025-04-12T*.json"

input_path = f"gs://{raw_bucket}/weather_forecast/bamberg/*.json"
output_path = f"gs://{processed_bucket}/parquet/weather_forecast/bamberg/"

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

# Write as Parquet partitioned by date
df.write.mode("overwrite").partitionBy("date").parquet(output_path)

print("✅ Finished converting weather forecast data.")