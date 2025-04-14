from pyspark.sql import SparkSession
from pyspark.sql.functions import to_timestamp, to_date, col
import os

# Init Spark
spark = SparkSession.builder \
    .appName("AirQualityToParquet - Single Day") \
    .getOrCreate()

# Auth for GCS
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "/app/credentials/fastapi-gcs-key.json"

# GCS paths
raw_bucket = "gooutside-raw"
processed_bucket = "gooutside-processed"

input_path = f"gs://{raw_bucket}/air/bamberg/*.json"
output_path = f"gs://{processed_bucket}/parquet/air/bamberg/"

# Read JSON from GCS
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

# Check schema
df.printSchema()
df.show(5, truncate=False)

# Write as Parquet partitioned by date
df.write.mode("overwrite").partitionBy("date").parquet(output_path)

print("✅ Finished converting air data.")

