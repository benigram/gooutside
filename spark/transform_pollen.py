from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date
import os

# Init Spark
spark = SparkSession.builder \
    .appName("PollenToParquet") \
    .getOrCreate()

# Auth for GCS
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "/app/credentials/fastapi-gcs-key.json"

# GCS paths
raw_bucket = "gooutside-raw"
processed_bucket = "gooutside-processed"

input_path = f"gs://{raw_bucket}/pollen/bayern_nord/*.json"
output_path = f"gs://{processed_bucket}/parquet/pollen/bayern_nord/"

# Read JSON from GCS
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

# Cast 'date' column to DateType
df = df.withColumn("date", to_date("date"))

df.printSchema()
df.show(5, truncate=False)

# Write as Parquet partitioned by date
df.write.mode("overwrite").partitionBy("date").parquet(output_path)

print("✅ Finished converting pollen data.")
