from pyspark.sql import SparkSession

# Init SparkSession
spark = SparkSession.builder \
    .appName("LookIntoParquet") \
    .getOrCreate()

# Read Parquet data
df = spark.read.parquet("gs://gooutside-processed/flat/weather_forecast_bamberg/2025-05-23/part-00000-7d16f81b-49d8-4bc2-a2db-400a28dbeafe-c000.snappy.parquet")

# Show schema and first rows
df.printSchema()
df.show(30, truncate=False)

# Optional: write to local CSV (for inspection/debugging)
# df.coalesce(1).write.mode("overwrite").option("header", True).csv("/tmp/look_output")

