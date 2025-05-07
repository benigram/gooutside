from pyspark.sql import SparkSession

# Init SparkSession
spark = SparkSession.builder \
    .appName("LookIntoParquet") \
    .getOrCreate()

# Read Parquet data
df = spark.read.parquet("gs://gooutside-processed/flat/pollen_bayern_nord/part-00000-016c7abb-3b89-4a66-bb9f-1f99be90dddc-c000.snappy.parquet")

# Show schema and first rows
df.printSchema()
df.show(30, truncate=False)

# Optional: write to local CSV (for inspection/debugging)
# df.coalesce(1).write.mode("overwrite").option("header", True).csv("/tmp/look_output")

