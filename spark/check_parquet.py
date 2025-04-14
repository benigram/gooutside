from pyspark.sql import SparkSession

# Spark-Session starten
spark = SparkSession.builder.appName("ReadParquet").getOrCreate()

# Parquet-Datei laden
df = spark.read.parquet("gs://gooutside-processed/parquet/air/bamberg/date=2024-04-02/")

# Schema anzeigen
df.printSchema()

# Inhalte anzeigen
df.show(10, truncate=False)
