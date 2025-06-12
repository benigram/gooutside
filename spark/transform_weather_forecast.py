import sys
import os
from datetime import datetime, timedelta, timezone
from pyspark.sql.types import DateType
from pyspark.sql import SparkSession
from pyspark.sql.functions import to_date, col, date_format, lit
from astral import LocationInfo
from astral.sun import sun
import pytz

# Argument: date string (e.g. "2025-05-12")
start_date_str = sys.argv[1]
start_date = datetime.strptime(start_date_str, "%Y-%m-%d").date()
end_date = start_date + timedelta(days=8)

# Init Spark
spark = SparkSession.builder \
    .appName("WeatherToParquet - Single Day") \
    .getOrCreate()

# Auth for GCS
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "/app/credentials/fastapi-gcs-key.json"

# Paths
raw_bucket = "gooutside-raw"
processed_bucket = "gooutside-processed"

print(f"🔍 Starting forecast transform for range: {start_date} → {end_date}")

for single_date in (start_date + timedelta(n) for n in range((end_date - start_date).days + 1)):
    date_str = single_date.isoformat()
    input_path = f"gs://{raw_bucket}/weather_forecast/bamberg/{date_str}T*.json"
    output_path = f"gs://{processed_bucket}/flat/weather_forecast_bamberg/{date_str}/"

    print(f"📥 Processing {input_path}")

    try:
        df = spark.read.option("multiline", "true").json(input_path)

        # Cast numeric fields
        df = df.withColumn("pressure_ml", col("pressure_ml").cast("double")) \
               .withColumn("relative_humidity", col("relative_humidity").cast("double")) \
               .withColumn("wind_gust_direction", col("wind_gust_direction").cast("double")) \
               .withColumn("visibility", col("visibility").cast("double")) \
               .withColumn("wind_direction", col("wind_direction").cast("double"))

        # Transform timestamp
        df = df.withColumn("timestamp", date_format("timestamp", "yyyy-MM-dd HH:mm:ss"))
        df = df.withColumn("date", lit(single_date).cast(DateType()))

        # sun raise and sun down
        city_info = LocationInfo(name="Bamberg", region="Germany", timezone="Europe/Berlin", latitude=49.8917, longitude=10.8918)
        sun_data = sun(city_info.observer, date=single_date, tzinfo=pytz.timezone(city_info.timezone))
        sunrise_str = sun_data['sunrise'].strftime("%H:%M")
        sunset_str = sun_data['sunset'].strftime("%H:%M")

        # new column
        df = df.withColumn("sunrise", lit(sunrise_str))
        df = df.withColumn("sunset", lit(sunset_str))

        # Debug preview
        df.printSchema()
        df.show(5, truncate=False)

        # Write Parquet → overwrite per day
        print(f"📝 Writing Parquet to {output_path}")
        df.write.mode("overwrite").parquet(output_path)

        print(f"✅ Finished {date_str}")

    except Exception as e:
        print(f"⚠️ No data for {date_str} or error occurred: {e}")

print(f"🎉 Finished all forecast dates {start_date} → {end_date}")