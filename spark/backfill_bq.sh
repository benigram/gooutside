#!/bin/bash

# ✅ Konfiguration
GCS_BASE_PATH="gs://gooutside-processed/flat/weather_bamberg/"
BQ_TABLE="gooutside_raw.weather_bamberg"

echo "🚀 Starte Backfill aller Partitionen von $GCS_BASE_PATH nach $BQ_TABLE"

# ✅ Hole alle Ordner (Partitionen = Datum)
for folder in $(gsutil ls ${GCS_BASE_PATH}); do
    echo "👉 Lade Partition: $folder"
    bq load --source_format=PARQUET $BQ_TABLE "${folder}part-*.parquet"
done

echo "✅ Backfill abgeschlossen!"
