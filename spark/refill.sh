#!/bin/bash

# Start- und Enddatum (ändern wie gewünscht)
start_date="2025-01-01"
end_date="2025-05-11"

# convert to seconds since epoch
start_sec=$(date -j -f "%Y-%m-%d" "$start_date" "+%s")
end_sec=$(date -j -f "%Y-%m-%d" "$end_date" "+%s")

# Tagesweise iterieren
current_sec=$start_sec
while [ "$current_sec" -le "$end_sec" ]; do
    date_str=$(date -j -f "%s" "$current_sec" "+%Y-%m-%d")
    echo "👉 Converting $date_str"
    docker exec gooutside-spark spark-submit /opt/spark-app/transform_weather.py "$date_str"
    current_sec=$((current_sec + 86400))
done

