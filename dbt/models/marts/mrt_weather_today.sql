{{ config(
    tags=['weather']
) }}

WITH hourly_forecast AS (
    SELECT
        forecast_ts AS timestamp,
        EXTRACT(HOUR FROM forecast_ts) AS hour,
        condition,
        temperature
    FROM {{ ref('core_weather_forecast') }}
    WHERE
        city = 'bamberg'
        AND DATE(forecast_ts) = CURRENT_DATE("Europe/Berlin")
    ORDER BY forecast_ts
)

SELECT * FROM hourly_forecast
