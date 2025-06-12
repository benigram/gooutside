{{ config(
    tags=['weather']
) }}

with raw as (
    select *
    from {{ source('raw', 'weather_forecast_bamberg') }}
)

select
    cast(timestamp as timestamp) as forecast_ts_utc,
    DATETIME(cast(timestamp as timestamp), "Europe/Berlin") as forecast_ts,
    date,
    city,
    condition,
    cast(temperature as float64) as temperature,
    cast(dew_point as float64) as dew_point,
    cast(pressure_ml as float64) as pressure_ml,
    cast(relative_humidity as float64) as relative_humidity,
    cast(visibility as float64) as visibility,
    cast(precipitation as float64) as precipitation,
    cast(solar as float64) as solar,
    cast(sunshine as float64) as sunshine,
    cast(wind_speed as float64) as wind_speed,
    cast(wind_gust_speed as float64) as wind_gust_speed,
    cast(wind_direction as float64) as wind_direction,
    cast(wind_gust_direction as float64) as wind_gust_direction,
    sunrise,
    sunset
from raw
