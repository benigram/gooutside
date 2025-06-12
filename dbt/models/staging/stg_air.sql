{{ config(
    tags=['air']
) }}

with raw as (
    select *
    from {{ source('raw', 'air_bamberg') }}
)

select
    aqi,
    city,
    station_code,
    station_id,
    cast(timestamp as timestamp) as timestamp,
    date,
    NO2,
    PM10,
    PM25
from raw