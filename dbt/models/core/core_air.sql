{{ config(
    tags=['air']
) }}

with stg as (
    select *
    from {{ ref('stg_air') }}
)

select
    station_id,
    station_code,
    city,
    timestamp,
    date,
    aqi,
    NO2,
    PM10,
    PM25
from stg