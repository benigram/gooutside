{{ config(
    tags=['air']
) }}

with air_data as (
    select *
    from {{ ref('stg_air') }}
    where city = 'bamberg'

),

latest_record as (
    select *
    from air_data
    qualify row_number() over (order by `timestamp` desc) = 1

)

select
    station_id,
    station_code,
    city,
    `timestamp` as measurement_ts,
    date,
    aqi,
    {{ aqi_level('aqi') }} as aqi_level,
    NO2,
    PM10,
    PM25
from latest_record

