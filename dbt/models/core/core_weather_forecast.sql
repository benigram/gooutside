{{ config(
    tags=['weather']
) }}

with stg as (
    select *
    from {{ ref('stg_weather_forecast') }}
)

select
    *
from stg