{{ config(
    tags=['weather']
) }}


with ranked_conditions as (
    select
        date,
        condition,
        count(*) as count,
        row_number() over (partition by date order by count(*) desc) as rank
    from {{ ref('core_weather_forecast') }}
    where DATE(forecast_ts) >= CURRENT_DATE("Europe/Berlin")
    group by date, condition
),

daily_extremes as (
    select
        date,
        min(temperature) as min_temp,
        max(temperature) as max_temp
    from {{ ref('core_weather_forecast') }}
    where DATE(forecast_ts) >= CURRENT_DATE("Europe/Berlin")
    group by date
)

select
    e.date,
    r.condition,
    e.min_temp,
    e.max_temp
from daily_extremes e
left join ranked_conditions r
  on e.date = r.date and r.rank = 1

