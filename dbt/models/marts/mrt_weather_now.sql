{{ config(
    tags=['weather']
) }}

with base as (
    select *
    from {{ ref('core_weather_forecast') }}
    where city = 'bamberg'
      and forecast_ts <= CURRENT_DATETIME("Europe/Berlin")
    order by forecast_ts
    limit 1
),

final as (
    select
        city,
        forecast_ts as measurement_ts,
        date(forecast_ts) as date,
        condition,
        temperature,
        dew_point,
        pressure_ml,
        relative_humidity,
        visibility,
        precipitation,
        solar,
        sunshine,
        wind_speed,
        round(wind_speed * 3.6, 1) as wind_speed_kmh,
        round(wind_gust_speed * 3.6, 1) as wind_gust_speed_kmh,
        wind_direction,
        {{ label_wind_direction('wind_direction') }} as wind_compass,
        wind_gust_direction,
        {{ label_wind_direction('wind_gust_direction') }} as wind_gust_compass,

        case 
            when temperature <= 10 and wind_speed * 3.6 > 4.8 then
                -- Windchill
                round(13.12 + 0.6215 * temperature - 11.37 * pow(wind_speed * 3.6, 0.16) + 0.3965 * temperature * pow(wind_speed * 3.6, 0.16), 1)
            else
                -- Humidex
                round(
                    temperature + 0.5555 * (6.11 * pow(2.71828, (5417.7530 * ((1/273.16) - (1/(273.15 + dew_point)))) ) - 10),
                    1
                )
        end as feels_like,

        PARSE_TIME('%H:%M', sunrise) AS sunrise_time,
        PARSE_TIME('%H:%M', sunset) AS sunset_time

    from base
)

select * from final
