with raw as (
    select *
    from {{ source('raw', 'pollen_bayern_nord') }}
)

select
    cast(date as date) as date,
    lower(region) as region,
    cast(region_id as int) as region_id,
    lower(region_slug) as region_slug,
    lower(city) as city,
    safe_cast(Esche as string) as esche,
    safe_cast(Graeser as string) as graeser,
    safe_cast(Beifuss as string) as beifuss,
    safe_cast(Birke as string) as birke,
    safe_cast(Ambrosia as string) as ambrosia,
    safe_cast(Erle as string) as erle,
    safe_cast(Hasel as string) as hasel,
    safe_cast(Roggen as string) as roggen
from raw