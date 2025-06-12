{{ config(
    tags=['pollen']
) }}

with stg as (
    select *
    from {{ ref('stg_pollen') }}
),

-- Helper CTE to extract numeric levels
cleaned as (
    select
        date,
        region,
        region_id,
        region_slug,
        city,
        {{ parse_level('esche') }} as esche_level,
        {{ parse_level('graeser') }} as graeser_level,
        {{ parse_level('beifuss') }} as beifuss_level,
        {{ parse_level('birke') }} as birke_level,
        {{ parse_level('ambrosia') }} as ambrosia_level,
        {{ parse_level('erle') }} as erle_level,
        {{ parse_level('hasel') }} as hasel_level,
        {{ parse_level('roggen') }} as roggen_level

    from stg
)

select * from cleaned
