{{ config(
    tags=['pollen']
) }}

with core as (
    select *
    from {{ ref('core_pollen') }}
)

select
    date,
    region,
    region_id,
    region_slug,
    city,
    {{ label_level('esche_level') }} as Esche,
    {{ label_level('graeser_level') }} as Graeser,
    {{ label_level('beifuss_level') }} as Beifuss,
    {{ label_level('birke_level') }} as Birke,
    {{ label_level('ambrosia_level') }} as Ambrosia,
    {{ label_level('erle_level') }} as Erle,
    {{ label_level('hasel_level') }} as Hasel,
    {{ label_level('roggen_level') }} as Roggen
from core
