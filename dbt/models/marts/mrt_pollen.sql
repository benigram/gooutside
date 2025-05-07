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
    {{ label_level('esche_level') }} as esche_label,
    {{ label_level('graeser_level') }} as graeser_label,
    {{ label_level('beifuss_level') }} as beifuss_label,
    {{ label_level('birke_level') }} as birke_label,
    {{ label_level('ambrosia_level') }} as ambrosia_label,
    {{ label_level('erle_level') }} as erle_label,
    {{ label_level('hasel_level') }} as hasel_label,
    {{ label_level('roggen_level') }} as roggen_label
from core
