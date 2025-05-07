{% macro parse_level(column_name) %}
    case
        when {{ column_name }} like '%-%' then (
            -- "0-1" → (0 + 1) / 2 = 0.5
            cast(split({{ column_name }}, '-')[offset(0)] as float64) +
            cast(split({{ column_name }}, '-')[offset(1)] as float64)
        ) / 2
        when REGEXP_CONTAINS({{ column_name }}, r'^\d+$') then cast({{ column_name }} as float64)
        else null
    end
{% endmacro %}


