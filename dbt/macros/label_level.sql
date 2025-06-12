{% macro label_level(level_column) %}
    case
        when {{ level_column }} = 0 then 'keine'
        when {{ level_column }} = 0.5 then 'keine bis schwach'
        when {{ level_column }} = 1 then 'schwach'
        when {{ level_column }} = 1.5 then 'schwach bis mäßig'
        when {{ level_column }} = 2 then 'mäßig'
        when {{ level_column }} = 2.5 then 'mäßig bis stark'
        when {{ level_column }} = 3 then 'stark'
        else null
    end
{% endmacro %}
