{% macro aqi_level(level_column) %}
    case
        when {{ level_column }} = 0 then 'sehr gut'
        when {{ level_column }} = 1 then 'gut'
        when {{ level_column }} = 2 then 'mäßig'
        when {{ level_column }} = 3 then 'schlecht'
        when {{ level_column }} = 4 then 'sehr schlecht'

        else null
    end
{% endmacro %}