{% macro label_level(level_column) %}
    case
        when {{ level_column }} = 0 then 'none'
        when {{ level_column }} = 0.5 then 'none-low'
        when {{ level_column }} = 1 then 'low'
        when {{ level_column }} = 1.5 then 'low-medium'
        when {{ level_column }} = 2 then 'medium'
        when {{ level_column }} = 2.5 then 'medium-high'
        when {{ level_column }} = 3 then 'high'
        else null
    end
{% endmacro %}
