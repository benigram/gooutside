{% macro label_wind_direction(degrees_column) %}
    case
        when {{ degrees_column }} is null then null
        when {{ degrees_column }} >= 337.5 or {{ degrees_column }} < 22.5 then 'N'
        when {{ degrees_column }} >= 22.5 and {{ degrees_column }} < 67.5 then 'NE'
        when {{ degrees_column }} >= 67.5 and {{ degrees_column }} < 112.5 then 'E'
        when {{ degrees_column }} >= 112.5 and {{ degrees_column }} < 157.5 then 'SE'
        when {{ degrees_column }} >= 157.5 and {{ degrees_column }} < 202.5 then 'S'
        when {{ degrees_column }} >= 202.5 and {{ degrees_column }} < 247.5 then 'SW'
        when {{ degrees_column }} >= 247.5 and {{ degrees_column }} < 292.5 then 'W'
        when {{ degrees_column }} >= 292.5 and {{ degrees_column }} < 337.5 then 'NW'
        else null
    end
{% endmacro %}
