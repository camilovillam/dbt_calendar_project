{% macro convert_to_timezone(json_field, tz) %}
    (
        CAST(json_extract_string({{ json_field }}, '$.dateTime') || ' UTC' AS TIMESTAMPTZ)
        AT TIME ZONE '{{ tz }}'
    )
{% endmacro %}
