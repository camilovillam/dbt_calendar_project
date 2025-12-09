{% macro convert_iso_string_to_timezone(dt_column, tz) %}
(
    CAST(
        -- Trim fractional seconds to max 6 digits (DuckDB limit)
        regexp_replace(
            {{ dt_column }},
            '\\.(\\d{6})\\d+',
            '.\\1'
        ) || 'Z'
        AS TIMESTAMPTZ
    ) AT TIME ZONE '{{ tz }}'
)
{% endmacro %}
