with base as (
    select *
    from {{ ref('stg_outlook_calendar_events_flat') }}
)
select
    *,
    -- derive Colombian time
    {{ convert_to_timezone('start_datetime', 'America/Bogota') }} AS start_colombia,
    {{ convert_to_timezone('end_datetime', 'America/Bogota') }}   AS end_colombia

from base
